#!/usr/bin/env python3
"""Cost and accuracy experiments on the real collector containers.

inline-cost   Branch CPU with and without each function, over offered rates.
              "without" forwards raw points only (the pipeline before the
              function); "with" adds the fanout and the sketch. Also records
              delivered points/s, branch memory and each answer path's delay.
accuracy-size Error against exact versus sketch size: KLL k (latency p99
              error) and Count-Min width (top-10 recall and count error),
              with the bytes merge receives per window.

Both reuse acceptance.py's time-scaled stack (10-second windows) and write a
CSV per experiment to --output.

    deploy/telemetry-functions/experiments.py inline-cost --docker-command "sudo docker"
"""
import argparse
import csv
import re
import shlex
import statistics
import tempfile
import time
from pathlib import Path

import acceptance as acc

BRANCH_WITHOUT = """# Stage 1 without the function: forward raw points to the exact baseline only.
version: otel_dataflow/v1
engine:
  http_admin:
    bind_address: 0.0.0.0:8085
policies:
  resources:
    core_allocation:
      type: core_count
      count: 1
groups:
  default:
    pipelines:
      main:
        nodes:
          receiver:
            type: receiver:otlp
            config:
              protocols:
                http: {listening_addr: "0.0.0.0:4318", wait_for_result: false}
          to_exact:
            type: exporter:otlp_http
            config:
              endpoint: http://exact:4318
              client_pool_size: 1
              max_in_flight: 1
              http: {timeout: 30s, compression: none}
        connections:
          - {from: receiver, to: to_exact}
"""


def prepare(use_case, into, rate, branch_variant="with", edits=()):
    acc.scaled_copy(use_case, into)
    for f in into.glob("generator-*.yaml"):
        f.write_text(re.sub(r"signals_per_second: \d+", f"signals_per_second: {rate}", f.read_text()))
    if branch_variant == "without":
        (into / "branch.yaml").write_text(BRANCH_WITHOUT)
    if branch_variant in ("with", "without") and not edits:
        # The exact stage only counts points per route (cheap), so it never
        # throttles the branch being measured.
        exact = into / "exact.yaml"
        exact.write_text(re.sub(
            r"(?s)(type: urn:asap:processor:exact_answers\n)            config:.*?\n(          to_prometheus:)",
            r"\1            config: {window: 10s, per_attribute: [http.route]}\n\2",
            exact.read_text()))
    for name, old, new in edits:
        path = into / name
        text = path.read_text()
        assert old in text, (name, old)
        path.write_text(text.replace(old, new))
    into.chmod(0o755)
    for f in into.iterdir():
        f.chmod(0o644)


def measure(stack, seconds):
    """Averages over the last `seconds`."""
    rng = f"{seconds}s"
    one = lambda expr: (stack.vector(expr) or [({}, float("nan"))])[0][1]
    by_stage = lambda expr: {m.get("stage"): v for m, v in stack.vector(expr)}
    cpu = by_stage(f'sum by (stage) (rate(cpu_time_total{{otel_scope_pipeline_id="main"}}[{rng}]))')
    rss = by_stage("max by (stage) (memory_rss)")
    # Total points over the range divided by the windows it covers.
    sel = 'asap_points{method="exact",attribute_key="http.route"}'
    windows_seen = one(f"max(count_over_time({sel}[{rng}]))")
    delivered = one(f"sum(sum_over_time({sel}[{rng}]))") / (windows_seen * acc.WINDOW_S)
    delay = {m["method"]: v for m, v in stack.vector(f"avg by (method) (avg_over_time(asap_answer_delay[{rng}]))")}
    return {
        "branch_cpu_cores": statistics.mean([cpu.get("branch-a", 0), cpu.get("branch-b", 0)]),
        "branch_rss_mib": max(rss.get("branch-a", 0), rss.get("branch-b", 0)) / 2**20,
        "exact_cpu_cores": cpu.get("exact", 0),
        "delivered_points_per_s": delivered,
        "sketch_answer_delay_s": delay.get("sketch", float("nan")),
        "exact_answer_delay_s": delay.get("exact", float("nan")),
    }


def run(args, use_case, rate, variant="with", edits=(), extra=None):
    with tempfile.TemporaryDirectory(prefix="asap-exp-") as tmp:
        prepare(use_case, Path(tmp), rate, variant, edits)
        stack = acc.Stack(shlex.split(args.docker_command), tmp, args.prometheus, args.loki)
        stack.compose("down", "--remove-orphans")
        stack.compose("up", "-d")
        try:
            time.sleep(args.warmup + args.measure)
            row = {"use_case": use_case, "variant": variant, "offered_per_source": rate}
            row.update(measure(stack, args.measure))
            if extra:
                row.update(extra(stack))
        finally:
            stack.compose("down", "--remove-orphans")
    print(row, flush=True)
    return row


def merge_bytes(stack, seconds=60):
    v = stack.vector(f'sum(rate(payload_size_bytes_total{{stage="merge",otel_scope_node_type="receiver"}}[{seconds}s]))')
    return {"merge_bytes_per_window": v[0][1] * acc.WINDOW_S if v else float("nan")}


def latency_error(stack):
    t = acc.latest_window(stack, 'attribute_key=""')
    exact = {m["http_route"]: v for m, v in stack.vector(f'asap_quantile{{method="exact",quantile="0.99"}} @ {t}')}
    sketch = {m["http_route"]: v for m, v in stack.vector(f'asap_quantile{{method="sketch",quantile="0.99",bound="estimate"}} @ {t}')}
    points = {m["http_route"]: v for m, v in stack.vector(f'asap_points{{method="exact"}} @ {t}')}
    errs = [abs(sketch[r] - exact[r]) / exact[r] for r in exact if r in sketch and points.get(r, 0) >= 20000]
    return {"p99_rel_error_median": statistics.median(errs) if errs else float("nan"),
            "p99_rel_error_max": max(errs) if errs else float("nan"), **merge_bytes(stack)}


def topk_error(stack):
    t = acc.latest_window(stack, 'attribute_key="http.route"')
    get = lambda m: {r["key"]: v for r, v in stack.vector(f'asap_topk{{method="{m}",bound=~"estimate|"}} @ {t}')}
    sketch, exact = get("sketch"), get("exact")
    errs = [abs(sketch[k] - exact[k]) / exact[k] for k in exact if k in sketch]
    return {"recall_at_10": len(set(sketch) & set(exact)) / max(1, len(exact)),
            "count_rel_error_mean": statistics.mean(errs) if errs else float("nan"), **merge_bytes(stack)}


def inline_cost(args):
    rows = []
    for use_case in args.use_cases:
        for rate in args.rates:
            for variant in ("without", "with"):
                rows.append(run(args, use_case, rate, variant))
    return rows


def accuracy_size(args):
    rows = []
    for k in ([] if args.only == "cms" else (50, 100, 200, 400)):
        edit = ("branch.yaml", "sketch_params: {k: 200}", f"sketch_params: {{k: {k}}}")
        rows.append(run(args, "latency", args.rates[0], edits=[edit],
                        extra=lambda s, k=k: {"param": f"kll_k={k}", **latency_error(s)}))
    # 100,000 Zipf routes: enough keys for Count-Min collisions to matter.
    keys = [(g, "cardinality: 200, zipf: 1.1", "cardinality: 100000, zipf: 1.1")
            for g in ("generator-a.yaml", "generator-b.yaml")]
    for cols in ([] if args.only == "kll" else (256, 512, 1024, 2048)):
        edit = ("branch.yaml", "columns: 2048", f"columns: {cols}")
        rows.append(run(args, "topk", args.rates[0], edits=[edit, *keys],
                        extra=lambda s, c=cols: {"param": f"cms_columns={c}", **topk_error(s)}))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=("inline-cost", "accuracy-size"))
    parser.add_argument("--use-cases", nargs="+", default=["latency", "cardinality", "topk"])
    parser.add_argument("--rates", type=int, nargs="+", default=[20000, 50000, 100000, 200000],
                        help="offered points/s per source")
    parser.add_argument("--warmup", type=int, default=40)
    parser.add_argument("--measure", type=int, default=60)
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    parser.add_argument("--loki", default="http://127.0.0.1:3100")
    parser.add_argument("--output", type=Path, default=Path("experiment-results"))
    parser.add_argument("--only", choices=("kll", "cms"), help="accuracy-size: run one sweep")
    args = parser.parse_args()
    rows = {"inline-cost": inline_cost, "accuracy-size": accuracy_size}[args.experiment](args)
    args.output.mkdir(parents=True, exist_ok=True)
    path = args.output / f"{args.experiment}.csv"
    with path.open("w", newline="") as f:
        fields = list(dict.fromkeys(k for r in rows for k in r))
        writer = csv.DictWriter(f, fieldnames=fields, restval="")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
