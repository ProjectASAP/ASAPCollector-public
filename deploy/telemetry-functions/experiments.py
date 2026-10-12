#!/usr/bin/env python3
"""Cost and accuracy experiments on the real collector containers.

pipelines     The three pipelines (sketch, exact, raw) side by side over
              offered rates, fed identical points: CPU of each stage, memory,
              points delivered per second and window completion delay. The raw
              pipeline's branch only forwards points, so the sketch branch's
              CPU minus the raw branch's is the cost of the function.
accuracy-size Error against exact versus sketch size: KLL k (latency p99
              error) and Count-Min width (top-10 recall and count error),
              with the bytes merge receives per window.

Both reuse acceptance.py's time-scaled stack (10-second windows) and write a
CSV per experiment to --output.

    deploy/telemetry-functions/experiments.py pipelines --docker-command "sudo docker"
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

# Stages of each pipeline: per-source branches, merge, end.
PIPELINES = {
    "sketch": ("branch", "merge", "detector"),
    "exact": ("exact-branch", "exact-merge", "exact-detector"),
    "raw": ("raw-branch", "raw-merge", "raw-sink"),
}


def prepare(use_case, root, rate, edits=()):
    pipelines, common = root / use_case, root / "common"
    acc.scaled_copy(use_case, pipelines)
    acc.scaled_copy("common", common)
    for f in pipelines.glob("generator-*.yaml"):
        f.write_text(re.sub(r"signals_per_second: \d+", f"signals_per_second: {rate}", f.read_text()))
    for name, old, new in edits:
        path = pipelines / name
        text = path.read_text()
        assert old in text, (name, old)
        path.write_text(text.replace(old, new))
    for f in [pipelines, common, *pipelines.iterdir(), *common.iterdir()]:
        f.chmod(0o755 if f.is_dir() else 0o644)
    return pipelines, common


def measure(stack, use_case, seconds):
    """Averages over the last `seconds`."""
    rng = f"{seconds}s"
    by = lambda label, expr: {m.get(label): v for m, v in stack.vector(expr)}
    cpu = by("stage", f'sum by (stage) (rate(cpu_time_total{{otel_scope_pipeline_id="main"}}[{rng}]))')
    rss = by("stage", "max by (stage) (memory_rss)")
    delay = by("method", f"avg by (method) (avg_over_time(asap_answer_delay[{rng}]))")
    flt = acc.CASES[use_case][0]
    row = {}
    for name, (branch, merge, end) in PIPELINES.items():
        stages = [f"{branch}-a", f"{branch}-b", merge, end]
        # Total points over the range divided by the windows it covers.
        sel = f'asap_points{{method="{name}"' + ("" if name == "raw" else f",{flt}") + "}"
        seen = (stack.vector(f"max(count_over_time({sel}[{rng}]))") or [({}, float("nan"))])[0][1]
        total = (stack.vector(f"sum(sum_over_time({sel}[{rng}]))") or [({}, float("nan"))])[0][1]
        row.update({
            f"{name}_delivered_per_s": total / (seen * acc.WINDOW_S),
            f"{name}_branch_cpu_cores": statistics.mean([cpu.get(stages[0], 0), cpu.get(stages[1], 0)]),
            f"{name}_merge_cpu_cores": cpu.get(merge, 0),
            f"{name}_end_cpu_cores": cpu.get(end, 0),
            f"{name}_rss_mib": sum(rss.get(s, 0) for s in stages) / 2**20,
            f"{name}_delay_s": delay.get(name, float("nan")),
        })
    transit = by("quantile", f"avg by (quantile) (avg_over_time(asap_raw_transit[{rng}]))")
    row["raw_transit_p99_s"] = transit.get("0.99", float("nan"))
    return row


def run(args, use_case, rate, edits=(), extra=None):
    with tempfile.TemporaryDirectory(prefix="asap-exp-") as tmp:
        pipelines, common = prepare(use_case, Path(tmp), rate, edits)
        stack = acc.Stack(shlex.split(args.docker_command), pipelines, common, args.prometheus, args.loki)
        stack.compose("down", "--remove-orphans")
        stack.compose("up", "-d")
        try:
            time.sleep(args.warmup + args.measure)
            row = {"use_case": use_case, "offered_per_source": rate}
            row.update(measure(stack, use_case, args.measure))
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


def pipelines(args):
    return [run(args, use_case, rate) for use_case in args.use_cases for rate in args.rates]


def accuracy_size(args):
    rows = []
    for k in ([] if args.only == "cms" else (50, 100, 200, 400)):
        edit = ("branch.yaml", "sketch_params: {k: 200}", f"sketch_params: {{k: {k}}}")
        rows.append(run(args, "latency", args.rates[0], edits=[edit],
                        extra=lambda s, k=k: {"param": f"kll_k={k}", **latency_error(s)}))
    # The top-k use case's 100,000 Zipf routes are enough keys for Count-Min
    # collisions to matter.
    for cols in ([] if args.only == "kll" else (256, 512, 1024, 2048)):
        edit = ("branch.yaml", "columns: 512", f"columns: {cols}")
        rows.append(run(args, "topk", args.rates[0], edits=[edit],
                        extra=lambda s, c=cols: {"param": f"cms_columns={c}", **topk_error(s)}))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=("pipelines", "accuracy-size"))
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
    rows = {"pipelines": pipelines, "accuracy-size": accuracy_size}[args.experiment](args)
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
