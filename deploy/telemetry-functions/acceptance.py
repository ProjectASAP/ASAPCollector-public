#!/usr/bin/env python3
"""End-to-end acceptance of the telemetry functions on real collector containers.

For each use case: start the three pipelines (sketch, exact, raw) plus
Prometheus, Loki and Grafana with `docker compose`, wait for the scenario to
play out, then check the sketch answers against the exact pipeline's ground
truth through Prometheus and Loki, and that every pipeline reports its window
completion delay. Prints one line per check and exits non-zero if any check
fails.

Time is scaled down 6x so a use case takes a few minutes: 10-second windows, a
5-second timeout for a missing source, changes after 1/6 of their configured time and growth 6x
faster per minute. Rules counted in windows (baselines, thresholds) are as
deployed. Rate and cardinality ramps are left out: acceptance checks the
answers at the starting rate; how each approach scales is what the demo shows.

    deploy/telemetry-functions/acceptance.py --docker-command "sudo docker"
    deploy/telemetry-functions/acceptance.py --use-cases drift topk

Requires the asap-collector:local image (run.sh builds it, or pass --build).
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
SCALE = 6
WINDOW_S = 60 // SCALE
USE_CASES = ("latency", "cardinality", "drift", "topk")
STAGES = tuple(f"{p}{s}" for p in ("", "exact-", "raw-") for s in ("generator-a", "generator-b"))
STAGES += ("branch-a", "branch-b", "merge", "detector", "exact-branch-a", "exact-branch-b",
           "exact-merge", "exact-detector", "raw-sink-a", "raw-sink-b", "hh-accuracy")
METHODS = ("sketch", "exact", "raw")


def rate(directory):
    """Data points per second per source at the start of the scenario."""
    text = (HERE / directory / "generator-a.yaml").read_text()
    return int(re.search(r"signals_per_second: (\d+)", text)[1])


def scaled_copy(directory, into):
    """A stage-config directory with time scaled down SCALE times."""
    into.mkdir(exist_ok=True)
    for src in (HERE / directory).glob("*.yaml"):
        text = src.read_text()
        text = text.replace("window_size: 60s", f"window_size: {WINDOW_S}s")
        text = text.replace("window: 60s", f"window: {WINDOW_S}s")
        # The 5 s timeout for a missing source is not scaled: merges emit as
        # soon as both sources sent a window, and shared CI runners can be slow.
        text = re.sub(r"\n\s*- \{after_seconds: \d+, (signals_per_second|attribute: [^,]+, cardinality): \d+\}", "", text)
        text = re.sub(r"after_seconds: (\d+)", lambda m: f"after_seconds: {int(m[1]) // SCALE}", text)
        text = re.sub(r"growth_per_minute: ([0-9.]+)",
                      lambda m: f"growth_per_minute: {float(m[1]) * SCALE:g}", text)
        (into / src.name).write_text(text)


class Stack:
    def __init__(self, docker, use_case, pipelines, common, prometheus, loki):
        self.docker, self.use_case = docker, use_case
        self.pipelines, self.common = pipelines, common
        self.rate = rate(use_case)
        self.prometheus, self.loki = prometheus, loki
        # The generators of all three pipelines count from the same second.
        self.start = int(time.time()) + 10

    def compose(self, *args):
        # A CI runner has about 16 GB; the raw pipeline's store gets 4.
        env = dict(os.environ, PIPELINES=str(self.pipelines), COMMON=str(self.common),
                   SCENARIO_START=str(self.start), LOKI_RAW_MEMORY="4g",
                   USE_CASE=self.use_case, WINDOW_SECONDS=str(WINDOW_S))
        cmd = self.docker + ["compose", "-f", str(HERE / "compose.yaml"), *args]
        return subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)

    def get(self, base, path, params):
        url = base + path + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.load(response)["data"]["result"]

    def vector(self, expr):
        return [(r["metric"], float(r["value"][1])) for r in self.get(self.prometheus, "/api/v1/query", {"query": expr})]

    def matrix(self, expr, minutes=5):
        rows = self.get(self.prometheus, "/api/v1/query", {"query": f"{expr}[{minutes}m]"})
        return [(r["metric"], {int(float(t)): float(v) for t, v in r["values"]}) for r in rows]

    def events(self, selector):
        rows = self.get(self.loki, "/loki/api/v1/query_range",
                        {"query": '{service_name="asap-detector"} ' + selector, "limit": 500,
                         "start": str(int((time.time() - 1800) * 1e9))})
        return [v[1] for s in rows for v in s["values"]]


def check(name, ok, detail):
    return {"check": name, "ok": bool(ok), "detail": detail}


def windows(stack, flt):
    """Per-window total points per method; sketch and exact for one series
    family, raw for the whole window."""
    out = {}
    for method in METHODS:
        totals = {}
        sel = f'method="{method}"' + ("" if method == "raw" else f",{flt}")
        for _, samples in stack.matrix(f"asap_points{{{sel}}}"):
            for t, v in samples.items():
                totals[t] = totals.get(t, 0) + v
        out[method] = totals
    return out


def latest_window(stack, flt):
    """End of the latest window answered by both sketch and exact."""
    w = windows(stack, flt)
    shared = sorted(set(w["sketch"]) & set(w["exact"]))
    return shared[-1] if shared else int(time.time())


def common_checks(stack, flt):
    up = stack.vector('up{job="collectors"}')
    yield check("every stage is up", len(up) == len(STAGES) and all(v == 1 for _, v in up),
                f"{sum(v == 1 for _, v in up)}/{len(STAGES)} collectors up")
    w = windows(stack, flt)
    shared = sorted(set(w["sketch"]) & set(w["exact"]))[1:-1]  # skip partial edges
    expected = 2 * stack.rate * WINDOW_S
    full = [w["exact"][t] for t in shared]
    yield check("full rate reaches the end (no backpressure)",
                full and all(abs(v - expected) <= 0.01 * expected for v in full),
                f"{len(full)} windows, points/window {sorted(set(int(v) for v in full))} vs {expected}")
    diff = [(t, int(w["sketch"][t]), int(w["exact"][t])) for t in shared if w["sketch"][t] != w["exact"][t]]
    yield check("sketch and exact windows hold the same points", shared and not diff,
                f"{len(shared)} windows compared" + (f"; differ (window end, sketch, exact): {diff}" if diff else ""))
    # The raw pipeline writes every point to its own store; whether it keeps
    # up is the measurement, so its rate is reported, not required.
    raw = [int(w["raw"][t]) for t in sorted(w["raw"])[1:-1]]
    delay = {m["method"]: v for m, v in stack.vector("avg by (method) (avg_over_time(asap_answer_delay[5m]))")}
    yield check("every pipeline reports its window completion delay",
                set(delay) >= set(METHODS),
                ", ".join(f"{m} {delay[m]:.1f} s" for m in METHODS if m in delay)
                + f"; raw points/window {sorted(set(raw))} vs {expected}")
    stages = {(m["method"], m["stage"]) for m, _ in stack.vector(
        "max by (method, stage) (max_over_time(asap_stage_emitted[5m]))")}
    want = {(m, s) for m in ("sketch", "exact") for s in ("merge", "detector")}
    yield check("merge and detector report their timing (latency breakdown)", stages >= want,
                f"{sorted(stages)}")
    queried = {m["method"] for m, _ in stack.vector("max_over_time(asap_query_seconds[5m])")}
    yield check("the query job times the backend query of sketch and exact",
                queried >= {"sketch", "exact"}, f"{sorted(queried)}")


def latency_checks(stack, t):
    exact = {(m["http_route"], m["quantile"]): v for m, v in stack.vector(f'asap_quantile{{method="exact"}} @ {t}')}
    points = {m["http_route"]: v for m, v in stack.vector(f'asap_points{{method="exact"}} @ {t}')}
    bounds = {(m["http_route"], m["quantile"], m["bound"]): v
              for m, v in stack.vector(f'asap_quantile{{method="sketch"}} @ {t}')}
    inside = total = 0
    for (route, q), truth in exact.items():
        if points.get(route, 0) < 2000 or (route, q, "lower") not in bounds:
            continue
        total += 1
        inside += bounds[(route, q, "lower")] <= truth <= bounds[(route, q, "upper")]
    yield check("exact p50/p99 inside the sketch bound (routes >= 2,000 points)",
                total and inside >= 0.95 * total, f"{inside}/{total} inside")
    p99 = [(abs(bounds[(r, "0.99", "estimate")] - v) / v) for (r, q), v in exact.items()
           if q == "0.99" and points.get(r, 0) >= 20000 and (r, q, "estimate") in bounds]
    yield check("p99 relative error for routes >= 20,000 points", p99,
                f"max {max(p99):.2%} over {len(p99)} routes" if p99 else "no routes")


def cardinality_checks(stack, t):
    state = {(m["attribute_key"], m["method"]): m["state"]
             for m, v in stack.vector(f"asap_cardinality_state @ {t} == 1")}
    keys = sorted({k for k, _ in state})
    yield check("sketch and exact agree on every key's class",
                keys and all(state.get((k, "sketch")) == state.get((k, "exact")) for k in keys),
                ", ".join(f"{k}={state.get((k, 'sketch'))}/{state.get((k, 'exact'))}" for k in keys))
    want = {"user.id": "identifier-like", "http.route": "stable", "http.method": "stable"}
    yield check("identifier and stable keys classified as generated",
                all(state.get((k, m)) == s for k, s in want.items() for m in ("sketch", "exact")), str(want))
    grew = stack.vector('max_over_time(asap_cardinality_state{state="growing",attribute_key="k8s.pod.name"}[10m])')
    yield check("growing key (k8s.pod.name) classified growing by both",
                {m["method"] for m, v in grew if v == 1} == {"sketch", "exact"},
                f"growing seen by {sorted(m['method'] for m, v in grew if v == 1)}")
    distinct = {(m["attribute_key"], m["method"]): v
                for m, v in stack.vector(f'asap_distinct{{bound=~"estimate|"}} @ {t}')}
    errs = {k: abs(distinct[(k, "sketch")] - distinct[(k, "exact")]) / distinct[(k, "exact")]
            for k in keys if (k, "sketch") in distinct and (k, "exact") in distinct}
    yield check("distinct-count error <= 2% for every key", errs and max(errs.values()) <= 0.02,
                ", ".join(f"{k} {e:.2%}" for k, e in errs.items()))
    # Store and query answers later than exact: compare the latest window it answered.
    raw = {m["attribute_key"]: series for m, series in stack.matrix('asap_distinct{method="raw"}')}
    exact = {m["attribute_key"]: series for m, series in stack.matrix('asap_distinct{method="exact"}')}
    ends = set.intersection(*[set(v) for v in raw.values()]) if raw else set()
    end = max(ends, default=None)
    raw_errs = {k: abs(raw[k][end] - exact[k][end]) / exact[k][end]
                for k in raw if end in exact.get(k, {})} if end else {}
    yield check("store and query: distinct counts from loki-raw, error <= 1% for every key",
                raw_errs and len(raw_errs) == len(raw) and max(raw_errs.values()) <= 0.01,
                ", ".join(f"{k} {e:.2%}" for k, e in sorted(raw_errs.items())) or "no raw distinct counts")
    events = stack.events('| event_kind="cardinality" | method="sketch"')
    yield check("class events in Loki", any("identifier-like" in e for e in events),
                f"{len(events)} sketch events")


def drift_checks(stack, t):
    seen = stack.vector("max_over_time(asap_drift[10m])")
    hit = {(m["http_route"], m["method"]) for m, v in seen if v == 1}
    yield check("route-3 drift detected by sketch and exact",
                {("http.route-3", "sketch"), ("http.route-3", "exact")} <= hit, str(sorted(hit)))
    yield check("no other route flagged (no false positives)",
                all(r == "http.route-3" for r, _ in hit), f"{len(seen)} series watched")
    events = stack.events('| event_kind="drift" | method="sketch"')
    yield check("drift event in Loki", any("http.route-3" in e and "drifted" in e for e in events),
                events[0] if events else "none")


def topk_checks(stack, t):
    new = stack.vector('max_over_time(asap_topk{key="/new/hot-route",bound=~"estimate|"}[10m])')
    yield check("new route in the sketch and exact top-10s", {m["method"] for m, _ in new} >= {"sketch", "exact"},
                str(sorted(m["method"] for m, _ in new)))
    top10 = 'rank=~"[1-9]|10"'
    top = {m: {r["key"] for r, _ in stack.vector(f'asap_topk{{method="{m}",bound=~"estimate|",{top10}}} @ {t}')}
           for m in ("sketch", "exact")}
    recall = len(top["sketch"] & top["exact"]) / max(1, len(top["exact"]))
    yield check("sketch top-10 recall vs exact >= 0.9", recall >= 0.9, f"recall {recall:.2f}")
    hh = {m["__name__"]: v for m, v in stack.vector(
        f'{{__name__=~"asap_hh_(precision|recall|are)",method="sketch"}} @ {t}')}
    yield check("heavy hitters (>= 1% of points): precision and recall >= 0.9",
                hh.get("asap_hh_precision", 0) >= 0.9 and hh.get("asap_hh_recall", 0) >= 0.9,
                ", ".join(f"{k.removeprefix('asap_hh_')} {v:.3f}" for k, v in sorted(hh.items())) or "no asap_hh gauges")
    raw = {m["__name__"]: v for m, v in stack.vector(
        '{__name__=~"asap_hh_(precision|recall)",method="raw"}')}
    loki = stack.vector('max_over_time(asap_query_seconds{method="raw",backend="loki"}[5m])')
    yield check("store and query: top-k from loki-raw, heavy-hitter recall >= 0.9",
                loki and raw.get("asap_hh_recall", 0) >= 0.9,
                ", ".join(f"{k.removeprefix('asap_hh_')} {v:.3f}" for k, v in sorted(raw.items()))
                + (f"; Loki query {loki[0][1]:.2f} s" if loki else "; no Loki query timed"))
    events = stack.events('| event_kind="novelty" | method="sketch"')
    flagged = stack.vector('max_over_time(asap_topk_new{key="/new/hot-route"}[10m])')
    yield check("novelty event in Loki", any("/new/hot-route" in e for e in events),
                next((e for e in events if "/new/hot-route" in e),
                     f"none; {len(events)} sketch novelty events, "
                     f"asap_topk_new gauge from {sorted(m['method'] for m, _ in flagged)}"))


CASES = {
    # use case: (series filter for point counts, checks, seconds to wait)
    "latency": ('attribute_key=""', latency_checks, 5 * WINDOW_S + 40),
    "cardinality": ('attribute_key="http.route"', cardinality_checks, 600 // SCALE + 6 * WINDOW_S + 40),
    "drift": ('attribute_key=""', drift_checks, 600 // SCALE + 3 * WINDOW_S + 40),
    "topk": ('attribute_key="http.route"', topk_checks, 600 // SCALE + 3 * WINDOW_S + 40),
}


def run_case(args, use_case):
    with tempfile.TemporaryDirectory(prefix=f"asap-{use_case}-") as tmp:
        os.chmod(tmp, 0o755)
        pipelines, common = Path(tmp) / use_case, Path(tmp) / "common"
        scaled_copy(use_case, pipelines)
        scaled_copy("common", common)
        for f in [pipelines, common, *pipelines.iterdir(), *common.iterdir()]:
            f.chmod(0o755 if f.is_dir() else 0o644)
        stack = Stack(shlex.split(args.docker_command), use_case, pipelines, common,
                      args.prometheus, args.loki)
        stack.compose("down", "--remove-orphans")
        stack.compose("up", "-d")
        try:
            flt, checks, wait = CASES[use_case]
            print(f"== {use_case}: running for {wait}s", flush=True)
            time.sleep(wait)
            t = latest_window(stack, flt)
            results = list(common_checks(stack, flt)) + list(checks(stack, t))
        finally:
            stack.compose("down", "--remove-orphans")
    for r in results:
        print(f"  [{'PASS' if r['ok'] else 'FAIL'}] {r['check']}: {r['detail']}", flush=True)
    return [dict(r, use_case=use_case) for r in results]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--use-cases", nargs="+", choices=USE_CASES, default=list(USE_CASES))
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    parser.add_argument("--loki", default="http://127.0.0.1:3100")
    parser.add_argument("--build", action="store_true", help="build the asap-collector image first")
    parser.add_argument("--report", type=Path, help="write all results as JSON")
    args = parser.parse_args()
    if args.build:
        env = dict(os.environ, DOCKER_COMMAND=args.docker_command)
        subprocess.run([str(HERE / "run.sh"), "--build-only"], env=env, check=True)
    results = [r for case in args.use_cases for r in run_case(args, case)]
    if args.report:
        args.report.write_text(json.dumps(results, indent=2) + "\n")
    failed = [r for r in results if not r["ok"]]
    print(f"{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
