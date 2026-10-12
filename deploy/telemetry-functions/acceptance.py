#!/usr/bin/env python3
"""End-to-end acceptance of the telemetry functions on real collector containers.

For each use case: start every stage (generators, branches, merge, detector,
exact baseline, Prometheus, Loki, Grafana) with `docker compose`, wait for the
scenario to play out, then check the sketch answers against the exact ground
truth through Prometheus and Loki. Prints one line per check and exits non-zero
if any check fails.

Time is scaled down 6x so a use case takes a few minutes: 10-second windows, a
2-second merge grace, changes after 1/6 of their configured time and growth 6x
faster per minute. Rules counted in windows (baselines, thresholds) are as
deployed.

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
RATE = 20000  # signals/s per source, as in the generator configs
WINDOW_S = 60 // SCALE
USE_CASES = ("latency", "cardinality", "drift", "topk")
STAGES = ("generator-a", "generator-b", "branch-a", "branch-b", "merge", "detector", "exact")


def scaled_copy(use_case, into):
    """The use case's stage configs with time scaled down SCALE times."""
    for src in (HERE / use_case).glob("*.yaml"):
        text = src.read_text()
        text = text.replace("window_size: 60s", f"window_size: {WINDOW_S}s")
        text = text.replace("window: 60s", f"window: {WINDOW_S}s")
        text = text.replace("{grace: 5s}", "{grace: 2s}")
        text = re.sub(r"after_seconds: (\d+)", lambda m: f"after_seconds: {int(m[1]) // SCALE}", text)
        text = re.sub(r"growth_per_minute: ([0-9.]+)",
                      lambda m: f"growth_per_minute: {float(m[1]) * SCALE:g}", text)
        (into / src.name).write_text(text)


class Stack:
    def __init__(self, docker, pipelines, prometheus, loki):
        self.docker, self.pipelines = docker, pipelines
        self.prometheus, self.loki = prometheus, loki

    def compose(self, *args):
        env = dict(os.environ, PIPELINES=str(self.pipelines))
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
    """Per-window total points (sketch, exact) for one series family."""
    out = {}
    for method in ("sketch", "exact"):
        totals = {}
        for _, samples in stack.matrix(f'asap_points{{method="{method}",{flt}}}'):
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
    expected = 2 * RATE * WINDOW_S
    full = [w["exact"][t] for t in shared]
    yield check("full rate reaches the end (no backpressure)",
                full and all(abs(v - expected) <= 0.01 * expected for v in full),
                f"{len(full)} windows, points/window {sorted(set(int(v) for v in full))} vs {expected}")
    yield check("sketch and exact windows hold the same points",
                shared and all(w["sketch"][t] == w["exact"][t] for t in shared),
                f"{len(shared)} windows compared")


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
    yield check("new route in both top-10s", {m["method"] for m, _ in new} == {"sketch", "exact"},
                str(sorted(m["method"] for m, _ in new)))
    top = {m: {r["key"] for r, _ in stack.vector(f'asap_topk{{method="{m}",bound=~"estimate|"}} @ {t}')}
           for m in ("sketch", "exact")}
    recall = len(top["sketch"] & top["exact"]) / max(1, len(top["exact"]))
    yield check("sketch top-10 recall vs exact >= 0.9", recall >= 0.9, f"recall {recall:.2f}")
    events = stack.events('| event_kind="novelty" | method="sketch"')
    yield check("novelty event in Loki", any("/new/hot-route" in e for e in events),
                next((e for e in events if "/new/hot-route" in e), "none"))


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
        scaled_copy(use_case, Path(tmp))
        for f in Path(tmp).iterdir():
            f.chmod(0o644)
        stack = Stack(shlex.split(args.docker_command), tmp, args.prometheus, args.loki)
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
