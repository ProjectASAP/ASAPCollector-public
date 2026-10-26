#!/usr/bin/env python3
"""Query job: the answer step of the store-and-query baseline, and the backend
query latency of every approach.

Once per window, after the window is complete in each pipeline:

* sketch and exact: times the Prometheus query that reads the window's
  answer (`asap_query_seconds{backend="prometheus"}`).
* raw (store and query): once both raw sinks report the window stored in
  loki-raw and the exact answer is in, computes the answer from the raw log
  records with LogQL and times it (`asap_query_seconds{backend="loki"}`; the
  whole answer when it takes several queries).
  - topk: one top-k query; reports `asap_topk{method="raw"}` and its
    heavy-hitter accuracy against exact (`asap_hh_*{method="raw"}`, same
    definition as the hh-accuracy node).
  - cardinality: one distinct-count query per attribute key exact counted;
    reports `asap_distinct{method="raw"}`. A query Loki refuses or answers
    with no result is reported as `asap_query_failed{method="raw"}` instead.

With each raw answer it also republishes the sketch and exact answers of the
same window (`asap_compare_topk`, `asap_compare_distinct`,
`asap_compare_class`, labelled `method`; `asap_compare_failed` for refused
keys, with the reason; the cardinality answers of a method whose window holds more than 1% fewer
points than the other's, because a source missed its merge, are left out) and that window's end
(`asap_compare_window_end`, ms). These carry no
timestamp, so a dashboard always finds the latest compared window however far
store and query lags behind.

The raw answer runs once exact has answered the same window, so a slow exact
pipeline delays it; freshness comes from the pipelines' own gauges and is not
affected. A window raw-sink reported with points still unacknowledged (its
50 s timeout) is answered anyway and flagged with `asap_raw_incomplete`.

The other gauges carry the window end as their timestamp, so Prometheus stores them at
the same time as the pipelines' own per-window gauges. The job also exposes
its own CPU time and resident memory. Only the top-k and cardinality use cases
have a raw answer here; for the others it only times the backend queries.

Configuration (environment): USE_CASE, WINDOW_SECONDS (60), K (20), PHI (0.01),
RAW_SOURCES (2), PROMETHEUS, LOKI_RAW, PORT (9100).
"""
import json
import os
import resource
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROMETHEUS = os.environ.get("PROMETHEUS", "http://prometheus:9090")
LOKI_RAW = os.environ.get("LOKI_RAW", "http://loki-raw:3100")
USE_CASE = os.environ.get("USE_CASE", "topk")
WINDOW_S = int(os.environ.get("WINDOW_SECONDS", "60"))
K = int(os.environ.get("K", "20"))
PHI = float(os.environ.get("PHI", "0.01"))
# Largest relative difference between sketch's and exact's points in a window for both to be compared.
POINTS_TOLERANCE = 0.01
RAW_SOURCES = int(os.environ.get("RAW_SOURCES", "2"))
PORT = int(os.environ.get("PORT", "9100"))
ATTRIBUTE = "http.route"
# Loki stores OTLP attributes as structured metadata with dots replaced.
LOKI_LABEL = ATTRIBUTE.replace(".", "_")
LOKI_STREAM = '{service_name="checkout"}'
TOPK_SELECTOR = f'attribute_key="{ATTRIBUTE}",bound="estimate"'
# What a dashboard reads for each use case's answer.
ANSWER_METRIC = {"topk": "asap_topk", "cardinality": "asap_distinct", "latency": "asap_quantile",
                 "drift": "asap_drift_ks"}.get(USE_CASE, "asap_points")

# Latest sample per series: (name, sorted labels) -> (value, timestamp ms).
samples = {}
lock = threading.Lock()


def get(base, path, params, timeout=300):
    url = base + path + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)["data"]["result"]


def prom(expr, at=None):
    params = {"query": expr}
    if at is not None:
        params["time"] = f"{at:.3f}"
    return get(PROMETHEUS, "/api/v1/query", params, timeout=30)


def window_ends(expr):
    """Window ends (s) with a sample of `expr` in the last 15 minutes, with
    the number of series that have one."""
    ends = {}
    for row in prom(f"{expr}[15m]"):
        for t, _ in row["values"]:
            ends[int(float(t))] = ends.get(int(float(t)), 0) + 1
    return ends


def at_window(expr, end):
    """Samples of `expr` stamped exactly at window end `end` (s): an instant
    query would also return series last seen in earlier windows."""
    rows = prom(f"{expr}[{WINDOW_S}s]", at=end)
    return [(r["metric"], float(v)) for r in rows for t, v in r["values"] if int(float(t)) == end]


def put(name, labels, value, end_s):
    """Sets a gauge stamped at window end `end_s`; None leaves it unstamped
    (Prometheus stamps it at the scrape)."""
    with lock:
        samples[(name, tuple(sorted(labels.items())))] = (value, None if end_s is None else end_s * 1000)


def replace(name, rows, end_s):
    """Replaces every sample of `name` with `rows` ((labels, value) pairs)."""
    with lock:
        for key in [k for k in samples if k[0] == name]:
            del samples[key]
    for labels, value in rows:
        put(name, labels, value, end_s)


def heavy_hitter_accuracy(points, exact, reported_list):
    """As `answers::heavy_hitter_accuracy`; None when the exact list may hide
    more heavy hitters."""
    threshold = PHI * points
    if len(exact) >= K and min((c for _, c in exact), default=float("inf")) >= threshold:
        return None
    truth = {k: c for k, c in exact if c >= threshold}
    reported = [(k, c) for k, c in reported_list if c >= threshold]
    errors = [abs(c - truth[k]) / truth[k] for k, c in reported if k in truth]
    tp = len(errors)
    precision = tp / len(reported) if reported else 1.0
    recall = tp / len(truth) if truth else 1.0
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return {"precision": precision, "recall": recall, "f1": f1, "truth": len(truth),
            "reported": len(reported), "are": sum(errors) / tp if tp else None}


def time_prometheus_answer(method, end):
    t0 = time.monotonic()
    at_window(f'{ANSWER_METRIC}{{method="{method}"}}', end)
    put("asap_query_seconds", {"method": method, "backend": "prometheus"}, time.monotonic() - t0, end)


def flag_incomplete(end):
    unacknowledged = sum(v for _, v in at_window('asap_raw_unacknowledged{method="raw"}', end))
    put("asap_raw_incomplete", {"method": "raw"}, 1.0 if unacknowledged > 0 else 0.0, end)


def loki_at_window_end(query, end):
    """Instant LogQL query over window [end - WINDOW_S, end): LogQL ranges are
    (t - W, t], so evaluate at the window's last millisecond."""
    return get(LOKI_RAW, "/loki/api/v1/query", {"query": query, "time": str(end * 1_000_000_000 - 1_000_000)})


def incomplete_methods(points):
    """Methods whose window holds more than POINTS_TOLERANCE fewer points than the other's: a
    source missed their merge, so their counts are too small. Nothing is judged when a side
    has no points yet."""
    sketch, exact = points["sketch"], points["exact"]
    if min(sketch, exact) <= 0 or abs(sketch - exact) <= POINTS_TOLERANCE * max(sketch, exact):
        return set()
    return {"sketch"} if sketch < exact else {"exact"}


def refusal_reason(error):
    """Why a query got no answer, as one line: the HTTP status and Loki's message, or the error."""
    if isinstance(error, urllib.error.HTTPError):
        message = error.read().decode("utf-8", "replace").strip().splitlines()
        return f"HTTP {error.code} {error.reason}" + (f": {message[0][:200]}" if message else "")
    return f"{type(error).__name__}: {error}"


def raw_distinct(end):
    """Distinct values per attribute key of the window from loki-raw; True when done."""
    exact = at_window('asap_distinct{method="exact"}', end)
    if not exact:
        return False
    flag_incomplete(end)
    failed = []
    compared = [({"method": m, "attribute_key": r["attribute_key"]}, v) for m in ("sketch", "exact")
                for r, v in at_window(f'asap_distinct{{method="{m}",bound="estimate"}}', end)]
    points = {m: max((v for _, v in at_window(f'asap_points{{method="{m}"}}', end)), default=0.0)
              for m in ("sketch", "exact")}
    incomplete = incomplete_methods(points)
    compared = [(labels, v) for labels, v in compared if labels["method"] not in incomplete]
    classes = [({"method": m, "attribute_key": r["attribute_key"], "state": r["state"]}, 1.0)
               for m in ("sketch", "exact")
               for r, v in at_window(f'asap_cardinality_state{{method="{m}"}}', end)
               if v == 1 and m not in incomplete]
    total = 0.0
    for key in sorted({r["attribute_key"] for r, _ in exact}):
        label = key.replace(".", "_")
        query = f"count(sum by ({label}) (count_over_time({LOKI_STREAM}[{WINDOW_S}s])))"
        labels = {"method": "raw", "attribute_key": key}
        t0 = time.monotonic()
        try:
            rows = loki_at_window_end(query, end)
            if not rows:
                raise ValueError("no result from loki-raw")
        except Exception as e:  # refused, timed out or empty: no answer for this key
            total += time.monotonic() - t0
            reason = refusal_reason(e)
            print(f"query_job: {key} at {end}: {reason}", flush=True)
            put("asap_query_failed", labels, 1.0, end)
            failed.append(({"attribute_key": key, "reason": reason}, 1.0))
            continue
        total += time.monotonic() - t0
        distinct = float(rows[0]["value"][1])
        put("asap_distinct", dict(labels, bound="estimate"), distinct, end)
        compared.append((labels, distinct))
    put("asap_query_seconds", {"method": "raw", "backend": "loki"}, total, end)
    replace("asap_compare_distinct", compared, None)
    replace("asap_compare_class", classes, None)
    replace("asap_compare_failed", failed, None)
    put("asap_compare_window_end", {}, end * 1000.0, None)
    return True


def raw_topk(end):
    """Top-k of window [end - WINDOW_S, end) from loki-raw; True when done."""
    exact_rows = at_window(f'asap_topk{{method="exact",{TOPK_SELECTOR}}}', end)
    exact_points = sum(v for _, v in at_window(
        f'asap_points{{method="exact",attribute_key="{ATTRIBUTE}"}}', end))
    if not exact_rows or not exact_points:
        return False
    flag_incomplete(end)
    query = (f"topk({K}, sum by ({LOKI_LABEL}) "
             f"(count_over_time({LOKI_STREAM}[{WINDOW_S}s])))")
    t0 = time.monotonic()
    rows = loki_at_window_end(query, end)
    latency = time.monotonic() - t0
    put("asap_query_seconds", {"method": "raw", "backend": "loki"}, latency, end)
    top = sorted(((r["metric"].get(LOKI_LABEL, ""), float(r["value"][1])) for r in rows),
                 key=lambda kc: -kc[1])
    replace("asap_topk", [({"method": "raw", "attribute_key": ATTRIBUTE, "bound": "estimate",
                            "rank": str(rank), "key": key}, count) for rank, (key, count) in enumerate(top, 1)], end)
    sketch_rows = at_window(f'asap_topk{{method="sketch",{TOPK_SELECTOR}}}', end)
    replace("asap_compare_topk",
            [({"method": m, "rank": r["rank"], "key": r["key"]}, v)
             for m, rows_ in (("sketch", sketch_rows), ("exact", exact_rows)) for r, v in rows_]
            + [({"method": "raw", "rank": str(rank), "key": key}, count) for rank, (key, count) in enumerate(top, 1)],
            None)
    put("asap_compare_window_end", {}, end * 1000.0, None)
    exact = sorted(((m["key"], v) for m, v in exact_rows), key=lambda kc: -kc[1])
    acc = heavy_hitter_accuracy(exact_points, exact, top)
    labels = {"method": "raw", "attribute_key": ATTRIBUTE, "phi": f"{PHI:g}"}
    if acc is None:
        put("asap_hh_incomplete", labels, 1.0, end)
    else:
        for name, value in acc.items():
            if value is not None:
                put(f"asap_hh_{name}", labels, float(value), end)
    return True


def loop():
    done = {"sketch": set(), "exact": set(), "raw": set()}
    started = time.time()
    while True:
        try:
            for method in ("sketch", "exact"):
                for end in sorted(window_ends(f'asap_answer_delay{{method="{method}"}}')):
                    if end not in done[method] and end > started - 2 * WINDOW_S:
                        time_prometheus_answer(method, end)
                        done[method].add(end)
            raw_answer = {"topk": raw_topk, "cardinality": raw_distinct}.get(USE_CASE)
            if raw_answer:
                ready = window_ends('asap_answer_delay{method="raw"}')
                for end in sorted(e for e, n in ready.items() if n >= RAW_SOURCES):
                    # Accuracy is against the exact answer of the same window.
                    if (end not in done["raw"] and end in done["exact"]
                            and end > started - 2 * WINDOW_S and raw_answer(end)):
                        done["raw"].add(end)
        except Exception as e:  # keep serving; the next round retries
            print(f"query_job: {e}", flush=True)
        time.sleep(1)


def escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def own_metrics():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    with open("/proc/self/statm") as f:
        rss = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    return [f"process_cpu_seconds_total {usage.ru_utime + usage.ru_stime}",
            f"process_resident_memory_bytes {rss}"]


class Metrics(BaseHTTPRequestHandler):
    def do_GET(self):
        lines = own_metrics()
        with lock:
            for (name, labels), (value, ts) in sorted(samples.items()):
                text = ",".join(f'{k}="{escape(v)}"' for k, v in labels)
                lines.append(f"{name}{{{text}}} {value}" + ("" if ts is None else f" {ts}"))
        body = ("\n".join(lines) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Metrics).serve_forever()
