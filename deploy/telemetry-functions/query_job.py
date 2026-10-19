#!/usr/bin/env python3
"""Query job: the answer step of the store-and-query baseline, and the backend
query latency of every approach.

Once per window, after the window is complete in each pipeline:

* sketch and exact: times the Prometheus query that reads the window's top-k
  answer (`asap_query_seconds{backend="prometheus"}`).
* raw (store and query): once both raw sinks report the window stored in
  loki-raw, computes the top-k from the raw log records with one LogQL query,
  times it (`asap_query_seconds{backend="loki"}`), and reports the answer
  (`asap_topk{method="raw"}`) and its heavy-hitter accuracy against the exact
  pipeline (`asap_hh_*{method="raw"}`, same definition as the hh-accuracy node).

Gauges carry the window end as their timestamp, so Prometheus stores them at
the same time as the pipelines' own per-window gauges. The job also exposes
its own CPU time and resident memory. Only the top-k use case has a raw answer
here; for other use cases it only times the backend queries.

Configuration (environment): USE_CASE, WINDOW_SECONDS (60), K (20), PHI (0.01),
RAW_SOURCES (2), PROMETHEUS, LOKI_RAW, PORT (9100).
"""
import json
import os
import resource
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROMETHEUS = os.environ.get("PROMETHEUS", "http://prometheus:9090")
LOKI_RAW = os.environ.get("LOKI_RAW", "http://loki-raw:3100")
USE_CASE = os.environ.get("USE_CASE", "topk")
WINDOW_S = int(os.environ.get("WINDOW_SECONDS", "60"))
K = int(os.environ.get("K", "20"))
PHI = float(os.environ.get("PHI", "0.01"))
RAW_SOURCES = int(os.environ.get("RAW_SOURCES", "2"))
PORT = int(os.environ.get("PORT", "9100"))
ATTRIBUTE = "http.route"
# Loki stores OTLP attributes as structured metadata with dots replaced.
LOKI_LABEL = ATTRIBUTE.replace(".", "_")
LOKI_STREAM = '{service_name="checkout"}'
TOPK_SELECTOR = f'attribute_key="{ATTRIBUTE}",bound="estimate"'

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
    with lock:
        samples[(name, tuple(sorted(labels.items())))] = (value, end_s * 1000)


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
    at_window(f'asap_topk{{method="{method}",{TOPK_SELECTOR}}}', end)
    put("asap_query_seconds", {"method": method, "backend": "prometheus"}, time.monotonic() - t0, end)


def raw_answer(end):
    """Top-k of window (end - WINDOW_S, end] from loki-raw; True when done."""
    exact_rows = at_window(f'asap_topk{{method="exact",{TOPK_SELECTOR}}}', end)
    exact_points = sum(v for _, v in at_window(
        f'asap_points{{method="exact",attribute_key="{ATTRIBUTE}"}}', end))
    if not exact_rows or not exact_points:
        return False
    query = (f"topk({K}, sum by ({LOKI_LABEL}) "
             f"(count_over_time({LOKI_STREAM}[{WINDOW_S}s])))")
    # The window is [end - W, end) in ms; LogQL ranges are (t - W, t].
    at_ns = end * 1_000_000_000 - 1_000_000
    t0 = time.monotonic()
    rows = get(LOKI_RAW, "/loki/api/v1/query", {"query": query, "time": str(at_ns)})
    latency = time.monotonic() - t0
    put("asap_query_seconds", {"method": "raw", "backend": "loki"}, latency, end)
    top = sorted(((r["metric"].get(LOKI_LABEL, ""), float(r["value"][1])) for r in rows),
                 key=lambda kc: -kc[1])
    with lock:
        for key in [k for k in samples if k[0] == "asap_topk"]:
            del samples[key]
    for rank, (key, count) in enumerate(top, 1):
        put("asap_topk", {"method": "raw", "attribute_key": ATTRIBUTE, "bound": "estimate",
                          "rank": str(rank), "key": key}, count, end)
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
            if USE_CASE == "topk":
                ready = window_ends('asap_answer_delay{method="raw"}')
                for end in sorted(e for e, n in ready.items() if n >= RAW_SOURCES):
                    # Accuracy is against the exact answer of the same window.
                    if (end not in done["raw"] and end in done["exact"]
                            and end > started - 2 * WINDOW_S and raw_answer(end)):
                        done["raw"].add(end)
        except Exception as e:  # keep serving; the next round retries
            print(f"query_job: {e}", flush=True)
        time.sleep(1)


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
                text = ",".join(f'{k}="{v}"' for k, v in labels)
                lines.append(f"{name}{{{text}}} {value} {ts}")
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
