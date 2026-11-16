#!/usr/bin/env python3
"""Query job: the answer step of the store-and-query (raw) baseline, and each
approach's timing per window: data available for query, query latency, and
query result computed minus data generated.

Every pipeline writes to its own Prometheus (prometheus-sketch,
prometheus-exact, prometheus-raw). Once per window, after the window is
complete in each pipeline:

* sketch and exact: times the PromQL query that reads the window's answer
  from that pipeline's Prometheus (`asap_query_seconds`).
* raw (store and query): once both raw sinks report the window stored in
  prometheus-raw and the exact answer is in, computes the answer from the raw
  samples with PromQL and times it (`asap_query_seconds`; the whole answer
  when it takes several queries).
  - topk: one top-k query; reports `asap_topk{method="raw"}` and its
    heavy-hitter accuracy against exact (`asap_hh_*{method="raw"}`, same
    definition as the hh-accuracy node).
  - cardinality: one distinct-count query per attribute key exact counted;
    reports `asap_distinct{method="raw"}`. A query Prometheus refuses or
    answers with no result is reported as `asap_query_failed{method="raw"}`.
  The raw pipeline has no detector, so this job also applies the detectors'
  event rules to the raw answers (new heavy hitters against the last
  NOVELTY_WINDOWS top-k lists; cardinality classes) and reports them like the
  detectors do (`asap_event{method="raw"}`, `asap_topk_new`,
  `asap_cardinality_state`).

Timing, per window and approach, from the window's points to its answer
(labels `method`; `quantile` 0.5 and 0.99 over the window's points):

* `asap_data_available_seconds`: point generated -> the window's data is
  available for query: for sketch and exact when the detector sent the
  window's answer to its Prometheus (Prometheus stores it milliseconds later),
  for raw when prometheus-raw acknowledged the window's last raw point (the
  later of the two sources). A point also waits for the rest of its window:
  points arrive evenly over a window, so the q-quantile over the window's
  points is the delay after the window's end plus q * WINDOW_SECONDS.
* `asap_query_seconds`: the query that returns the window's answer, on the
  approach's own Prometheus.
* `asap_result_age_seconds`: point generated -> query result computed, that is
  data available plus query latency, as if the query ran as soon as the data
  was available (the query job runs raw only after exact answered, which would
  add a wait that is not raw's).

Events (`asap_event`) carry the window's end in seconds as their value.

With each raw answer it also republishes the sketch and exact answers of the
same window (`asap_compare_topk`, `asap_compare_distinct`,
`asap_compare_class`, labelled `method`; `asap_compare_failed` for refused
keys, with the reason; the cardinality answers of a method whose window holds more than 1% fewer
points than the other's, because a source missed its merge, are left out) and that window's end
(`asap_compare_window_end`, ms). These carry no
timestamp, so a dashboard always finds the latest compared window however far
store and query lags behind.

The raw answer runs once exact has answered the same window, so a slow exact
pipeline delays it; the timing gauges come from the pipelines' own gauges and is not
affected. A window raw-sink reported with points still unacknowledged (its
50 s timeout) is answered anyway and flagged with `asap_raw_incomplete`.

The other gauges carry the window end as their timestamp, so Prometheus stores them at
the same time as the pipelines' own per-window gauges. The job also exposes
its own CPU time and resident memory. Only the top-k and cardinality use cases
have a raw answer here; for the others it only times the backend queries.

Configuration (environment): USE_CASE, WINDOW_SECONDS (60), K (20), PHI (0.01),
RAW_SOURCES (2), PROMETHEUS (monitoring), PORT (9100).
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
PIPELINE_PROMETHEUS = {m: f"http://prometheus-{m}:9090" for m in ("sketch", "exact", "raw")}
# The raw points' metric in prometheus-raw (OTLP name with dots as underscores).
RAW_METRIC = "http_server_request_duration"
# The detectors' event rules (`novelty_windows`, `cardinality:` in their
# configs; test_query_job.py checks they match).
NOVELTY_WINDOWS = 10
IDENTIFIER_RATIO, GROWTH, CLASS_BASELINE_WINDOWS, MIN_POINTS = 0.9, 0.2, 10, 100
QUANTILES = (0.5, 0.99)
USE_CASE = os.environ.get("USE_CASE", "topk")
WINDOW_S = int(os.environ.get("WINDOW_SECONDS", "60"))
K = int(os.environ.get("K", "20"))
PHI = float(os.environ.get("PHI", "0.01"))
# Largest relative difference between sketch's and exact's points in a window for both to be compared.
POINTS_TOLERANCE = 0.01
RAW_SOURCES = int(os.environ.get("RAW_SOURCES", "2"))
PORT = int(os.environ.get("PORT", "9100"))
ATTRIBUTE = "http.route"
# Prometheus stores OTLP attributes as labels with dots replaced.
RAW_LABEL = ATTRIBUTE.replace(".", "_")
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


def prom(expr, at=None, base=PROMETHEUS, timeout=30):
    params = {"query": expr}
    if at is not None:
        params["time"] = f"{at:.3f}"
    return get(base, "/api/v1/query", params, timeout=timeout)


def window_ends(expr):
    """Window ends (s) with a sample of `expr` in the last 15 minutes, with
    the number of series that have one."""
    ends = {}
    for row in prom(f"{expr}[15m]"):
        for t, _ in row["values"]:
            ends[int(float(t))] = ends.get(int(float(t)), 0) + 1
    return ends


def at_window(expr, end, base=PROMETHEUS):
    """Samples of `expr` stamped exactly at window end `end` (s): an instant
    query would also return series last seen in earlier windows."""
    rows = prom(f"{expr}[{WINDOW_S}s]", at=end, base=base)
    return [(r["metric"], float(v)) for r in rows for t, v in r["values"] if int(float(t)) == end]


def put(name, labels, value, end_s):
    """Sets a gauge stamped at window end `end_s`; None leaves it unstamped
    (Prometheus stamps it at the scrape)."""
    with lock:
        samples[(name, tuple(sorted(labels.items())))] = (value, None if end_s is None else end_s * 1000)


def forget_before(ms):
    """Drops stamped samples older than `ms`: Prometheus would refuse them anyway."""
    with lock:
        for key in [k for k, (_, ts) in samples.items() if ts is not None and ts < ms]:
            del samples[key]


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


def report_timing(method, end, after_end_s, query_s):
    """Data available, and query result computed, minus each point's generation:
    `after_end_s` after the window's end plus the point's wait to the window's
    end (quantile q of the window's points waits q * WINDOW_S)."""
    for q in QUANTILES:
        available = after_end_s + q * WINDOW_S
        labels = {"method": method, "quantile": f"{q:g}"}
        put("asap_data_available_seconds", labels, available, end)
        if query_s is not None:
            put("asap_result_age_seconds", labels, available + query_s, end)


def time_prometheus_answer(method, end):
    """Times reading the window's answer from the pipeline's own Prometheus,
    and reports the window's timing."""
    t0 = time.monotonic()
    at_window(f'{ANSWER_METRIC}{{method="{method}"}}', end, base=PIPELINE_PROMETHEUS[method])
    query_s = time.monotonic() - t0
    put("asap_query_seconds", {"method": method}, query_s, end)
    delays = [v for _, v in at_window(f'asap_answer_delay{{method="{method}"}}', end)]
    if delays:
        report_timing(method, end, max(delays), query_s)


def raw_points(end):
    """Points of the window that prometheus-raw acknowledged (both sources)."""
    return sum(v for _, v in at_window('asap_points{method="raw"}', end))


def flag_incomplete(end):
    unacknowledged = sum(v for _, v in at_window('asap_raw_unacknowledged{method="raw"}', end))
    put("asap_raw_incomplete", {"method": "raw"}, 1.0 if unacknowledged > 0 else 0.0, end)


def raw_at_window_end(query, end):
    """Instant PromQL query on prometheus-raw over window [end - WINDOW_S, end):
    PromQL ranges are (t - W, t], so evaluate at the window's last millisecond."""
    return prom(query, at=end - 0.001, base=PIPELINE_PROMETHEUS["raw"], timeout=300)


class RawEvents:
    """The detectors' event rules (`NoveltyTracker`, `CardinalityClassifier` in
    answers.rs) applied to the raw answers."""

    def __init__(self):
        self.tops = []  # top-k key sets of the last NOVELTY_WINDOWS windows
        self.distinct = {}  # attribute key -> distinct counts of the last windows
        self.state = {}  # attribute key -> last class

    def novelty(self, top, end):
        """New heavy hitters of `top` (key, count), ranked, as gauges and events."""
        new = []
        if len(self.tops) >= 3:
            for rank, (key, count) in enumerate(top, 1):
                if any(key in seen for seen in self.tops):
                    continue
                new.append((rank, key, count))
                labels = {"method": "raw", "attribute_key": ATTRIBUTE, "key": key}
                put("asap_topk_new", labels, count, end)
                put("asap_event", dict(labels, kind="novelty", rank=str(rank), count=f"{count:.0f}",
                                       message=f"new heavy hitter {ATTRIBUTE}={key}: rank {rank} with "
                                               f"{count:.0f} points, absent from the top {len(top)} of "
                                               f"the previous {len(self.tops)} windows"), end, end)
        self.tops.append({k for k, _ in top})
        del self.tops[:-NOVELTY_WINDOWS]
        return new

    def cardinality(self, key, distinct, points, end):
        """Class of `key` in this window; reports its state and an event when it changed."""
        if points < MIN_POINTS:
            return None
        window = self.distinct.setdefault(key, [])
        baseline = sum(window) / len(window) if len(window) >= 3 else None
        ratio = distinct / points
        if ratio >= IDENTIFIER_RATIO:
            state = "identifier-like"
        elif baseline is not None and distinct > baseline * (1 + GROWTH):
            state = "growing"
        else:
            state = "stable"
        window.append(distinct)
        del window[:-CLASS_BASELINE_WINDOWS]
        labels = {"method": "raw", "attribute_key": key}
        put("asap_distinct_ratio", labels, ratio, end)
        for s in ("stable", "growing", "identifier-like"):
            put("asap_cardinality_state", dict(labels, state=s), 1.0 if s == state else 0.0, end)
        if self.state.get(key) != state:
            put("asap_event", dict(labels, kind="cardinality", state=state, distinct=f"{distinct:.0f}",
                                   points=f"{points:.0f}",
                                   message=f"attribute {key} is {state}: {distinct:.0f} distinct values "
                                           f"in {points:.0f} points ({ratio:.0%})"), end, end)
        self.state[key] = state
        return state


raw_events = RawEvents()


def incomplete_methods(points):
    """Methods whose window holds more than POINTS_TOLERANCE fewer points than the other's: a
    source missed their merge, so their counts are too small. Nothing is judged when a side
    has no points yet."""
    sketch, exact = points["sketch"], points["exact"]
    if min(sketch, exact) <= 0 or abs(sketch - exact) <= POINTS_TOLERANCE * max(sketch, exact):
        return set()
    return {"sketch"} if sketch < exact else {"exact"}


def refusal_reason(error):
    """Why a query got no answer, as one line: the HTTP status and the backend's message, or the error."""
    if isinstance(error, urllib.error.HTTPError):
        message = error.read(1024).decode("utf-8", "replace").strip().splitlines()
        return f"HTTP {error.code} {error.reason}" + (f": {message[0][:200]}" if message else "")
    return f"{type(error).__name__}: {error}"


def raw_distinct(end):
    """Distinct values per attribute key of the window from prometheus-raw; its query seconds when done, else None."""
    exact = at_window('asap_distinct{method="exact"}', end)
    if not exact:
        return None
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
    window_points = raw_points(end)
    for key in sorted({r["attribute_key"] for r, _ in exact}):
        label = key.replace(".", "_")
        query = f'count(count by ({label}) (count_over_time({RAW_METRIC}{{{label}!=""}}[{WINDOW_S}s])))'

        labels = {"method": "raw", "attribute_key": key}
        t0 = time.monotonic()
        try:
            rows = raw_at_window_end(query, end)
            if not rows:
                raise ValueError("no result from prometheus-raw")
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
        state = raw_events.cardinality(key, distinct, window_points, end)
        if state is not None:
            classes.append(({"method": "raw", "attribute_key": key, "state": state}, 1.0))
    put("asap_query_seconds", {"method": "raw"}, total, end)
    replace("asap_compare_distinct", compared, None)
    replace("asap_compare_class", classes, None)
    replace("asap_compare_failed", failed, None)
    put("asap_compare_window_end", {}, end * 1000.0, None)
    return total


def raw_topk(end):
    """Top-k of window [end - WINDOW_S, end) from prometheus-raw; its query seconds when done, else None."""
    exact_rows = at_window(f'asap_topk{{method="exact",{TOPK_SELECTOR}}}', end)
    exact_points = sum(v for _, v in at_window(
        f'asap_points{{method="exact",attribute_key="{ATTRIBUTE}"}}', end))
    if not exact_rows or not exact_points:
        return None
    flag_incomplete(end)
    query = (f"topk({K}, sum by ({RAW_LABEL}) "
             f"(count_over_time({RAW_METRIC}[{WINDOW_S}s])))")
    t0 = time.monotonic()
    rows = raw_at_window_end(query, end)
    latency = time.monotonic() - t0
    put("asap_query_seconds", {"method": "raw"}, latency, end)
    top = sorted(((r["metric"].get(RAW_LABEL, ""), float(r["value"][1])) for r in rows),
                 key=lambda kc: -kc[1])
    raw_events.novelty(top, end)
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
    return latency


# Windows each method has finished, shared by the two loops.
done = {"sketch": set(), "exact": set(), "raw": set()}
STARTED = time.time()


def recent(end):
    return end > STARTED - 2 * WINDOW_S


def every_second(step):
    """Runs `step` once a second for ever; an error is printed and retried."""
    while True:
        try:
            step()
        except Exception as e:  # keep serving; the next round retries
            print(f"query_job: {e}", flush=True)
        time.sleep(1)


def sketch_and_exact_step():
    for method in ("sketch", "exact"):
        for end in sorted(window_ends(f'asap_answer_delay{{method="{method}"}}')):
            if end not in done[method] and recent(end):
                time_prometheus_answer(method, end)
                done[method].add(end)
    forget_before((time.time() - 15 * WINDOW_S) * 1000)


def raw_step():
    """Raw answers run in their own loop: a raw query that takes minutes or
    hangs must not stop sketch and exact from being measured."""
    raw_answer = {"topk": raw_topk, "cardinality": raw_distinct}.get(USE_CASE)
    ready = window_ends('asap_answer_delay{method="raw"}')
    for end in sorted(e for e, n in ready.items() if n >= RAW_SOURCES):
        # Accuracy is against the exact answer of the same window.
        if end in done["raw"] or not recent(end) or (raw_answer and end not in done["exact"]):
            continue
        query_s = raw_answer(end) if raw_answer else None
        if raw_answer is None or query_s is not None:
            done["raw"].add(end)
            stored = [v for _, v in at_window('asap_answer_delay{method="raw"}', end)]
            report_timing("raw", end, max(stored), query_s)


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
    threading.Thread(target=every_second, args=(sketch_and_exact_step,), daemon=True).start()
    threading.Thread(target=every_second, args=(raw_step,), daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Metrics).serve_forever()
