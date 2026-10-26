"""Builds the top-k and cardinality dashboards in grafana/dashboards:
    python3 grafana/gen_dashboards.py topk grafana/dashboards/topk.json
    python3 grafana/gen_dashboards.py cardinality grafana/dashboards/cardinality.json
"""
import json
import sys

USE_CASE, OUT = sys.argv[1], sys.argv[2]
PROM = {"type": "prometheus", "uid": "asap-prometheus"}
LOKI = {"type": "loki", "uid": "asap-loki"}
COLORS = {"sketch": "#5794f2", "exact": "#ff9830", "store and query": "#8e8e8e",
          "raw": "#8e8e8e", "Prometheus (shared)": "#73bf69"}
panels = []
y = 0


def color_overrides(names):
    return [{"matcher": {"id": "byName", "options": n},
             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS[n]}}]}
            for n in names if n in COLORS]


def add(panel, w, h, x):
    panel["gridPos"] = {"h": h, "w": w, "x": x, "y": y}
    panel["id"] = len(panels) + 1
    panels.append(panel)


def row(title):
    global y
    add({"type": "row", "title": title, "collapsed": False, "panels": []}, 24, 1, 0)
    y += 1


def ts(title, targets, unit=None, desc="", stack=False, log=False, w=12, x=0, h=8,
       min0=True, names=(), last=False):
    custom = {"spanNulls": True}
    if stack:
        custom.update({"stacking": {"mode": "normal"}, "fillOpacity": 60,
                       "lineInterpolation": "stepAfter", "lineWidth": 1})
    if log:
        custom["scaleDistribution"] = {"type": "log", "log": 10}
    defaults = {"custom": custom}
    if unit:
        defaults["unit"] = unit
    if min0 and not log:
        defaults["min"] = 0
    add({"type": "timeseries", "title": title, "datasource": PROM, "description": desc,
         "targets": [dict(refId=chr(65 + i), expr=e, legendFormat=l) for i, (e, l) in enumerate(targets)],
         "fieldConfig": {"defaults": defaults, "overrides": color_overrides(names)},
         "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull"]}
                     if last else {"displayMode": "list", "placement": "bottom"},
                     "tooltip": {"mode": "multi", "sort": "none"}}}, w, h, x)


def latest(sel):
    """Only the series of the latest window (older keys stay within the
    lookback otherwise)."""
    return f"{sel} and (timestamp({sel}) == scalar(max(timestamp({sel}))))"


def box(name, small, color, bg):
    return (f'<td style="border:none;padding:0"><div style="border:1px solid {color};border-radius:6px;'
            f'padding:5px 8px;text-align:center;background:{bg}"><b>{name}</b><br><small>{small}</small></div></td>')


ARROW = '<td style="border:none;padding:0 6px;color:#8e8e8e;text-align:center;white-space:nowrap">&rarr;</td>'


def flow(cells):
    return "<tr>" + ARROW.join(cells) + "</tr>"


backend = ("#6e7681", "none")
blue, orange, grey = ("#5794f2", "rgba(87,148,242,0.12)"), ("#ff9830", "rgba(255,152,48,0.10)"), ("#8e8e8e", "rgba(142,142,142,0.12)")


def by_name(name, *properties):
    return {"matcher": {"id": "byName", "options": name}, "properties": list(properties)}


def colored(name, method):
    return by_name(name, {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS[method]}},
                   {"id": "custom.cellOptions", "value": {"type": "color-text"}})


def window_compared(h):
    add({"type": "stat", "title": "Window compared (end)", "datasource": PROM,
         "description": "End of the window the table compares: the latest one store and query has answered.",
         "targets": [{"refId": "A", "expr": "asap_compare_window_end", "instant": True}],
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "colorMode": "none", "graphMode": "none", "textMode": "value"},
         "fieldConfig": {"defaults": {"unit": "dateTimeAsIsoNoDateIfToday"}, "overrides": []}}, 4, h, 20)


def topk():
    global y
    html = ('<table style="width:100%;border-collapse:separate;border-spacing:0 6px;border:none">'
            + flow([box("generator-a/b", "sources A and B", *blue),
                    box("branch-a/b", "Count-Min 4&times;512 + top-20 heap per 1-min window", *blue),
                    box("merge", "both sources&#39; sketches per window", *blue),
                    box("detector", "top-20 with bounds; new heavy hitters", *blue),
                    box("Prometheus", "query: method=&quot;sketch&quot;", *backend)])
            + flow([box("exact-generator-a/b", "same points", *orange),
                    box("exact-branch-a/b", "route &rarr; count map per 1-min window", *orange),
                    box("exact-merge", "adds both sources&#39; maps: every key", *orange),
                    box("exact-detector", "exact top-20; new heavy hitters", *orange),
                    box("Prometheus", "query: method=&quot;exact&quot;", *backend)])
            + flow([box("raw-generator-a/b", "same points", *grey),
                    box("raw-sink-a/b", "the only stage: every point into the log store", *grey),
                    box("loki-raw", "stores every raw point", *grey),
                    box("query-job", "LogQL top-20 once per window", *grey),
                    box("Prometheus", "answer: method=&quot;raw&quot;", *backend)])
            + "</table>"
            "<p style=\"margin:4px 0 0 0\"><small>Every stage is one pinned container. Traffic grows over 30 minutes: "
            "the rate per source doubles every 5 minutes (5k to 80k points/s), <code>/new/hot-route</code> takes 15% "
            "of traffic at minute 10, and the number of routes grows from 100,000 to 1,000,000 at minute 15. "
            "hh-accuracy compares the sketch with exact; query-job compares store and query with exact and times "
            "every approach's backend query.</small></p>")
    add({"type": "text", "title": "Three approaches to the same question: which routes are heavy hitters, and which are new?",
         "options": {"mode": "html", "content": html}}, 24, 10, 0)
    y += 10

    row("Answers")
    TOP10 = 'rank=~"[1-9]|10"'


    def top10(method, label):
        sel = f'asap_compare_topk{{method="{method}",{TOP10}}}'
        return f'max by (key, {label}) (label_replace({sel}, "{label}", "$1", "rank", "(.*)"))'


    def count(method):
        return f'asap_compare_topk{{method="{method}"}}'


    def error(method):
        return (f'100 * (max by (key) ({count(method)}) - on (key) max by (key) ({count("exact")}))'
                f' / on (key) max by (key) ({count("exact")})'
                f' and on (key) ({" or ".join(top10(m, m + "_rank") for m in ("exact", "sketch", "raw"))})')


    compare = [(top10("exact", "exact_rank"), "exact"), (top10("sketch", "sketch_rank"), "sketch"),
               (top10("raw", "raw_rank"), "raw"), (error("sketch"), "sketch_error"), (error("raw"), "raw_error")]
    columns = {"key": "Route", "exact_rank": "Exact rank", "Value #A": "Exact count",
               "sketch_rank": "Sketch rank", "Value #B": "Sketch count", "Value #D": "Sketch error",
               "raw_rank": "Store+query rank", "Value #C": "Store+query count",
               "Value #E": "Store+query error"}




    add({"type": "table", "title": "Top-10 routes of the three approaches, latest window store and query answered (union of their top-10s)",
         "description": "One row per route in any approach's top-10, all from the latest window store and query has "
                        "answered (query-job republishes sketch and exact for it), which lags behind when the log "
                        "store is slow; the panel on the right shows which window. Error is (count - exact) / exact; "
                        "empty when the route is outside that approach's reported top-20.",
         "datasource": PROM,
         "targets": [{"refId": chr(65 + i), "expr": e, "format": "table", "instant": True}
                     for i, (e, _) in enumerate(compare)],
         "transformations": [
             {"id": "joinByField", "options": {"byField": "key", "mode": "outer"}},
             {"id": "organize", "options": {
                 "excludeByName": {f: True for f in ["Time", "Time 1", "Time 2", "Time 3", "Time 4", "Time 5"]},
                 "indexByName": {name: i for i, name in enumerate(columns)},
                 "renameByName": columns}},
             {"id": "convertFieldType", "options": {"conversions": [
                 {"targetField": c, "destinationType": "number"}
                 for c in ["Exact rank", "Sketch rank", "Store+query rank"]]}}],
         "options": {"showHeader": True, "sortBy": [{"displayName": "Exact count", "desc": True}]},
         "fieldConfig": {"defaults": {"decimals": 0, "custom": {"align": "auto"}},
                         "overrides": [
                             by_name("Sketch count", {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS["sketch"]}},
                                     {"id": "custom.cellOptions", "value": {"type": "color-text"}}),
                             by_name("Exact count", {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS["exact"]}},
                                     {"id": "custom.cellOptions", "value": {"type": "color-text"}}),
                             by_name("Store+query count",
                                     {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS["raw"]}},
                                     {"id": "custom.cellOptions", "value": {"type": "color-text"}}),
                             by_name("Route", {"id": "custom.width", "value": 150}),
                             by_name("Sketch error", {"id": "unit", "value": "percent"}, {"id": "decimals", "value": 1}),
                             by_name("Store+query error", {"id": "unit", "value": "percent"},
                                     {"id": "decimals", "value": 1})]}}, 20, 12, 0)
    window_compared(12)
    y += 12
    for i, (method, title) in enumerate([("sketch", "Sketch (CMS + heap): top-10 routes, latest window"),
                                         ("exact", "Exact: top-10 routes, latest window"),
                                         ("raw", "Store and query (LogQL over loki-raw): top-10 routes, latest window")]):
        sel = f'asap_topk{{method="{method}",bound="estimate",rank=~"[1-9]|10"}}'
        add({"type": "bargauge", "title": title, "datasource": PROM,
             "targets": [{"refId": "A", "expr": f"sort_desc({latest(sel)})", "legendFormat": "#{{rank}} {{key}}",
                          "instant": True}],
             "options": {"orientation": "horizontal", "displayMode": "basic",
                         "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
             "fieldConfig": {"defaults": {"decimals": 0, "color": {"mode": "fixed", "fixedColor": COLORS[method]}},
                             "overrides": []}}, 8, 10, 8 * i)
    y += 10
    add({"type": "logs", "title": "New heavy hitters (Loki events from both detectors)", "datasource": LOKI,
         "targets": [{"refId": "A", "expr": '{service_name="asap-detector"} | event_kind="novelty"'}],
         "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}}, 24, 8, 0)
    y += 8

    row("Accuracy against exact")
    ts("Heavy hitters (routes with >= 1% of the window's points): precision, recall, F1",
       [('asap_hh_precision{method="sketch"}', "sketch precision"), ('asap_hh_recall{method="sketch"}', "sketch recall"),
        ('asap_hh_f1{method="sketch"}', "sketch F1"), ('asap_hh_precision{method="raw"}', "store and query precision"),
        ('asap_hh_recall{method="raw"}', "store and query recall"), ('asap_hh_f1{method="raw"}', "store and query F1")],
       unit="percentunit", desc="Truth: exact counts >= 1% of the window's points. Store and query loses accuracy "
       "only when points never reached the log store.", min0=False)
    ts("Average relative count error of the heavy hitters found (ARE)",
       [('asap_hh_are{method="sketch"}', "sketch"), ('asap_hh_are{method="raw"}', "store and query")],
       unit="percentunit", x=12, names=("sketch", "store and query"))
    y += 8

    row("Traffic over time: volume and shape")
    ts("End-to-end throughput: points per second each window delivered to the end of its pipeline",
       [('sum by (method) (last_over_time(asap_points{method=~"sketch|exact",attribute_key="http.route"}[2m])) / 60',
         "{{method}}"),
        ('sum(last_over_time(asap_points{method="raw"}[2m])) / 60', "store and query")],
       unit="short", desc="Both sources together. The generators skip batches a pipeline cannot take, so a "
       "pipeline that falls behind delivers less. Store and query: points loki-raw acknowledged within 50 s of the "
       "window's end. A pipeline that stops reporting shows a gap after 2 minutes.", names=("sketch", "exact", "store and query"))
    ts("Data shape: distinct routes per window (exact)",
       [('asap_distinct{method="exact",attribute_key="http.route"}', "distinct routes")], unit="short", x=12)
    y += 8



def freshness(RAW_QUERY):
    global y
    row("Freshness: from a sample's generation to the queryable answer")
    ts("Total freshness of a window's first sample",
       [('60 + max(asap_answer_delay{method="sketch"}) + max(asap_query_seconds{method="sketch"})', "sketch"),
        ('60 + max(asap_answer_delay{method="exact"}) + max(asap_query_seconds{method="exact"})', "exact"),
        ('60 + max(asap_answer_delay{method="raw"}) + max(asap_query_seconds{method="raw"})', "store and query")],
       unit="s", w=24, h=7, names=("sketch", "exact", "store and query"),
       desc="Window wait (up to 60 s for the first sample) + the pipeline until the answer is stored + the backend "
       "query that returns it.")
    y += 7
    for i, method in enumerate(("sketch", "exact")):
        st = lambda metric, stage: f'max(asap_stage_{metric}{{method="{method}",stage="{stage}"}})'
        ts(f"{'Sketch' if method == 'sketch' else 'Exact'}: latency breakdown per window",
           [('0 * max(asap_answer_delay{method="%s"}) + 60' % method, "window wait"),
            (st("received", "merge"), "branch + to merge"),
            (f'{st("emitted", "merge")} - {st("received", "merge")}', "merge (waits for both sources)"),
            (f'{st("received", "detector")} - {st("emitted", "merge")}', "to detector"),
            (f'{st("emitted", "detector")} - {st("received", "detector")}', "detector"),
            (f'max(asap_query_seconds{{method="{method}"}})', "Prometheus query")],
           unit="s", stack=True, w=8, x=8 * i,
           desc="Seconds per segment. branch + to merge: the window's end until the last source's sketch reached the "
           "merge. merge: waiting for the other source's sketch (5 s timeout).")
    ts("Store and query: latency breakdown per window",
       [('0 * max(asap_answer_delay{method="raw"}) + 60', "window wait"),
        ('max(asap_answer_delay{method="raw"})', "into loki-raw (last point acknowledged)"),
        ('max(asap_query_seconds{method="raw"})', RAW_QUERY)],
       unit="s", stack=True, w=8, x=16,
       desc="Seconds per segment. into loki-raw: the window's end until loki-raw acknowledged its last point (or "
       "raw-sink's 50 s timeout). The query time includes refused and timed-out queries; with several queries per "
       "window it is their sum.")
    y += 8



APPROACHES = [
    # (title, collector stages, process jobs scraped directly)
    ("Sketch", "branch-.|merge|detector", None),
    ("Exact", "exact-(branch-.|merge|detector)", None),
    ("Store and query", "raw-sink-.", "loki-raw|query-job"),
]


def cpu(sel, jobs):
    return (f'sum by (stage) (rate(cpu_time_total{{job="collectors",stage=~"{sel}",otel_scope_pipeline_id="main"}}[1m]))'
            + (f' or sum by (stage) (rate(process_cpu_seconds_total{{job=~"{jobs}"}}[1m]))' if jobs else ""))


def mem(sel, jobs):
    return (f'max by (stage) (memory_rss{{job="collectors",stage=~"{sel}"}})'
            + (f' or max by (stage) (process_resident_memory_bytes{{job=~"{jobs}"}})' if jobs else ""))


def bits(sel, jobs):
    return (f'sum by (stage) (rate(payload_size_bytes_total{{job="collectors",stage=~"{sel}",'
            f'otel_scope_node_type="receiver"}}[2m])) * 8'
            + (' or label_replace(sum(rate(loki_distributor_bytes_received_total[2m])) * 8, '
               '"stage", "loki-raw", "", "")' if jobs else ""))


def per_approach(title, query, unit, desc, log=False):
    global y
    for i, (name, sel, jobs) in enumerate(APPROACHES):
        expr = query(sel, jobs)
        targets = [(expr, "{{stage}}")]
        if query is not bits:
            targets.append((f"sum({expr})", "total"))
        ts(f"{name}: {title}", targets, unit=unit, log=log, w=8, x=8 * i, h=12, desc=desc, last=True)
    y += 12


def resources():
    global y
    row("Resources per approach (generators and observers excluded)")
    per_approach("CPU cores per component", cpu, "short",
                 "Pipeline stages from the engine's CPU time; loki-raw and query-job from their process CPU time. "
                 "total: the whole approach.")
    per_approach("resident memory per component", mem, "bytes", "total: the whole approach.")
    per_approach("bits per second into each stage", bits, "bps",
                 "into branch-a/b, exact-branch-a/b, raw-sink-a/b: raw points from the generators. into merge / "
                 "exact-merge: sketches / exact maps. into detector / exact-detector: merged windows. into loki-raw: "
                 "raw log records.", log=True)
    ts("Shared by all three: Prometheus CPU cores",
       [('sum(rate(process_cpu_seconds_total{job="prometheus"}[1m]))', "Prometheus")], unit="short",
       desc="Prometheus holds the answers of all three approaches and the monitoring data.")
    ts("Shared by all three: Prometheus resident memory",
       [('max(process_resident_memory_bytes{job="prometheus"})', "Prometheus")], unit="bytes", x=12)
    y += 8


KEYS = ("http.route", "http.method", "k8s.pod.name", "user.id")


def cardinality():
    global y
    html = ('<table style="width:100%;border-collapse:separate;border-spacing:0 6px;border:none">'
            + flow([box("generator-a/b", "sources A and B", *blue),
                    box("branch-a/b", "HLL per attribute key per 1-min window", *blue),
                    box("merge", "both sources&#39; HLLs per window", *blue),
                    box("detector", "distinct counts with bounds; class per key", *blue),
                    box("Prometheus", "query: method=&quot;sketch&quot;", *backend)])
            + flow([box("exact-generator-a/b", "same points", *orange),
                    box("exact-branch-a/b", "value &rarr; count map per key per 1-min window", *orange),
                    box("exact-merge", "adds both sources&#39; maps: every value", *orange),
                    box("exact-detector", "exact distinct counts and classes", *orange),
                    box("Prometheus", "query: method=&quot;exact&quot;", *backend)])
            + flow([box("raw-generator-a/b", "same points", *grey),
                    box("raw-sink-a/b", "the only stage: every point into the log store", *grey),
                    box("loki-raw", "stores every raw point", *grey),
                    box("query-job", "LogQL distinct count per key once per window", *grey),
                    box("Prometheus", "answer: method=&quot;raw&quot;", *backend)])
            + "</table>"
            "<p style=\"margin:4px 0 0 0\"><small>Every stage is one pinned container. Four attributes: "
            "<code>http.route</code> and <code>http.method</code> stable, <code>k8s.pod.name</code> growing 5%/min, "
            "<code>user.id</code> unique per point. Traffic grows over 30 minutes: the rate per source doubles every "
            "5 minutes (5k to 80k points/s), and at minute 10 <code>http.route</code> explodes from 20 to 20,000 "
            "values. query-job compares store and query with exact and times every approach&#39;s backend "
            "query.</small></p>"
            "<p style=\"margin:4px 0 0 0\"><small><b>Class</b> (per key and window, from the last 10 windows; "
            "exact and sketch use the same rules on their own counts, so a different class means the sketch's error "
            "crossed a threshold): <b>identifier-like</b> when distinct / points is at least 0.9, i.e. nearly every "
            "point has a new value; else <b>growing</b> when distinct is more than 20% above the mean of the last "
            "10 windows (needs 3); else <b>stable</b>. Not classified below 100 points.</small></p>")
    add({"type": "text", "title": "Three approaches to the same question: how many distinct values does each attribute have?",
         "options": {"mode": "html", "content": html}}, 24, 11, 0)
    y += 11

    row("Answers")

    def distinct(method):
        sel = f'asap_compare_distinct{{method="{method}"}}'
        return f"max by (attribute_key) ({sel})"

    def error(method):
        return f'100 * ({distinct(method)} - on (attribute_key) {distinct("exact")}) / on (attribute_key) {distinct("exact")}'

    def state(method, label):
        sel = f'asap_compare_class{{method="{method}"}}'
        return f'max by (attribute_key, {label}) (label_replace({sel}, "{label}", "$1", "state", "(.*)"))'

    compare = [distinct("exact"), distinct("sketch"), distinct("raw"), error("sketch"), error("raw"),
               state("exact", "exact_class"), state("sketch", "sketch_class"),
               "max by (attribute_key, reason) (asap_compare_failed)"]
    columns = {"attribute_key": "Attribute key", "Value #A": "Exact distinct", "exact_class": "Exact class",
               "Value #B": "Sketch estimate", "Value #D": "Sketch error", "sketch_class": "Sketch class",
               "Value #C": "Store+query", "Value #E": "Store+query error", "reason": "Store+query refused because"}
    hidden = ["Time", "Value #F", "Value #G", "Value #H"] + [f"Time {i}" for i in range(1, 9)]
    add({"type": "table", "title": "Distinct values per attribute key, latest window store and query answered",
         "description": "All from the latest window store and query has answered (query-job republishes sketch and "
                        "exact for it), which lags behind when the log store is slow; the panel on the right shows "
                        "which window. Error is (count - exact) / exact; when exact's window holds more than 1% fewer or more points than the sketch's (a source missed the exact merge), exact and the errors are left empty. Class: the detector's verdict from the last 10 windows "
                        "(stable, growing, identifier-like). Store+query refused because: why Loki refused, timed out "
                        "or returned nothing for the key's LogQL query, which leaves its count empty.",
         "datasource": PROM,
         "targets": [{"refId": chr(65 + i), "expr": e, "format": "table", "instant": True}
                     for i, e in enumerate(compare)],
         "transformations": [
             {"id": "joinByField", "options": {"byField": "attribute_key", "mode": "outer"}},
             {"id": "organize", "options": {
                 "excludeByName": {f: True for f in hidden},
                 "indexByName": {name: i for i, name in enumerate(columns)},
                 "renameByName": columns}}],
         "options": {"showHeader": True, "sortBy": [{"displayName": "Exact distinct", "desc": True}]},
         "fieldConfig": {"defaults": {"decimals": 0, "custom": {"align": "auto"}},
                         "overrides": [colored("Exact distinct", "exact"), colored("Sketch estimate", "sketch"),
                                       colored("Store+query", "raw"),
                                       by_name("Sketch error", {"id": "unit", "value": "percent"},
                                               {"id": "decimals", "value": 2}),
                                       by_name("Store+query error", {"id": "unit", "value": "percent"},
                                               {"id": "decimals", "value": 2})]}}, 20, 7, 0)
    window_compared(7)
    y += 7
    add({"type": "logs", "title": "Class changes (Loki events from both detectors)", "datasource": LOKI,
         "targets": [{"refId": "A", "expr": '{service_name="asap-detector"} | event_kind="cardinality"'}],
         "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}}, 24, 8, 0)
    y += 8

    row("Accuracy against exact")
    ex = 'max by (attribute_key) (asap_distinct{method="exact"})'

    def same_window(expr, method):
        # Prometheus's lookback would otherwise pair a stale answer with a newer exact window.
        stamp = lambda m: f'max by (attribute_key) (timestamp(asap_distinct{{method="{m}"}}))'
        return f"({expr}) and on (attribute_key) ({stamp(method)} == on (attribute_key) {stamp('exact')})"

    for i, method in enumerate(("sketch", "raw")):
        est = f'max by (attribute_key) (asap_distinct{{method="{method}",bound="estimate"}})'
        ts(f"{'Sketch (HLL)' if method == 'sketch' else 'Store and query'}: relative error of the distinct count per key",
           [(same_window(f'abs({est} - on (attribute_key) {ex}) / on (attribute_key) {ex}', method),
             "{{attribute_key}}")],
           unit="percentunit", x=12 * i,
           desc="|estimate - exact| / exact per window." + (
               " Store and query is exact while every point reaches the log store." if method == "raw" else ""))
    y += 8

    row("Traffic over time: volume and shape")
    ts("End-to-end throughput: points per second each window delivered to the end of its pipeline",
       [('sum by (method) (last_over_time(asap_points{method=~"sketch|exact",attribute_key="http.route"}[2m])) / 60',
         "{{method}}"),
        ('sum(last_over_time(asap_points{method="raw"}[2m])) / 60', "store and query")],
       unit="short", desc="Both sources together. The generators skip batches a pipeline cannot take, so a "
       "pipeline that falls behind delivers less. Store and query: points loki-raw acknowledged within 50 s of the "
       "window's end. A pipeline that stops reporting shows a gap after 2 minutes.", names=("sketch", "exact", "store and query"))
    ts("Data shape: distinct values per key per window (exact), log scale",
       [('max by (attribute_key) (asap_distinct{method="exact"})', "{{attribute_key}}")], unit="short", x=12,
       log=True)
    y += 8


{"topk": topk, "cardinality": cardinality}[USE_CASE]()
freshness({"topk": "LogQL top-k query", "cardinality": "LogQL distinct counts (one query per key)"}[USE_CASE])
resources()
TITLES = {"topk": "Heavy hitters: sketch vs exact vs store and query (real collector containers)",
          "cardinality": "Cardinality: sketch vs exact vs store and query (real collector containers)"}
json.dump({"uid": f"asap-{USE_CASE}", "title": TITLES[USE_CASE],
           "schemaVersion": 39, "version": 1, "refresh": "10s", "time": {"from": "now-35m", "to": "now"},
           "panels": panels}, open(OUT, "w"), indent=2)
open(OUT, "a").write("\n")
