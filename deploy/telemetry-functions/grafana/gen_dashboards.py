"""Builds the top-k and cardinality dashboards in grafana/dashboards:
    python3 grafana/gen_dashboards.py topk grafana/dashboards/topk.json
    python3 grafana/gen_dashboards.py cardinality grafana/dashboards/cardinality.json
"""
import json
import sys

USE_CASE, OUT = sys.argv[1], sys.argv[2]
PROM = {"type": "prometheus", "uid": f"asap-prometheus-{USE_CASE}"}
COLORS = {"sketch": "#5794f2", "exact": "#ff9830", "store and query": "#8e8e8e",
          "raw": "#8e8e8e"}
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


def events(kind, title):
    """Table of the `asap_event` gauges of `kind` in the dashboard's time range."""
    global y
    add({"type": "table", "title": title, "datasource": PROM,
         "description": "Detectors (sketch, exact) and the query job (store and query) write each event as an "
                        "asap_event gauge whose value is its window's end. Newest first.",
         "targets": [{"refId": "A", "format": "table", "instant": True,
                      "expr": f'last_over_time(asap_event{{kind="{kind}"}}[$__range]) * 1000'}],
         "transformations": [{"id": "organize", "options": {
             "includeByName": {"Value": True, "method": True, "message": True},
             "indexByName": {"Value": 0, "method": 1, "message": 2},
             "renameByName": {"Value": "Window end", "method": "Approach", "message": "Event"}}}],
         "options": {"showHeader": True, "sortBy": [{"displayName": "Window end", "desc": True}]},
         "fieldConfig": {"defaults": {"custom": {"align": "auto"}},
                         "overrides": [by_name("Window end", {"id": "unit", "value": "dateTimeAsIso"},
                                               {"id": "custom.width", "value": 190}),
                                       by_name("Approach", {"id": "custom.width", "value": 100})]}}, 24, 8, 0)
    y += 8


def topk():
    global y
    html = ('<table style="width:100%;border-collapse:separate;border-spacing:0 6px;border:none">'
            + flow([box("generator-a/b", "sources A and B", *blue),
                    box("branch-a/b", "Count-Min 4&times;512 + top-20 heap per 1-min window", *blue),
                    box("merge", "both sources&#39; sketches per window", *blue),
                    box("detector", "top-20 with bounds; new heavy hitters", *blue),
                    box("prometheus-sketch", "query: the stored top-20", *backend)])
            + flow([box("exact-generator-a/b", "same points", *orange),
                    box("exact-branch-a/b", "route &rarr; count map per 1-min window", *orange),
                    box("exact-merge", "adds both sources&#39; maps: every key", *orange),
                    box("exact-detector", "exact top-20; new heavy hitters", *orange),
                    box("prometheus-exact", "query: the stored top-20", *backend)])
            + flow([box("raw-generator-a/b", "same points", *grey),
                    box("raw-sink-a/b", "the only stage: forwards every point", *grey),
                    box("prometheus-raw", "stores every raw point, one sample each", *grey),
                    box("query-job", "PromQL top-20 once per window; new heavy hitters", *grey)])
            + "</table>"
            "<p style=\"margin:4px 0 0 0\"><small>Every stage is one pinned container. Traffic grows over 30 minutes: "
            "the rate per source doubles every 5 minutes (5k to 80k points/s), <code>/new/hot-route</code> takes 15% "
            "of traffic at minute 10, and the number of routes grows from 100,000 to 1,000,000 at minute 15. "
            "Each approach writes to its own Prometheus. raw-sink adds a <code>point_slot</code> label so points of one "
            "series at the same millisecond stay separate samples. hh-accuracy compares the sketch with exact; "
            "query-job compares store and query with exact and times every approach's query.</small></p>")
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
                        "answered (query-job republishes sketch and exact for it), which lags behind when "
                        "prometheus-raw is slow; the panel on the right shows which window. Error is (count - exact) / exact; "
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
                                         ("raw", "Store and query (PromQL over prometheus-raw): top-10 routes, latest window")]):
        sel = f'asap_topk{{method="{method}",bound="estimate",rank=~"[1-9]|10"}}'
        add({"type": "bargauge", "title": title, "datasource": PROM,
             "targets": [{"refId": "A", "expr": f"sort_desc({latest(sel)})", "legendFormat": "#{{rank}} {{key}}",
                          "instant": True}],
             "options": {"orientation": "horizontal", "displayMode": "basic",
                         "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
             "fieldConfig": {"defaults": {"decimals": 0, "color": {"mode": "fixed", "fixedColor": COLORS[method]}},
                             "overrides": []}}, 8, 10, 8 * i)
    y += 10

    row("Accuracy against exact")
    ts("Heavy hitters (routes with >= 1% of the window's points): precision, recall, F1",
       [('asap_hh_precision{method="sketch"}', "sketch precision"), ('asap_hh_recall{method="sketch"}', "sketch recall"),
        ('asap_hh_f1{method="sketch"}', "sketch F1"), ('asap_hh_precision{method="raw"}', "store and query precision"),
        ('asap_hh_recall{method="raw"}', "store and query recall"), ('asap_hh_f1{method="raw"}', "store and query F1")],
       unit="percentunit", desc="Truth: exact counts >= 1% of the window's points. Store and query loses accuracy "
       "only when points never reached prometheus-raw.", min0=False)
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
       "pipeline that falls behind delivers less. Store and query: points prometheus-raw acknowledged within 50 s of "
       "the window's end. A pipeline that stops reporting shows a gap after 2 minutes.", names=("sketch", "exact", "store and query"))
    ts("Data shape: distinct routes per window (exact)",
       [('asap_distinct{method="exact",attribute_key="http.route"}', "distinct routes")], unit="short", x=12)
    y += 8



def timing(raw_query):
    global y
    row("From data generated to query result, per window (the same definitions for all three)")
    methods = (("sketch", "sketch"), ("exact", "exact"), ("raw", "store and query"))
    names = tuple(n for _, n in methods)

    def quantiles(metric):
        return [(f'max({metric}{{method="{m}",quantile="{q}"}})', f"{n} p{int(float(q) * 100)}")
                for m, n in methods for q in ("0.5", "0.99")]

    ts("1. Delay before data is available for query (data generated -> available)",
       quantiles("asap_data_available_seconds"), unit="s", w=8, x=0, h=9, last=True, log=True,
       desc="Per point, p50 and p99 over the window's points: from its generation until its window's data can be "
       "queried. Every point first waits for its window to end (up to 60 s). sketch and exact: then until the "
       "detector sent the window's answer to its Prometheus. store and query: then until prometheus-raw "
       "acknowledged the window's last raw point.")
    ts("2. Query latency (the query that returns the window's answer)",
       [(f'max(asap_query_seconds{{method="{m}"}})', n) for m, n in methods],
       unit="s", w=8, x=8, h=9, log=True, names=names, last=True,
       desc="Measured by query-job once per window against each approach's own Prometheus. sketch and exact read "
       f"the stored answer (estimates only, so both read the same series); store and query computes it from every raw sample ({raw_query}). Includes refused and "
       "timed-out queries.")
    ts("3. Query result computed - data generated (1 + 2)",
       quantiles("asap_result_age_seconds"), unit="s", w=8, x=16, h=9, last=True, log=True,
       desc="Per point, p50 and p99: from its generation until the query result that includes it is computed, "
       "i.e. data available plus query latency, as if the query ran as soon as the data was available.")
    y += 9
    for i, method in enumerate(("sketch", "exact")):
        st = lambda metric, stage: f'max(asap_stage_{metric}{{method="{method}",stage="{stage}"}})'
        ts(f"{'Sketch' if method == 'sketch' else 'Exact'}: where the delay after the window's end goes",
           [(st("received", "merge"), "branch + to merge"),
            (f'{st("emitted", "merge")} - {st("received", "merge")}', "merge (waits for both sources)"),
            (f'{st("received", "detector")} - {st("emitted", "merge")}', "to detector"),
            (f'{st("emitted", "detector")} - {st("received", "detector")}', "detector"),
            (f'max(asap_query_seconds{{method="{method}"}})', "Prometheus query")],
           unit="s", stack=True, w=8, x=8 * i,
           desc="Seconds per segment after the window's end, until the answer goes to Prometheus, then the query "
           "that reads it. branch + to merge: "
           "the window's end until the last source's sketch reached the merge. merge: waiting for the other "
           "source's sketch (5 s timeout).")
    ts("Store and query: where the delay after the window's end goes",
       [('max(asap_answer_delay{method="raw"})', "into prometheus-raw (last point acknowledged)"),
        ('max(asap_query_seconds{method="raw"})', "PromQL query")],
       unit="s", stack=True, w=8, x=16,
       desc="The window's end until prometheus-raw acknowledged its last point (or raw-sink's 50 s timeout), "
       "then the PromQL query that computes the answer from the raw samples.")
    y += 8


APPROACHES = [
    # (title, collector stages, its Prometheus and other processes scraped directly)
    ("Sketch", "branch-.|merge|detector", "prometheus-sketch"),
    ("Exact", "exact-(branch-.|merge|detector)", "prometheus-exact"),
    ("Store and query", "raw-sink-.", "prometheus-raw|query-job"),
]


def cpu(sel, jobs):
    return (f'sum by (stage) (rate(cpu_time_total{{job="collectors",stage=~"{sel}",otel_scope_pipeline_id="main"}}[1m]))'
            + (f' or sum by (stage) (rate(process_cpu_seconds_total{{stage=~"{jobs}"}}[1m]))' if jobs else ""))


def mem(sel, jobs):
    return (f'max by (stage) (memory_rss{{job="collectors",stage=~"{sel}"}})'
            + (f' or max by (stage) (process_resident_memory_bytes{{stage=~"{jobs}"}})' if jobs else ""))


def bits(sel, _jobs):
    return (f'sum by (stage) (rate(payload_size_bytes_total{{job="collectors",stage=~"{sel}",'
            f'otel_scope_node_type="receiver"}}[2m])) * 8')


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
                 "Pipeline stages from the engine's CPU time; each approach's Prometheus and query-job from their "
                 "process CPU time. "
                 "total: the whole approach.")
    per_approach("resident memory per component", mem, "bytes", "total: the whole approach.")
    per_approach("bits per second into each stage", bits, "bps",
                 "into branch-a/b, exact-branch-a/b, raw-sink-a/b: raw points from the generators. into merge / "
                 "exact-merge: sketches / exact maps. into detector / exact-detector: merged windows.", log=True)


KEYS = ("http.route", "http.method", "k8s.pod.name", "user.id")


def cardinality():
    global y
    html = ('<table style="width:100%;border-collapse:separate;border-spacing:0 6px;border:none">'
            + flow([box("generator-a/b", "sources A and B", *blue),
                    box("branch-a/b", "HLL per attribute key per 1-min window", *blue),
                    box("merge", "both sources&#39; HLLs per window", *blue),
                    box("detector", "distinct counts with bounds; class per key", *blue),
                    box("prometheus-sketch", "query: the stored counts", *backend)])
            + flow([box("exact-generator-a/b", "same points", *orange),
                    box("exact-branch-a/b", "value &rarr; count map per key per 1-min window", *orange),
                    box("exact-merge", "adds both sources&#39; maps: every value", *orange),
                    box("exact-detector", "exact distinct counts and classes", *orange),
                    box("prometheus-exact", "query: the stored counts", *backend)])
            + flow([box("raw-generator-a/b", "same points", *grey),
                    box("raw-sink-a/b", "the only stage: forwards every point", *grey),
                    box("prometheus-raw", "stores every raw point, one sample each", *grey),
                    box("query-job", "PromQL distinct count per key once per window; classes", *grey)])
            + "</table>"
            "<p style=\"margin:4px 0 0 0\"><small>Every stage is one pinned container. Four attributes: "
            "<code>http.route</code> and <code>http.method</code> stable, <code>k8s.pod.name</code> growing 5%/min, "
            "<code>user.id</code> unique per point. Traffic grows over 30 minutes: the rate per source doubles every "
            "5 minutes (5k to 80k points/s), and at minute 10 <code>http.route</code> explodes from 20 to 20,000 "
            "values. Each approach writes to its own Prometheus; query-job compares store and query with exact "
            "and times every approach&#39;s query.</small></p>"
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
                        "exact for it), which lags behind when prometheus-raw is slow; the panel on the right shows "
                        "which window. Error is (count - exact) / exact; when one method's window holds more than 1% fewer points than the other's (a source missed its merge), that method's count and class are left empty, and so are the errors that compare with it. Class: the detector's verdict from the last 10 windows "
                        "(stable, growing, identifier-like). Store+query refused because: why prometheus-raw refused, "
                        "timed out or returned nothing for the key's PromQL query, which leaves its count empty.",
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
    events("cardinality", "Class changes (events of all three approaches, from Prometheus)")

    row("Accuracy against exact")
    ex = 'max by (attribute_key) (asap_distinct{method="exact"})'

    def same_window(expr, method):
        # Prometheus's lookback would otherwise pair a stale answer with a newer exact window.
        stamp = lambda m: f'max by (attribute_key) (timestamp(asap_distinct{{method="{m}"}}))'
        # A window whose points differ by more than 1% from exact's lost a source: no error for it.
        points = lambda m: ('sum(asap_points{method="raw"})' if m == "raw"
                            else f'max(asap_points{{method="{m}",attribute_key="http.route"}})')
        complete = f"abs({points(method)} - {points('exact')}) <= 0.01 * {points(method)}"
        return (f"({expr}) and on (attribute_key) ({stamp(method)} == on (attribute_key) {stamp('exact')})"
                f" and on () ({complete})")

    for i, method in enumerate(("sketch", "raw")):
        est = f'max by (attribute_key) (asap_distinct{{method="{method}",bound="estimate"}})'
        ts(f"{'Sketch (HLL)' if method == 'sketch' else 'Store and query'}: relative error of the distinct count per key",
           [(same_window(f'abs({est} - on (attribute_key) {ex}) / on (attribute_key) {ex}', method),
             "{{attribute_key}}")],
           unit="percentunit", x=12 * i,
           desc="|estimate - exact| / exact per window; left out when the two windows' points differ by more "
           "than 1% (a source missed a merge)." + (
               " Store and query is exact while every point reaches prometheus-raw." if method == "raw" else ""))
    y += 8

    row("Traffic over time: volume and shape")
    ts("End-to-end throughput: points per second each window delivered to the end of its pipeline",
       [('sum by (method) (last_over_time(asap_points{method=~"sketch|exact",attribute_key="http.route"}[2m])) / 60',
         "{{method}}"),
        ('sum(last_over_time(asap_points{method="raw"}[2m])) / 60', "store and query")],
       unit="short", desc="Both sources together. The generators skip batches a pipeline cannot take, so a "
       "pipeline that falls behind delivers less. Store and query: points prometheus-raw acknowledged within 50 s of "
       "the window's end. A pipeline that stops reporting shows a gap after 2 minutes.", names=("sketch", "exact", "store and query"))
    ts("Data shape: distinct values per key per window (exact), log scale",
       [('max by (attribute_key) (asap_distinct{method="exact"})', "{{attribute_key}}")], unit="short", x=12,
       log=True)
    y += 8


{"topk": topk, "cardinality": cardinality}[USE_CASE]()
timing({"topk": "one PromQL top-k query",
                       "cardinality": "one PromQL distinct count per key, summed"}[USE_CASE])
resources()
TITLES = {"topk": "Heavy hitters: sketch vs exact vs store and query (real collector containers)",
          "cardinality": "Cardinality: sketch vs exact vs store and query (real collector containers)"}
json.dump({"uid": f"asap-{USE_CASE}", "title": TITLES[USE_CASE],
           "schemaVersion": 39, "version": 1, "refresh": "10s", "time": {"from": "now-35m", "to": "now"},
           "panels": panels}, open(OUT, "w"), indent=2)
open(OUT, "a").write("\n")
