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
       min0=True, names=(), last=False, bars=False):
    custom = {"spanNulls": True}
    if bars:
        custom.update({"drawStyle": "bars", "fillOpacity": 80, "lineWidth": 1})
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


def stat(title, expr, legend, desc, w, x, h=4):
    add({"type": "stat", "title": title, "datasource": PROM, "description": desc,
         "targets": [{"refId": "A", "expr": expr, "legendFormat": legend, "instant": True}],
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "colorMode": "none", "graphMode": "none", "textMode": "value_and_name", "justifyMode": "center"},
         "fieldConfig": {"defaults": {"unit": "short", "decimals": 0}, "overrides": []}}, w, h, x)


def traffic(shape_title, shape_expr, shape_legend, shape_desc):
    """The current volume and shape of the input, as numbers."""
    global y
    stat("Input: points per second (both sources)",
         'max(sum by (method) (last_over_time(asap_points{method=~"sketch|exact",attribute_key="http.route"}[2m]))) / 60',
         "points/s", "Points per second of the latest window, both sources together.", 8, 0)
    stat(shape_title, shape_expr, shape_legend, shape_desc, 16, 8)
    y += 4


def gauges(title, exprs, desc, w, x, good_high, h=7):
    """One gauge per series, green toward the good end (accuracy 100%, error 0%)."""
    steps = ([{"color": "red", "value": None}, {"color": "orange", "value": 0.8}, {"color": "green", "value": 0.9}]
             if good_high else
             [{"color": "green", "value": None}, {"color": "orange", "value": 0.02}, {"color": "red", "value": 0.05}])
    add({"type": "gauge", "title": title, "datasource": PROM, "description": desc,
         "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": legend, "instant": True}
                     for i, (e, legend) in enumerate(exprs)],
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "showThresholdLabels": False, "showThresholdMarkers": True},
         "fieldConfig": {"defaults": {"unit": "percentunit", "decimals": 1, "min": 0,
                                      "max": 1 if good_high else 0.1,
                                      "thresholds": {"mode": "absolute", "steps": steps}},
                         "overrides": []}}, w, h, x)


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
    traffic("Data shape: distinct routes in the latest window",
            'max(last_over_time(asap_distinct{method="exact",attribute_key="http.route"}[2m]))', "distinct routes",
            "From the exact pipeline.")

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

    row("Accuracy against exact")
    gauges("Sketch heavy hitters (routes with >= 1% of the window's points), latest window",
           [(f'last_over_time(asap_hh_{m}{{method="sketch"}}[5m])', name)
            for m, name in (("precision", "precision"), ("recall", "recall"), ("f1", "F1"))],
           "Sketch against exact. Truth: exact counts >= 1% of the window's points.", w=16, x=0, good_high=True)
    gauges("Sketch average relative count error of the heavy hitters found (ARE)",
           [('last_over_time(asap_hh_are{method="sketch"}[5m])', "ARE")], "Mean |estimate - exact| / exact.",
           w=8, x=16, good_high=False)
    y += 7




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
    # (name, color key, ingest stages, collector stages after ingest, processes after ingest)
    ("sketch", "sketch", "branch-.", "merge|detector", "prometheus-sketch"),
    ("exact", "exact", "exact-branch-.", "exact-merge|exact-detector", "prometheus-exact"),
    ("store and query", "raw", "raw-sink-.", None, "prometheus-raw|query-job"),
]


def total(*parts):
    """Sum of the parts that exist (a part with no series counts 0)."""
    return " + ".join(f"({p} or vector(0))" for p in parts)


def cpu(stages, processes=None):
    parts = [f'sum(rate(cpu_time_total{{job="collectors",stage=~"{stages}",otel_scope_pipeline_id="main"}}[1m]))']
    if processes:
        parts.append(f'sum(rate(process_cpu_seconds_total{{stage=~"{processes}"}}[1m]))')
    return total(*parts)


def mem(stages, processes):
    return total(f'sum(max by (stage) (memory_rss{{job="collectors",stage=~"{stages}"}}))',
                 f'sum(max by (stage) (process_resident_memory_bytes{{stage=~"{processes}"}}))')


def bars(title, exprs, unit, desc, w, x, h=7):
    """One horizontal bar per approach."""
    add({"type": "bargauge", "title": title, "datasource": PROM, "description": desc,
         "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": name, "instant": True}
                     for i, (name, e) in enumerate(exprs)],
         "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                     "valueMode": "color", "namePlacement": "left",
                     "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
         "fieldConfig": {"defaults": {"unit": unit, "min": 0, "decimals": 2}, "overrides": [
             by_name(name, {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS[key]}})
             for name, key, *_ in APPROACHES]}}, w, h, x)


def resources():
    global y
    row("Resources per approach (generators and monitoring excluded)")
    whole = lambda ingest, after: f"{ingest}|{after}" if after else ingest
    ts("CPU cores of the whole approach over time",
       [(cpu(whole(ingest, after), procs), name) for name, _, ingest, after, procs in APPROACHES],
       unit="short", w=24, h=8, names=("sketch", "exact", "store and query"), last=True,
       desc="Every component of the approach: ingest (branch-a/b, exact-branch-a/b, raw-sink-a/b), merge and "
       "detector, its Prometheus, and for store and query the query job.", bars=True)
    y += 8
    bars("Memory: whole approach",
         [(name, mem(whole(ingest, after), procs)) for name, _, ingest, after, procs in APPROACHES],
         "bytes", "Resident memory of every component of the approach, its Prometheus included.", 12, 0)
    received = lambda stages: (f'sum(rate(payload_size_bytes_total{{job="collectors",stage=~"{stages}",'
                               f'otel_scope_node_type="receiver"}}[2m])) * 8')
    bars("Network: total bits per second of the whole approach",
         [(name, total(received(whole(ingest, after)),
                       *(['sum(rate(asap_raw_sent_bytes{method="raw"}[3m])) * 8'] if name == "store and query" else [])))
          for name, _, ingest, after, _ in APPROACHES],
         "bps", "Every hop of the approach: the raw points from the generators into its first stage, then "
         "sketch and exact: sketches / exact maps into merge and merged windows into detector; store and query: "
         "every raw point from raw-sink into prometheus-raw. The answers into the approach's Prometheus (a few "
         "kilobytes per window) are not counted.", 12, 12)
    y += 7


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
    traffic("Data shape: distinct values per key in the latest window",
            'max by (attribute_key) (last_over_time(asap_distinct{method="exact"}[2m])) or max by (attribute_key) '
            '(last_over_time(asap_distinct{method="sketch",bound="estimate"}[2m]))', "{{attribute_key}}",
            "From the exact pipeline; the sketch's estimate for a key exact has not answered recently.")

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
        points = lambda m: f'max(asap_points{{method="{m}",attribute_key="http.route"}})'
        complete = f"abs({points(method)} - {points('exact')}) <= 0.01 * {points(method)}"
        return (f"({expr}) and on (attribute_key) ({stamp(method)} == on (attribute_key) {stamp('exact')})"
                f" and on () ({complete})")

    est = 'max by (attribute_key) (asap_distinct{method="sketch",bound="estimate"})'
    gauges("Sketch (HLL): relative error of the distinct count per key, latest window compared",
           [(same_window(f'abs({est} - on (attribute_key) {ex}) / on (attribute_key) {ex}', "sketch"),
             "{{attribute_key}}")],
           "|estimate - exact| / exact; empty when the latest windows' points differ by more than 1% (a source "
           "missed a merge) or exact has no answer.", w=24, x=0, good_high=False)
    y += 7



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
