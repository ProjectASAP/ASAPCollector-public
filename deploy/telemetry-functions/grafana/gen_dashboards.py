"""Builds the top-k and cardinality dashboards in grafana/dashboards:
    python3 grafana/gen_dashboards.py topk grafana/dashboards/topk.json
    python3 grafana/gen_dashboards.py cardinality grafana/dashboards/cardinality.json
"""
import json
import sys

USE_CASE, OUT = sys.argv[1], sys.argv[2]
PROM = {"type": "prometheus", "uid": f"asap-prometheus-{USE_CASE}"}
COLORS = {"sketch": "#5794f2", "exact": "#ff9830", "raw": "#8e8e8e"}
# What each approach is called on the dashboards (method label -> name).
NAMES = {"sketch": "Sketch", "exact": "Exact", "raw": "Raw"}
panels = []
y = 0


def add(panel, w, h, x):
    panel["gridPos"] = {"h": h, "w": w, "x": x, "y": y}
    panel["id"] = len(panels) + 1
    panels.append(panel)


def row(title):
    global y
    add({"type": "row", "title": title, "collapsed": False, "panels": []}, 24, 1, 0)
    y += 1


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


def approaches(rows, notes):
    """The three approaches, one row each: name and idea on the left, then each
    step as a box (what it does, and its container in small grey), and the
    PromQL query that returns the answer on the right."""
    def label(name, idea, color):
        return (f'<td style="border:none;padding:0 10px 0 0;white-space:nowrap;vertical-align:middle">'
                f'<b style="color:{color}">{name}</b><br><small>{idea}</small></td>')
    def query(promql):
        return (f'<td style="border:none;padding:0 0 0 12px;vertical-align:middle;max-width:360px">'
                f'<small style="color:#8e8e8e">PromQL</small><br><code style="font-size:11px;white-space:normal">'
                f'{promql}</code></td>')
    html = '<table style="width:100%;border-collapse:separate;border-spacing:0 6px;border:none">'
    for name, idea, (color, bg), steps, promql in rows:
        cells = [box(what, f'<span style="color:#8e8e8e">{container}</span>', color, bg) for what, container in steps]
        # Raw has fewer steps: pad so the queries line up in one column.
        pad = '<td style="border:none"></td>' * (2 * (max(len(r[3]) for r in rows) - len(steps)))
        html += "<tr>" + label(name, idea, color) + ARROW.join(cells) + pad + query(promql) + "</tr>"
    return html + "</table>" + "".join(f'<p style="margin:4px 0 0 0"><small>{n}</small></p>' for n in notes)


blue, orange, grey = ("#5794f2", "rgba(87,148,242,0.12)"), ("#ff9830", "rgba(255,152,48,0.10)"), ("#8e8e8e", "rgba(142,142,142,0.12)")


def by_name(name, *properties):
    return {"matcher": {"id": "byName", "options": name}, "properties": list(properties)}


def colored(name, method):
    return by_name(name, {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS[method]}},
                   {"id": "custom.cellOptions", "value": {"type": "color-text"}})


def window_compared(h):
    add({"type": "stat", "title": "Window compared (end)", "datasource": PROM,
         "description": "End of the window the table compares: the latest one Raw has answered.",
         "targets": [{"refId": "A", "expr": "asap_compare_window_end", "instant": True}],
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                     "colorMode": "none", "graphMode": "none", "textMode": "value"},
         "fieldConfig": {"defaults": {"unit": "dateTimeAsIsoNoDateIfToday"}, "overrides": []}}, 4, h, 20)


def events(kind, title):
    """Table of the `asap_event` gauges of `kind` in the dashboard's time range."""
    global y
    add({"type": "table", "title": title, "datasource": PROM,
         "description": "Detectors (sketch, exact) and the query job (Raw) write each event as an "
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
    html = approaches([
        ("Sketch", "summarize each minute at the source", blue, [
            ("Sources A and B", "generator-a/b"),
            ("Summarize each minute: Count-Min sketch + top-20 list", "branch-a/b"),
            ("Combine A and B", "merge"),
            ("Top routes, and which are new", "detector"),
            ("Store the answer", "prometheus-sketch"),
            ("Query: read the answer", "query-job")], "asap_topk{method=&quot;sketch&quot;, bound=&quot;estimate&quot;}[60s]"),
        ("Exact", "count every route each minute", orange, [
            ("Sources A and B (same points)", "exact-generator-a/b"),
            ("Count every route each minute", "exact-branch-a/b"),
            ("Add up A and B", "exact-merge"),
            ("Top routes, and which are new", "exact-detector"),
            ("Store the answer", "prometheus-exact"),
            ("Query: read the answer", "query-job")], "asap_topk{method=&quot;exact&quot;, bound=&quot;estimate&quot;}[60s]"),
        ("Raw", "store every point, compute when asked", grey, [
            ("Sources A and B (same points)", "raw-generator-a/b"),
            ("Forward each point as it arrives (OTLP forwarder, no window)", "raw-sink-a/b"),
            ("Store every point", "prometheus-raw"),
            ("Query: compute the top 20 of the last 60 s", "query-job")], "topk(20, sum by (http_route) (count_over_time(http_server_request_duration[60s])))"),
    ], ["Same input for all three. Traffic grows over 30 minutes: the rate per source doubles every 5 minutes "
        "(5k to 80k points/s), <code>/new/hot-route</code> takes 15% of traffic at minute 10, and the number of "
        "routes grows from 100,000 to 1,000,000 at minute 15. Every step is one container pinned to one CPU; each "
        "approach has its own Prometheus.",
        "Raw adds a <code>point_slot</code> label so points of one route in the same millisecond stay separate "
        "samples (Prometheus keeps one sample per series and millisecond). hh-accuracy compares Sketch with Exact; "
        "query-job compares Raw with Exact and times every approach&#39;s query.",
        "Sketch and Exact read the stored top 20 of the minute (evaluated at the minute&#39;s end, keeping the "
        "samples stamped with it). Raw computes it from every raw point of the minute (evaluated 1 ms before the "
        "minute&#39;s end, because PromQL ranges include their end)."])
    add({"type": "text", "title": "Three ways to answer: which routes get the most requests each minute, and which are new?",
         "options": {"mode": "html", "content": html}}, 24, 10, 0)
    y += 10
    traffic("Data shape: distinct routes in the latest window",
            'max(last_over_time(asap_distinct{method="exact",attribute_key="http.route"}[2m]))', "distinct routes",
            "From the exact pipeline.")

    row("Accuracy against exact")
    gauges("Sketch heavy hitters (routes with >= 1% of the window's points), latest window",
           [(f'last_over_time(asap_hh_{m}{{method="sketch"}}[5m])', name)
            for m, name in (("precision", "precision"), ("recall", "recall"), ("f1", "F1"))],
           "Sketch against exact. Truth: exact counts >= 1% of the window's points.", w=16, x=0, good_high=True)
    gauges("Sketch average relative count error of the heavy hitters found (ARE)",
           [('last_over_time(asap_hh_are{method="sketch"}[5m])', "ARE")], "Mean |estimate - exact| / exact.",
           w=8, x=16, good_high=False)
    y += 7

    def answers():
        global y
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
                   "raw_rank": "Raw rank", "Value #C": "Raw count",
                   "Value #E": "Raw error"}




        add({"type": "table", "title": "Top-10 routes of the three approaches, latest window Raw answered (union of their top-10s)",
             "description": "One row per route in any approach's top-10, all from the latest window Raw has "
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
                     for c in ["Exact rank", "Sketch rank", "Raw rank"]]}}],
             "options": {"showHeader": True, "sortBy": [{"displayName": "Exact count", "desc": True}]},
             "fieldConfig": {"defaults": {"decimals": 0, "custom": {"align": "auto"}},
                             "overrides": [
                                 by_name("Sketch count", {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS["sketch"]}},
                                         {"id": "custom.cellOptions", "value": {"type": "color-text"}}),
                                 by_name("Exact count", {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS["exact"]}},
                                         {"id": "custom.cellOptions", "value": {"type": "color-text"}}),
                                 by_name("Raw count",
                                         {"id": "color", "value": {"mode": "fixed", "fixedColor": COLORS["raw"]}},
                                         {"id": "custom.cellOptions", "value": {"type": "color-text"}}),
                                 by_name("Route", {"id": "custom.width", "value": 150}),
                                 by_name("Sketch error", {"id": "unit", "value": "percent"}, {"id": "decimals", "value": 1}),
                                 by_name("Raw error", {"id": "unit", "value": "percent"},
                                         {"id": "decimals", "value": 1})]}}, 20, 12, 0)
        window_compared(12)
        y += 12

    return answers


def timing(raw_query):
    """One stacked bar per approach, from data generated to the query result:
    window wait (the configured window; none for Raw), time until
    the data is queryable, and the query."""
    global y
    row("From data generated to query result (latest window)")
    def named(e):
        for method, name in NAMES.items():
            e = f'label_replace({e}, "approach", "{name}", "method", "{method}")'
        return e
    latest = lambda m: f'max by (method) (last_over_time({m}[5m]))'
    segments = [
        ("wait for the window to end", latest("asap_window_wait_seconds")),
        ("then until data is queryable",
         f'{latest("asap_data_available_seconds")} - on (method) {latest("asap_window_wait_seconds")}'),
        ("query", 'max by (method) (last_over_time(asap_query_seconds[5m]))'),
    ]
    add({"type": "barchart", "title": "Data generated -> query result: where the time goes", "datasource": PROM,
         "description": "1. Window wait: sketch and exact compute per 1-minute window (the configured window), "
                        "so a window's answer exists only after the window ends; Raw has no window. "
                        "2. Then until the data is queryable: sketch and exact until the detector wrote the window's "
                        "answer to their Prometheus; Raw until prometheus-raw acknowledged the window's "
                        "last point (otlp-forwarder forwards each point as it arrives). 3. The query that returns "
                        "the answer on the approach's own Prometheus: sketch and exact read the stored answer "
                        f"(estimates only); Raw computes it from every raw sample ({raw_query}).",
         "targets": [{"refId": chr(65 + i), "expr": named(e), "format": "table", "instant": True}
                     for i, (_, e) in enumerate(segments)],
         "transformations": [
             {"id": "joinByField", "options": {"byField": "approach", "mode": "outer"}},
             {"id": "organize", "options": {
                 "includeByName": {"approach": True, "Value #A": True, "Value #B": True, "Value #C": True},
                 "indexByName": {"approach": 0, "Value #A": 1, "Value #B": 2, "Value #C": 3},
                 "renameByName": {"Value #A": segments[0][0], "Value #B": segments[1][0],
                                  "Value #C": segments[2][0]}}},
             {"id": "sortBy", "options": {"sort": [{"field": "approach"}]}}],
         "options": {"orientation": "horizontal", "stacking": "normal", "xField": "approach", "showValue": "always",
                     "barWidth": 0.6, "groupWidth": 0.7, "legend": {"displayMode": "list", "placement": "bottom",
                                                                   "showLegend": True},
                     "tooltip": {"mode": "multi", "sort": "none"}},
         "fieldConfig": {"defaults": {"unit": "s", "decimals": 2, "min": 0,
                                      "custom": {"fillOpacity": 85, "lineWidth": 0}},
                         "overrides": [
                             by_name(segments[0][0], {"id": "color", "value": {"mode": "fixed", "fixedColor": "#8e8e8e"}}),
                             by_name(segments[1][0], {"id": "color", "value": {"mode": "fixed", "fixedColor": "#ff9830"}}),
                             by_name(segments[2][0], {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794f2"}})]}},
        24, 8, 0)
    y += 8


APPROACHES = [
    # (name, color key, ingest stages, collector stages after ingest, processes after ingest)
    ("Sketch", "sketch", "branch-.", "merge|detector", "prometheus-sketch"),
    ("Exact", "exact", "exact-branch-.", "exact-merge|exact-detector", "prometheus-exact"),
    ("Raw", "raw", "raw-sink-.", None, "prometheus-raw|query-job"),
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
    bars("CPU cores: whole approach",
         [(name, cpu(whole(ingest, after), procs)) for name, _, ingest, after, procs in APPROACHES], "short",
         "Every component of the approach: ingest (branch-a/b, exact-branch-a/b, otlp-forwarder-a/b), merge and "
         "detector, its Prometheus, and for Raw the query job.", 8, 0)
    bars("Memory: whole approach",
         [(name, mem(whole(ingest, after), procs)) for name, _, ingest, after, procs in APPROACHES],
         "bytes", "Resident memory of every component of the approach, its Prometheus included.", 8, 8)
    received = lambda stages: (f'sum(rate(payload_size_bytes_total{{job="collectors",stage=~"{stages}",'
                               f'otel_scope_node_type="receiver"}}[2m])) * 8')
    bars("Network: total bits per second of the whole approach",
         [(name, total(received(whole(ingest, after)),
                       *(['sum(rate(asap_raw_sent_bytes{method="raw"}[3m])) * 8'] if key == "raw" else [])))
          for name, key, ingest, after, _ in APPROACHES],
         "bps", "Every hop of the approach: the raw points from the generators into its first stage, then "
         "sketch and exact: sketches / exact maps into merge and merged windows into detector; Raw: "
         "every raw point from otlp-forwarder into prometheus-raw. The answers into the approach's Prometheus (a few "
         "kilobytes per window) are not counted.", 8, 16)
    y += 7


KEYS = ("http.route", "http.method", "k8s.pod.name", "user.id")


def cardinality():
    global y
    html = approaches([
        ("Sketch", "summarize each minute at the source", blue, [
            ("Sources A and B", "generator-a/b"),
            ("Summarize each minute: one HyperLogLog per attribute", "branch-a/b"),
            ("Combine A and B", "merge"),
            ("Distinct counts, and each attribute&#39;s class", "detector"),
            ("Store the answer", "prometheus-sketch"),
            ("Query: read the answer", "query-job")], "asap_distinct{method=&quot;sketch&quot;, bound=&quot;estimate&quot;}[60s]"),
        ("Exact", "keep every value each minute", orange, [
            ("Sources A and B (same points)", "exact-generator-a/b"),
            ("Keep every value of every attribute each minute", "exact-branch-a/b"),
            ("Add up A and B", "exact-merge"),
            ("Distinct counts, and each attribute&#39;s class", "exact-detector"),
            ("Store the answer", "prometheus-exact"),
            ("Query: read the answer", "query-job")], "asap_distinct{method=&quot;exact&quot;, bound=&quot;estimate&quot;}[60s]"),
        ("Raw", "store every point, compute when asked", grey, [
            ("Sources A and B (same points)", "raw-generator-a/b"),
            ("Forward each point as it arrives (OTLP forwarder, no window)", "raw-sink-a/b"),
            ("Store every point", "prometheus-raw"),
            ("Query: count distinct values per attribute over the last 60 s", "query-job")], "count(count by (user_id) (count_over_time(http_server_request_duration{user_id!=&quot;&quot;}[60s]))), one per attribute"),
    ], ["Same input for all three. Four attributes: <code>http.route</code> and <code>http.method</code> stable, "
        "<code>k8s.pod.name</code> growing 5%/min, <code>user.id</code> unique per point. Traffic grows over 30 "
        "minutes: the rate per source doubles every 5 minutes (5k to 80k points/s), and at minute 10 "
        "<code>http.route</code> explodes from 20 to 20,000 values. Every step is one container pinned to one CPU; "
        "each approach has its own Prometheus.",
        "<b>Class</b> (per attribute and minute, from the last 10 minutes; every approach applies the same rules to "
        "its own counts): <b>identifier-like</b> when distinct / points is at least 0.9, i.e. nearly every point has "
        "a new value; else <b>growing</b> when distinct is more than 20% above the mean of the last 10 minutes "
        "(needs 3); else <b>stable</b>. Not classified below 100 points.",
        "Sketch and Exact read the stored distinct counts of the minute (evaluated at the minute&#39;s end, keeping "
        "the samples stamped with it). Raw computes each from every raw point of the minute (evaluated 1 ms before "
        "the minute&#39;s end)."])
    add({"type": "text", "title": "Three ways to answer: how many distinct values does each attribute have each minute?",
         "options": {"mode": "html", "content": html}}, 24, 11, 0)
    y += 11
    traffic("Data shape: distinct values per key in the latest window",
            'max by (attribute_key) (last_over_time(asap_distinct{method="exact"}[2m])) or max by (attribute_key) '
            '(last_over_time(asap_distinct{method="sketch",bound="estimate"}[2m]))', "{{attribute_key}}",
            "From the exact pipeline; the sketch's estimate for a key exact has not answered recently.")

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

    def answers():
        global y
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
                   "Value #C": "Raw", "Value #E": "Raw error", "reason": "Raw refused because"}
        hidden = ["Time", "Value #F", "Value #G", "Value #H"] + [f"Time {i}" for i in range(1, 9)]
        add({"type": "table", "title": "Distinct values per attribute key, latest window Raw answered",
             "description": "All from the latest window Raw has answered (query-job republishes sketch and "
                            "exact for it), which lags behind when prometheus-raw is slow; the panel on the right shows "
                            "which window. Error is (count - exact) / exact; when one method's window holds more than 1% fewer points than the other's (a source missed its merge), that method's count and class are left empty, and so are the errors that compare with it. Class: the detector's verdict from the last 10 windows "
                            "(stable, growing, identifier-like). Raw refused because: why prometheus-raw refused, "
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
                                           colored("Raw", "raw"),
                                           by_name("Sketch error", {"id": "unit", "value": "percent"},
                                                   {"id": "decimals", "value": 2}),
                                           by_name("Raw error", {"id": "unit", "value": "percent"},
                                                   {"id": "decimals", "value": 2})]}}, 20, 7, 0)
        window_compared(7)
        y += 7
        events("cardinality", "Class changes (events of all three approaches, from Prometheus)")

    return answers


answers = {"topk": topk, "cardinality": cardinality}[USE_CASE]()
timing({"topk": "one PromQL top-k query",
       "cardinality": "one PromQL distinct count per key, summed"}[USE_CASE])
resources()
answers()
TITLES = {"topk": "Heavy hitters: sketch vs exact vs Raw (real collector containers)",
          "cardinality": "Cardinality: sketch vs exact vs Raw (real collector containers)"}
json.dump({"uid": f"asap-{USE_CASE}", "title": TITLES[USE_CASE],
           "schemaVersion": 39, "version": 1, "refresh": "10s", "time": {"from": "now-35m", "to": "now"},
           "panels": panels}, open(OUT, "w"), indent=2)
open(OUT, "a").write("\n")
