# Telemetry functions on real collector containers

Every stage is the official OTAP dataflow engine (`asap-collector`: the
upstream controller with the ASAP nodes linked in), one container per stage,
each pinned to its own CPU, connected by OTLP/HTTP over the compose network.

Three approaches run side by side over the same input and share no processor:

```text
sketch:          generator-a/b -> branch-a/b (sketch per window) -> merge -> detector -> Prometheus, Loki
exact:           exact-generator-a/b -> exact-branch-a/b (exact window) -> exact-merge -> exact-detector -> Prometheus, Loki
store and query: raw-generator-a/b -> raw-sink-a/b -> loki-raw; query-job runs the answer query over loki-raw
```

The store-and-query pipeline (`method="raw"`) keeps every raw point and
computes the answer later, the way a log store would be used without inline
functions.

The two generators of each pipeline (sources a and b) run the same configs
with the same scenario start (`SCENARIO_START`), and every point is a function
of scenario time, so all three pipelines receive identical points.

| Stage | What it runs |
| --- | --- |
| generator-a/b, exact-generator-a/b, raw-generator-a/b | `scenario_generator`: seeded traffic for the use case (temporary stand-in for the official traffic generator) |
| branch-a/b | `asap_sketches`: one sketch per series per 1-minute window |
| merge | `window_merge`: merges both sources' sketches per window and emits it as soon as both have sent it (5 s timeout for a missing source) |
| detector | `sketch_detector`: answers with bounds, baseline comparisons, events (`method="sketch"`) |
| exact-branch-a/b | `exact_branch`: every value per quantile series and a value -> count map per attribute key, per 1-minute window |
| exact-merge | `exact_merge`: adds both sources' exact windows and emits each as soon as both have sent it (5 s timeout) |
| exact-detector | `exact_detector`: the same answers and events from the exact windows (`method="exact"`): the ground truth |
| raw-sink-a/b | `raw_sink`, the only stage per source: writes every raw point as one log record to `loki-raw` (4 requests in flight) and reports when `loki-raw` acknowledged each window's last point (`source` label), as soon as its source has sent a later point and every message of the window is acknowledged (5 s timeout if the source idles) |
| query-job | `query_job.py`: once per window, times each approach's backend query (`asap_query_seconds`, labelled `method` and `backend`); it also answers from `loki-raw` with LogQL (`method="raw"`): in the top-k use case the top-20 and its heavy-hitter accuracy against exact, in the cardinality use case the distinct count of every attribute key (`asap_query_failed` when Loki refuses a query) |
| hh-accuracy | `heavy_hitter_accuracy`, outside the pipelines: in the top-k use case both detectors send it a copy of their answers; per window it reports heavy-hitter precision, recall, F1 and average relative count error |

**Heavy-hitter accuracy** (top-k use case). A heavy hitter is a route with at
least 1% of the window's points by the exact counts; the sketch reports a
route when its estimate reaches 1%. Per window, `hh-accuracy` emits
`asap_hh_precision` (reported routes that are heavy hitters / reported),
`asap_hh_recall` (heavy hitters reported / heavy hitters), `asap_hh_f1`,
`asap_hh_are` (mean |estimate - exact| / exact over the heavy hitters found),
`asap_hh_truth` and `asap_hh_reported`. Both detectors report their top 20 so
that every heavy hitter is in the exact list (about 10 routes reach 1% here);
a window whose exact top 20 is all above the threshold only gets
`asap_hh_incomplete`.

Each dashboard also shows every pipeline's throughput: the points per window
that reached the end of the pipeline (`asap_points`; for raw, the points
`loki-raw` acknowledged within 50 s of the window's end, after which raw-sink
reports the window anyway) divided by the window length. A pipeline that stops
reporting shows a gap after 2 minutes rather than its last value.

**Latency breakdown.** Merges and detectors report, per window, when they
received its last input and when they emitted it, in seconds after the window's
end (`asap_stage_received` and `asap_stage_emitted`, labelled `method` and
`stage`); detectors pass the merge's timing on to Prometheus. With the query
job's backend query time, the top-k dashboard stacks each approach's freshness:
the window wait (up to 60 s for a window's first sample), branch and transfer to
the merge, the merge's wait for both sources, transfer to the detector, the detector, and the
query. For store and query: the window wait, ingestion into `loki-raw` (last
point acknowledged) and the LogQL query.

**Whole-system cost.** Prometheus also scrapes `loki-raw`, itself and the
query job, so the top-k dashboard shows CPU and memory per component and per
approach: sketch and exact count their four stages, store and query its sinks,
`loki-raw` and the query job; Prometheus, which holds every approach's answers
and the monitoring data, is shown on its own.

The merges count a window's messages, not distinct sources: a source that sends
a window twice (a retried request, or a branch restarted or reconfigured
mid-window) completes it early, and the other source's late input is logged and
dropped. If raw-sink is still waiting for `loki-raw` 50 s after a window's end,
it reports the window anyway with `asap_raw_unacknowledged` (points still
waiting) and that time as its delay; the query job flags such windows with
`asap_raw_incomplete`. The generators size their batches for the scenario's
peak rate, so at the starting rate a batch holds about 60 points instead of
1,024. A cardinality change on a growing attribute applies the growth from the
start of the scenario, not from the change.

Every pipeline reports its window completion delay as `asap_answer_delay`
(seconds from a window's end until its result is complete at the end of the
pipeline): sketch and exact when their detector emits the window's answers,
raw when `loki-raw` has acknowledged the window's last point. raw-sink also
reports each point's time from its event time to that acknowledgement
(`asap_raw_transit`, p50 and p99) and points `loki-raw` rejected
(`asap_raw_failed`).

The top-k and cardinality dashboards are generated by
`grafana/gen_dashboards.py` (`python3 grafana/gen_dashboards.py topk
grafana/dashboards/topk.json`, likewise `cardinality`); edit the script, not
the JSON.

## Use cases

`USE_CASE` selects the stage configs in the directory of the same name.

| `USE_CASE` | Sketch | Scenario | Dashboard |
| --- | --- | --- | --- |
| `latency` | KLL per route | latency over 20 routes | `/d/asap-latency`: p50/p99 with bound vs exact |
| `cardinality` | HLL per attribute key (`per_attribute: ["*"]`) | `user.id` unique, `k8s.pod.name` growing 5%/min, `http.route` and `http.method` fixed; the rate per source doubles every 5 minutes (5k to 80k points/s) and `http.route` explodes from 20 to 20,000 values at minute 10 | `/d/asap-cardinality`: distinct values per key of all three approaches side by side (with error against exact and each detector's class), class events, relative error over time, throughput and distinct values over time, freshness breakdown, CPU/memory/bits per component of each approach |
| `drift` | KLL per route | `http.route-3`'s median triples at minute 10 | `/d/asap-drift`: KS distance from the last 5 windows, drift events |
| `topk` | Count-Min (4 x 512) + top-20 heap per attribute key | Zipf routes; the rate per source doubles every 5 minutes (5k to 80k points/s), a new route takes 15% of traffic at minute 10, routes grow from 100,000 to 1,000,000 at minute 15 | `/d/asap-topk`: top-10 of all three approaches side by side (one table: rank, count and error against exact per approach), heavy-hitter precision/recall/F1/ARE, throughput and distinct routes over time, freshness breakdown, CPU/memory per component and per approach, bits per second into each stage |

**Cardinality classes.** Each detector classifies every attribute key per
window from its last 10 windows (`cardinality:` in `detector.yaml`):
`identifier-like` when distinct / points is at least `identifier_ratio` (0.9),
that is nearly every point has a new value; otherwise `growing` when distinct
is more than `growth` (20%) above the mean of the baseline windows (at least
3); otherwise `stable`. Keys with fewer than `min_points` (default 100, not set in `detector.yaml`) points are not classified.
Exact and sketch apply the same rules to their own distinct counts, so
differing classes show where the sketch's error crosses a threshold.

Answers reach Prometheus over OTLP as `asap_quantile`, `asap_distinct`,
`asap_topk`, `asap_points`, `asap_cardinality_state`, `asap_drift_ks`,
`asap_drift`, `asap_topk_new` and `asap_answer_delay`, labelled
`method="sketch"` or `"exact"` (`asap_answer_delay`, `asap_points` and
`asap_raw_transit` also `"raw"`). Events (class changes, drift start/end, new
heavy hitters) reach Loki as OTLP logs from `service.name=asap-detector`.
Prometheus also scrapes each engine's telemetry
(`/api/v1/metrics?format=prometheus`): CPU time per pipeline, resident memory
and payload bytes per receiver, labelled with the stage.

## Run

Linux, Docker with Compose, 18 free CPUs (0-17 by default; `CPU_BRANCH_A=20`
etc. move a stage) and memory for `loki-raw` (capped at `LOKI_RAW_MEMORY`,
default 16g). From the repository root:

```sh
USE_CASE=cardinality deploy/telemetry-functions/run.sh   # DOCKER_COMMAND="sudo docker" if needed
```

Open `http://localhost:3000/d/asap-<use case>`: one Grafana (`grafana-compose.yaml`,
project `tf-grafana`) shows every demo, and each dashboard reads its own demo's
Prometheus and Loki (`tf-<use case>-prometheus-1`), so a dashboard stays empty
until its demo runs.
The first answers appear about
65 seconds after the first full minute. Stop with
`docker compose -p tf-<use case> -f deploy/telemetry-functions/compose.yaml down`.

The shared Grafana joins each demo's network, so stopping a demo leaves that network in
place until Grafana is stopped too
(`docker compose -p tf-grafana -f deploy/telemetry-functions/grafana-compose.yaml down`).
A demo started before this change runs as project `telemetry-functions` with its own Grafana
on :3000: stop it with `docker compose -p telemetry-functions -f deploy/telemetry-functions/compose.yaml down`
first.

Several demos can run side by side, each a full stack of its own (they share
no Prometheus, so their metrics do not mix). `DEMO_SLOT=N` gives the Nth demo
host ports shifted by N (Prometheus 9090+N, Loki 3100+2N, loki-raw 3101+2N) and
CPUs 18N to 18N+17; `NO_BUILD=1` reuses the built image. Grafana stays on
`:3000` for all of them:

```sh
USE_CASE=cardinality deploy/telemetry-functions/run.sh
NO_BUILD=1 DEMO_SLOT=1 USE_CASE=topk deploy/telemetry-functions/run.sh   # dashboard /d/asap-topk on the same Grafana
docker compose -p tf-topk -f deploy/telemetry-functions/compose.yaml down
```

A demo is one compose project per use case, so a use case runs once at a time: starting it
again restarts it. `acceptance.py` and `experiments.py` use the default project and ports, so stop
the demos before running them. Slots must fit the host: slot N needs CPUs 18N to 18N+17.

Each demo also needs its own `LOKI_RAW_MEMORY` (default 16g) and disk for
`loki-raw`. Stages on different slots can share a physical core when the host
has hyper-threads (CPU n and n+28 on a 56-CPU host), so run one demo at a time
for capacity measurements.

## End-to-end acceptance

```sh
deploy/telemetry-functions/run.sh --build-only
deploy/telemetry-functions/acceptance.py --report acceptance.json
```

Runs all four use cases on the containers with time scaled down 6x (10-second
windows, changes at second 100, at each scenario's starting rate without its
rate and cardinality ramps) and checks the sketch answers against the
exact ground truth through Prometheus and Loki. It exits non-zero if any check
fails. The checks:

| Use case | Checks |
| --- | --- |
| all | every stage up; points per window equal the offered rate (no backpressure); sketch and exact windows hold the same points; every pipeline reports its window completion delay (the raw pipeline's points per window are reported, not required); merges and detectors report their timing; the query job times the sketch and exact backend queries |
| latency | exact p50/p99 inside the sketch's bound (routes with at least 2,000 points) |
| cardinality | sketch and exact give every key the same class; the generated classes are found; distinct-count error at most 2%; store and query answers every key from `loki-raw` within 1% of exact; class events in Loki |
| drift | the changed route drifts in both methods; no other route does; drift event in Loki |
| topk | the new route enters both top-10s; sketch recall@10 at least 0.9; heavy-hitter precision and recall at least 0.9; store and query answers from `loki-raw` with heavy-hitter recall at least 0.9; novelty event in Loki |

CI runs it on every PR (`.github/workflows/ci.yml`) with the stages sharing
the runner's CPUs: a correctness check, not a capacity measurement.

## Experiments

`experiments.py pipelines` runs the three pipelines side by side over offered
rates and records each stage's CPU, memory, delivered rate and window
completion delay (the raw pipeline's per-source stage is raw-sink-a/b);
`experiments.py accuracy-size` measures error
against exact as KLL `k` and Count-Min width change. Results and the
merge/serialization benchmark (`asap-merge-bench`) are in
[docs/results](../../docs/results/).
