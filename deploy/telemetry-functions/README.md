# Telemetry functions on real collector containers

Every stage is the official OTAP dataflow engine (`asap-collector`: the
upstream controller with the ASAP nodes linked in), one container per stage,
each pinned to its own CPU, connected by OTLP/HTTP over the compose network.
This is the deployment behind [RFC 0001](../../docs/rfcs/0001-sketch-telemetry-functions.md).

Three pipelines run side by side over the same input and share no processor:

```text
sketch: generator-a/b -> branch-a/b (sketch per window) -> merge -> detector -> Prometheus, Loki
exact:  exact-generator-a/b -> exact-branch-a/b (exact window) -> exact-merge -> exact-detector -> Prometheus, Loki
raw:    raw-generator-a/b -> raw-branch-a/b (forward) -> raw-merge (forward) -> raw-sink -> loki-raw
```

The two generators of each pipeline (sources a and b) run the same configs
with the same scenario start (`SCENARIO_START`), and every point is a function
of scenario time, so all three pipelines receive identical points.

| Stage | What it runs |
| --- | --- |
| generator-a/b, exact-generator-a/b, raw-generator-a/b | `scenario_generator`: seeded traffic for the use case (temporary stand-in for the official traffic generator) |
| branch-a/b | `asap_sketches`: one sketch per series per 1-minute window |
| merge | `window_merge`: merges both sources' sketches per window, 5 s after the window ends |
| detector | `sketch_detector`: answers with bounds, baseline comparisons, events (`method="sketch"`) |
| exact-branch-a/b | `exact_branch`: every value per quantile series and a value -> count map per attribute key, per 1-minute window |
| exact-merge | `exact_merge`: adds both sources' exact windows, 5 s after the window ends |
| exact-detector | `exact_detector`: the same answers and events from the exact windows (`method="exact"`): the ground truth |
| raw-branch-a/b, raw-merge | forward raw points (OTLP receiver -> exporter) |
| raw-sink | `raw_sink`: writes every raw point as one log record to `loki-raw` and reports when `loki-raw` acknowledged each window's last point |

Every pipeline reports its window completion delay as `asap_answer_delay`
(seconds from a window's end until its result is complete at the end of the
pipeline): sketch and exact when their detector emits the window's answers,
raw when `loki-raw` has acknowledged the window's last point. raw-sink also
reports each point's time from its event time to that acknowledgement
(`asap_raw_transit`, p50 and p99) and points `loki-raw` rejected
(`asap_raw_failed`).

## Use cases

`USE_CASE` selects the stage configs in the directory of the same name.

| `USE_CASE` | Sketch | Scenario | Dashboard |
| --- | --- | --- | --- |
| `latency` | KLL per route | latency over 20 routes | `/d/asap-latency`: p50/p99 with bound vs exact |
| `cardinality` | HLL per attribute key (`per_attribute: ["*"]`) | `user.id` unique, `k8s.pod.name` growing 5%/min, `http.route` and `http.method` fixed | `/d/asap-cardinality`: distinct values with bound, class per key (stable, growing, identifier-like), events |
| `drift` | KLL per route | `http.route-3`'s median triples at minute 10 | `/d/asap-drift`: KS distance from the last 5 windows, drift events |
| `topk` | Count-Min (4 x 512) + top-10 heap per attribute key | 100,000 Zipf routes; a new route takes 15% of traffic at minute 10 | `/d/asap-topk`: top-10 with bound vs exact, new heavy hitters |

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

Open `http://localhost:3000/d/asap-<use case>`. The first answers appear about
65 seconds after the first full minute. Stop with
`docker compose -f deploy/telemetry-functions/compose.yaml down`.

## End-to-end acceptance

```sh
deploy/telemetry-functions/run.sh --build-only
deploy/telemetry-functions/acceptance.py --report acceptance.json
```

Runs all four use cases on the containers with time scaled down 6x (10-second
windows, changes at second 100) and checks the sketch answers against the
exact ground truth through Prometheus and Loki. It exits non-zero if any check
fails. The checks:

| Use case | Checks |
| --- | --- |
| all | every stage up; points per window equal the offered rate (no backpressure); sketch and exact windows hold the same points; every pipeline reports its window completion delay (the raw pipeline's points per window are reported, not required) |
| latency | exact p50/p99 inside the sketch's bound (routes with at least 2,000 points) |
| cardinality | sketch and exact give every key the same class; the generated classes are found; distinct-count error at most 2%; class events in Loki |
| drift | the changed route drifts in both methods; no other route does; drift event in Loki |
| topk | the new route enters both top-10s; sketch recall@10 at least 0.9; novelty event in Loki |

CI runs it on every PR (`.github/workflows/ci.yml`) with the stages sharing
the runner's CPUs: a correctness check, not a capacity measurement.

## Experiments

`experiments.py pipelines` runs the three pipelines side by side over offered
rates and records each stage's CPU, memory, delivered rate and window
completion delay (the sketch branch's CPU minus the raw branch's is the cost of
the function); `experiments.py accuracy-size` measures error
against exact as KLL `k` and Count-Min width change. Results and the
merge/serialization benchmark (`asap-merge-bench`) are in
[docs/results](../../docs/results/).
