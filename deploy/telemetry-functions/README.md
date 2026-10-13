# Telemetry functions on real collector containers

Every stage is the official OTAP dataflow engine (`asap-collector`: the
upstream controller with the ASAP nodes linked in), one container per stage,
each pinned to its own CPU, connected by OTLP/HTTP over the compose network.
This is the deployment behind [RFC 0001](../../docs/rfcs/0001-sketch-telemetry-functions.md).

```text
generator-a -> branch-a --sketch--> merge -> detector --metrics--> Prometheus -> Grafana
generator-b -> branch-b --sketch--/                   \--events---> Loki ------/
                  \--raw copy (fanout)--> exact (same metrics and events, method="exact")
```

| Stage | What it runs |
| --- | --- |
| generator-a/b | `scenario_generator`: seeded traffic for the use case (temporary stand-in for the official traffic generator) |
| branch-a/b | the official `fanout` copies each message: `asap_sketches` builds one sketch per series per 1-minute window; the raw copy goes to `exact` |
| merge | `window_merge`: merges both sources' sketches per window, 5 s after the window ends |
| detector | `sketch_detector`: answers with bounds, baseline comparisons, events |
| exact | `exact_answers`: the same answers from every raw point: ground truth and exact baseline |

## Use cases

`USE_CASE` selects the stage configs in the directory of the same name.

| `USE_CASE` | Sketch | Scenario | Dashboard |
| --- | --- | --- | --- |
| `latency` | KLL per route | latency over 20 routes | `/d/asap-latency`: p50/p99 with bound vs exact |
| `cardinality` | HLL per attribute key (`per_attribute: ["*"]`) | `user.id` unique, `k8s.pod.name` growing 5%/min, `http.route` and `http.method` fixed | `/d/asap-cardinality`: distinct values with bound, class per key (stable, growing, identifier-like), events |
| `drift` | KLL per route | `http.route-3`'s median triples at minute 10 | `/d/asap-drift`: KS distance from the last 5 windows, drift events |
| `topk` | Count-Min + top-10 heap per attribute key | 200 Zipf routes; a new route takes 15% of traffic at minute 10 | `/d/asap-topk`: top-10 with bound vs exact, new heavy hitters |

Answers reach Prometheus over OTLP as `asap_quantile`, `asap_distinct`,
`asap_topk`, `asap_points`, `asap_cardinality_state`, `asap_drift_ks`,
`asap_drift`, `asap_topk_new` and `asap_answer_delay`, labelled
`method="sketch"` or `"exact"`. Events (class changes, drift start/end, new
heavy hitters) reach Loki as OTLP logs from `service.name=asap-detector`.
Prometheus also scrapes each engine's telemetry
(`/api/v1/metrics?format=prometheus`): CPU time per pipeline, resident memory
and payload bytes per receiver, labelled with the stage.

## Run

Linux, Docker with Compose, 7 free CPUs (0-6 by default; `CPU_BRANCH_A=10`
etc. move a stage). From the repository root:

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
| all | every stage up; points per window equal the offered rate (no backpressure); sketch and exact windows hold the same points |
| latency | exact p50/p99 inside the sketch's bound (routes with at least 2,000 points) |
| cardinality | sketch and exact give every key the same class; the generated classes are found; distinct-count error at most 2%; class events in Loki |
| drift | the changed route drifts in both methods; no other route does; drift event in Loki |
| topk | the new route enters both top-10s; sketch recall@10 at least 0.9; novelty event in Loki |

CI runs it on every PR (`.github/workflows/ci.yml`) with the stages sharing
the runner's CPUs: a correctness check, not a capacity measurement.

## Experiments

`experiments.py inline-cost` measures branch CPU with and without each
function over offered rates; `experiments.py accuracy-size` measures error
against exact as KLL `k` and Count-Min width change. Results and the
merge/serialization benchmark (`asap-merge-bench`) are in
[docs/results](../../docs/results/).
