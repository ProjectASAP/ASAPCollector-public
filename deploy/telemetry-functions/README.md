# Telemetry functions on real collector containers

Every stage is the official OTAP dataflow engine (`asap-collector`: the
upstream controller with the ASAP nodes linked in), one container per stage,
each pinned to its own CPU, connected by OTLP/HTTP over the compose network.

Three pipelines run side by side over the same input and share no processor:

```text
sketch: generator-a/b -> branch-a/b (sketch per window) -> merge -> detector -> Prometheus
exact:  exact-generator-a/b -> exact-branch-a/b (exact window) -> exact-merge -> exact-detector -> Prometheus
raw:    raw-generator-a/b -> raw-branch-a/b (forward) -> raw-merge (forward) -> raw-sink -> loki-raw
```

The two generators of each pipeline (sources a and b) run the same configs
with the same scenario start (`SCENARIO_START`), and every point is a function
of scenario time, so all three pipelines receive identical points.

| Stage | Config | What it runs |
| --- | --- | --- |
| generator-a/b, exact-generator-a/b, raw-generator-a/b | `latency/generator-*.yaml` | `scenario_generator`: seeded latency points over 20 routes (temporary stand-in for the official traffic generator) |
| branch-a/b | `latency/branch.yaml` | `asap_sketches`: one KLL per route per 1-minute window |
| merge | `latency/merge.yaml` | `window_merge`: merges both sources' sketches per window, 5 s after the window ends |
| detector | `latency/detector.yaml` | `sketch_detector`: p50/p99 per route with lower/upper bounds (`method="sketch"`) |
| exact-branch-a/b | `latency/exact-branch.yaml` | `exact_branch`: every value per route per 1-minute window |
| exact-merge | `common/exact-merge.yaml` | `exact_merge`: adds both sources' exact windows, 5 s after the window ends |
| exact-detector | `latency/exact-detector.yaml` | `exact_detector`: exact p50/p99 per route (`method="exact"`): the ground truth |
| raw-branch-a/b, raw-merge | `common/raw-*.yaml` | forward raw points (OTLP receiver -> exporter) |
| raw-sink | `common/raw-sink.yaml` | `raw_sink`: writes every raw point as one log record to `loki-raw` and reports when `loki-raw` acknowledged each window's last point |

Prometheus receives the answers over OTLP (`asap_quantile`, `asap_points`,
labelled `method="sketch"` or `"exact"`) and scrapes each collector's engine
telemetry (`/api/v1/metrics?format=prometheus`): CPU time per pipeline
(`cpu_time_total`), resident memory (`memory_rss`) and payload bytes per
receiver, labelled with the stage.

Every pipeline reports its window completion delay as `asap_answer_delay`
(seconds from a window's end until its result is complete at the end of the
pipeline): sketch and exact when their detector emits the window's answers,
raw when `loki-raw` has acknowledged the window's last point. raw-sink also
reports each point's time from its event time to that acknowledgement
(`asap_raw_transit`, p50 and p99) and points `loki-raw` rejected
(`asap_raw_failed`).

## Run

Linux, Docker with Compose, 18 free CPUs (0-17 by default; `CPU_BRANCH_A=20`
etc. move a stage) and memory for `loki-raw` (capped at `LOKI_RAW_MEMORY`,
default 16g). From the repository root:

```sh
deploy/telemetry-functions/run.sh       # DOCKER_COMMAND="sudo docker" if needed
```

Open <http://localhost:3000/d/asap-latency>. The first answers appear about
65 seconds after the first full minute. Stop with
`docker compose -f deploy/telemetry-functions/compose.yaml down`.
