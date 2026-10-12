# Telemetry functions on real collector containers

Every stage is the official OTAP dataflow engine (`asap-collector`: the
upstream controller with the ASAP nodes linked in), one container per stage,
each pinned to its own CPU, connected by OTLP/HTTP over the compose network.

```text
generator-a -> branch-a --sketch--> merge -> detector --> Prometheus -> Grafana
generator-b -> branch-b --sketch--/
                  \--raw copy (fanout)--> exact ------/
```

| Stage | Pipeline | What it does |
| --- | --- | --- |
| generator-a/b | `pipelines/generator-*.yaml` | `scenario_generator`: seeded latency points over 20 routes (temporary stand-in for the official traffic generator) |
| branch-a/b | `pipelines/branch.yaml` | `fanout` copies the input: `asap_sketches` builds one KLL per route per 1-minute window; the raw copy goes to `exact` |
| merge | `pipelines/merge.yaml` | `window_merge`: merges both sources' sketches per window, 5 s after the window ends |
| detector | `pipelines/detector.yaml` | `sketch_detector`: p50/p99 per route with lower/upper bounds |
| exact | `pipelines/exact.yaml` | `exact_answers`: exact p50/p99 per route from every raw point (ground truth and exact baseline) |

Prometheus receives both answer streams over OTLP (`asap_quantile`,
`asap_points`, labelled `method="sketch"` or `"exact"`) and scrapes each
collector's engine telemetry (`/api/v1/metrics?format=prometheus`): CPU time
per pipeline (`cpu_time_total`), resident memory (`memory_rss`) and payload
bytes per receiver, labelled with the stage.

## Run

Linux, Docker with Compose, 7 free CPUs (0-6 by default; `CPU_BRANCH_A=10`
etc. move a stage). From the repository root:

```sh
deploy/telemetry-functions/run.sh       # DOCKER_COMMAND="sudo docker" if needed
```

Open <http://localhost:3000/d/asap-latency>. The first answers appear about
65 seconds after the first full minute. Stop with
`docker compose -f deploy/telemetry-functions/compose.yaml down`.
