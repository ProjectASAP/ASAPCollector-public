# Telemetry functions demo: Sketch vs Exact vs Raw

This demo answers one monitoring question three ways over the same traffic, and
shows what each way costs as the traffic ramps up and down:

| Approach | Idea | How it answers |
| --- | --- | --- |
| **Sketch** | summarize at the source | each source keeps a small fixed-size sketch per minute; the sketches are merged and the answer is stored in Prometheus |
| **Exact** | count everything at the source | each source keeps every value and its count per minute; the counts are added up and the exact answer is stored in Prometheus |
| **Raw** | store every point, compute when asked | every raw point is stored in Prometheus; a PromQL query computes the answer from all of them |

Every approach answers over **the last 5 minutes, once a minute**, and Exact is
the ground truth the other two are compared with. Each approach writes to its
own Prometheus, so the CPU, memory and network the dashboards show are that
approach's alone.

## Quick start

You need Linux, Docker with Compose, and 18 free CPUs per demo. From the
repository root:

```sh
# Build the collector image and start the top-k demo
USE_CASE=topk deploy/telemetry-functions/run.sh          # add DOCKER_COMMAND="sudo docker" if Docker needs sudo

# Optionally, start the cardinality demo beside it (reuse the image, next CPU/port slot)
NO_BUILD=1 DEMO_SLOT=1 USE_CASE=cardinality deploy/telemetry-functions/run.sh
```

Open Grafana at **http://localhost:3000** (no login):

- top-k: http://localhost:3000/d/asap-topk
- cardinality: http://localhost:3000/d/asap-cardinality

If the demo runs on a remote machine, forward the port first:
`ssh -L 3000:localhost:3000 <user>@<host>`.

The first answers appear about two minutes after the start; full 5-minute
answers after about six. Stop a demo, and the shared Grafana, with:

```sh
docker compose -p tf-topk -f deploy/telemetry-functions/compose.yaml down
docker compose -p tf-grafana -f deploy/telemetry-functions/grafana-compose.yaml down
```

## The demos

`USE_CASE` picks the question and its traffic. In every use case two sources
(a and b) send the same kind of traffic, and the rate per source ramps up and
back down every 40 minutes, again and again: 5k, 10k, 20k, 40k, 80k, 40k, 20k,
10k points/s, 5 minutes each.

| `USE_CASE` | Question | Traffic | Sketch |
| --- | --- | --- | --- |
| `topk` | Which client addresses sent the most requests (top talkers), and which are new? | client addresses drawn Zipf(1.0) over 20 million IPv4 addresses: about 1 million distinct per 5 minutes at the lowest rate, about 7 million at the highest; a new client, `203.0.113.7`, takes 15% of traffic at minute 10 | Count-Min (4 x 4096) + top-20 list |
| `cardinality` | How many distinct values does each attribute have, and is it stable, growing or identifier-like? | `user.id` unique per point, `k8s.pod.name` growing 5% per minute, `http.route` and `http.method` fixed; `http.route` explodes from 20 to 20,000 values at minute 10 | one HyperLogLog per attribute |
| `latency` | p50 / p99 latency per route | latency over 20 routes | KLL per route |
| `drift` | Which route's latency distribution changed? | `http.route-3`'s median triples at minute 10 | KLL per route |

The top-k and cardinality dashboards compare all three approaches; the latency
and drift dashboards compare Sketch with Exact.

## What the dashboards show

The top-k and cardinality dashboards, top to bottom:

1. **Diagram.** One row per approach: each step in plain words (its container
   name in grey), and on the right the PromQL query that returns the answer.
   Sketch and Exact read an answer that is already computed; Raw computes it
   from every raw point of the last 5 minutes.
2. **Accuracy against exact** (Sketch only). Top-k: heavy-hitter precision,
   recall, F1 and average relative count error. Cardinality: relative error of
   the distinct count per attribute.
3. **From data generated to query result.** Per approach over time: the time
   after the 5-minute range ends until the answer is in hand, that is, until
   the data is queryable plus the query (log scale). The 5-minute wait for the
   range to end is the same for all three, so it is left out. Beside it: the
   input rate and the data shape (distinct values), with their history, so
   you can see each approach's time follow the ramp.
4. **Resources per approach.** CPU, memory and network of the whole approach,
   its own Prometheus included.
5. **Answers.** The latest answers of the three approaches side by side, with
   each approach's error against Exact.

What to look for: Sketch's time, memory and traffic stay flat as the input
grows; Exact's grow with the number of distinct values; Raw's query time grows
with the number of raw points, until `prometheus-raw` runs out of memory
(every distinct `user.id` or client address is a series of its own).

## How it works

### Pipelines

Every stage is the OTAP dataflow engine (`asap-collector`: the upstream
controller with the ASAP nodes linked in), one container per stage, pinned to
its own CPU, talking OTLP/HTTP over the compose network.

```text
Sketch: generator-a/b -> branch-a/b (sketch per minute) -> merge -> detector -> prometheus-sketch
Exact:  exact-generator-a/b -> exact-branch-a/b (counts per minute) -> exact-merge -> exact-detector -> prometheus-exact
Raw:    raw-generator-a/b -> raw-sink-a/b (forwards each point, no window) -> prometheus-raw <- query-job (PromQL)
```

The generators of all three pipelines start from the same second
(`SCENARIO_START`) and every point is a function of scenario time, so all
three pipelines get identical points.

| Stage | What it does |
| --- | --- |
| generator-a/b (and `exact-`, `raw-`) | `scenario_generator`: seeded traffic for the use case |
| branch-a/b | `asap_sketches`: one sketch per series per 1-minute window |
| merge | `window_merge`: merges both sources' sketches of a window as soon as both sent it (5 s timeout for a missing source) |
| detector | `sketch_detector`: merges each series' last 5 windows (`range_windows: 5`) and writes the answers, with bounds, and events (`method="sketch"`) |
| exact-branch-a/b | `exact_branch`: every value, and a value -> count map per attribute, per 1-minute window |
| exact-merge | `exact_merge`: adds up both sources' windows as soon as both sent them (60 s timeout: at the top rate an exact window takes more than 5 s to arrive) |
| exact-detector | `exact_detector`: adds up each series' last 5 windows and writes the exact answers and events (`method="exact"`) |
| raw-sink-a/b | `raw_sink` (shown as "otlp-forwarder"): forwards every raw point unchanged as one sample to `prometheus-raw`, and reports when `prometheus-raw` acknowledged each minute's last point |
| query-job | `query_job.py`: once a minute, times each approach's query on its own Prometheus, computes Raw's answer with PromQL, and applies the detectors' event rules to Raw's answers |
| hh-accuracy | `heavy_hitter_accuracy` (top-k only): compares the Sketch and Exact answers and reports heavy-hitter accuracy |

### Prometheus instances

| Prometheus | Holds |
| --- | --- |
| `prometheus-sketch`, `prometheus-exact` | that approach's answers, events and timing |
| `prometheus-raw` | every raw point, one sample each (2 h retention, memory capped by `PROMETHEUS_RAW_MEMORY`) |
| `prometheus` (monitoring) | what Grafana reads: every stage's CPU, memory and traffic, a copy of the Sketch and Exact answers (federation of `asap_*`), Raw's timing, heavy-hitter accuracy and the query job's results. It belongs to no approach. |

There is no log store: detection events are `asap_event` gauges.

### Every raw point in Prometheus

Prometheus keeps one sample per series and millisecond, and a generator batch
(up to 1,024 points) shares one timestamp, so two requests from the same client
in one batch would collide and Prometheus would keep only the first. raw-sink
therefore labels each point `point_slot`: 0 for the first point of its series
at that millisecond, 1 for the second, and so on. No point is lost, and queries
sum over the slot, for example
`topk(20, sum by (client_address) (count_over_time(http_server_request_duration[5m])))`.
The two sources stay apart through their resource attribute `source`, which
`prometheus-raw` promotes to a label.

### Timing: from data generated to query result

The same definitions for all three approaches, per answer:

| Gauge | Meaning |
| --- | --- |
| `asap_window_wait_seconds` | 5 minutes: the range's oldest point waits that long for the range to end |
| `asap_data_available_seconds` | the wait, plus the time until the data is queryable: Sketch and Exact until the detector sent the answer to its Prometheus; Raw until `prometheus-raw` acknowledged the range's last point |
| `asap_query_seconds` | the query that returns the answer on the approach's own Prometheus: Sketch and Exact read the stored estimates; Raw computes it from the raw points |
| `asap_result_age_seconds` | data available plus the query, as if the query ran as soon as the data was available |

Merges and detectors also report when each stage received and emitted each
window (`asap_stage_received`, `asap_stage_emitted`), to find which stage a
delay comes from.

### Accuracy

**Heavy hitters** (top-k). A heavy hitter is a client address with at least 1%
of the points by the exact counts; the sketch reports an address when its
estimate reaches 1%. `hh-accuracy` reports `asap_hh_precision` (reported
addresses that are heavy hitters / reported), `asap_hh_recall` (heavy hitters
reported / heavy hitters), `asap_hh_f1`, `asap_hh_are` (mean |estimate - exact|
/ exact over the heavy hitters found), `asap_hh_truth` and `asap_hh_reported`.
Both detectors report their top 20, so every heavy hitter is in the exact list
(about 6 addresses reach 1% here); when the exact top 20 is all above the
threshold the window only gets `asap_hh_incomplete`.

**Cardinality classes.** Each detector classifies every attribute from its
last 10 answers (`cardinality:` in `detector.yaml`): `identifier-like` when
distinct / points is at least 0.9 (nearly every point has a new value); else
`growing` when distinct is more than 20% above the mean of the last answers
(needs 3); else `stable`. Attributes with fewer than 100 points are not
classified. All three approaches apply the same rules to their own counts, so
a different class shows where the sketch's error crossed a threshold.

A comparison where one approach's points differ from Exact's by more than 1%
(a source missed a merge) is left out of the error.

### Metrics reference

Answers reach Prometheus over OTLP as `asap_quantile`, `asap_distinct`,
`asap_topk`, `asap_points`, `asap_cardinality_state`, `asap_drift_ks`,
`asap_drift`, `asap_topk_new`, `asap_event` and `asap_answer_delay`, labelled
`method` (`sketch`, `exact` or `raw`). Each event (class change, drift start or
end, new heavy hitter) is an `asap_event` gauge labelled `kind`, the event's
fields and `message` (one sentence), whose value is the window's end in
seconds. raw-sink also reports each point's time to acknowledgement
(`asap_raw_transit`), points `prometheus-raw` rejected (`asap_raw_failed`), the
bytes it sent (`asap_raw_sent_bytes`), and windows still unacknowledged 50 s
after their end (`asap_raw_unacknowledged`; the query job flags them with
`asap_raw_incomplete`). The monitoring Prometheus scrapes each engine's own
telemetry (`/api/v1/metrics?format=prometheus`): CPU time per pipeline,
resident memory and payload bytes per receiver, labelled `stage`.

Known limits: the merges count a window's messages, not distinct sources, so a
source that sends a window twice completes it early and the other source's late
input is dropped. The generators size their batches for the scenario's peak
rate, so at the starting rate a batch holds about 60 points instead of 1,024.

## Running options

| Setting | Default | Meaning |
| --- | --- | --- |
| `USE_CASE` | `latency` | `topk`, `cardinality`, `latency` or `drift` |
| `DEMO_SLOT` | `0` | run another demo beside the others: host ports +N and CPUs 18N to 18N+17 |
| `NO_BUILD` | unset | `1` reuses the built `asap-collector:local` image |
| `DOCKER_COMMAND` | `docker` | e.g. `sudo docker` |
| `PROMETHEUS_PORT` | `9090` + slot | the monitoring Prometheus on the host |
| `PROMETHEUS_RAW_PORT` | `9190` + slot | `prometheus-raw` on the host, to query the raw points by hand |
| `PROMETHEUS_RAW_MEMORY` | `32g` | memory cap of `prometheus-raw` |
| `RANGE_WINDOWS` | `5` | windows each Raw answer covers; keep it equal to `range_windows` in the detectors |
| `CPU_<STAGE>` | slot's CPUs | pin a stage to another CPU, e.g. `CPU_BRANCH_A=20` |

- Each use case is one compose project, `tf-<use case>`; starting it again
  restarts it. Keep that project name: the Grafana data sources address
  `tf-<use case>-prometheus-1`.
- One Grafana (`grafana-compose.yaml`, project `tf-grafana`) serves every demo;
  `run.sh` connects it to each demo's network. A dashboard stays empty until
  its demo runs.
- Demos on different slots can share a physical core on hosts with
  hyper-threads, so run one demo at a time for capacity measurements.
- The dashboards are generated by `grafana/gen_dashboards.py`
  (`python3 grafana/gen_dashboards.py topk grafana/dashboards/topk.json`,
  likewise `cardinality`): edit the script, not the JSON.

## End-to-end acceptance

```sh
deploy/telemetry-functions/run.sh --build-only
deploy/telemetry-functions/acceptance.py --report acceptance.json
```

Runs all four use cases with time scaled down 6x (10-second windows, changes
at second 100, at each scenario's starting rate, answers per window rather than
over 5 windows) and checks the answers through the monitoring Prometheus. It
exits non-zero if any check fails. Stop the demos first: it uses the default
project and ports.

| Use case | Checks |
| --- | --- |
| all | every stage up; points per window equal the offered rate (no backpressure); Sketch and Exact windows hold the same points; every pipeline reports its window completion delay; merges and detectors report their timing; the query job times the Sketch and Exact queries |
| latency | exact p50/p99 inside the sketch's bound (routes with at least 2,000 points) |
| cardinality | Sketch and Exact give every attribute the same class; the generated classes are found, the growing one by all three approaches; distinct-count error at most 2%; Raw answers every attribute within 1% of Exact; class events in Prometheus |
| drift | the changed route drifts in both methods; no other route does; drift event in Prometheus |
| topk | the new client (`203.0.113.7`) enters both top-10s; sketch recall@10 at least 0.9; heavy-hitter precision and recall at least 0.9; Raw's heavy-hitter recall at least 0.9; novelty event in Prometheus |

CI runs it on every PR (`.github/workflows/ci.yml`) with the stages sharing
the runner's CPUs: a correctness check, not a capacity measurement.

## Experiments

`experiments.py pipelines` runs the three pipelines side by side over offered
rates and records each stage's CPU, memory, delivered rate and window
completion delay; `experiments.py accuracy-size` measures the error against
Exact as KLL `k` and Count-Min width change. Results and the
merge/serialization benchmark (`asap-merge-bench`) are in
[docs/results](../../docs/results/).
