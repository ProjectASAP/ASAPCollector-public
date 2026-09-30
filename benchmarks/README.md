# ASAP sketch performance benchmarks

**The default nightly evaluation now uses persistent OTLP/HTTP streaming across
seven containers, real data-interface RX/TX counters, bounded backpressure, and
backend-validated throughput. See [STREAMING.md](STREAMING.md) for the current
setup, bandwidth profiles, commands, and artifacts.**

The remainder of this document describes the historical file-backed batch
baseline. Run it with `./benchmarks/run-file-nightly.sh`; its throughput figures
are not directly comparable to the streaming evaluation.

The nightly benchmark runs with OTAP's official Python `pipeline_perf_test`
orchestrator. It deploys raw, exact-quantile, and ASAP KLL scenarios as
identically configured Docker containers, observes each cgroup with the
upstream `docker_component` monitor, and emits upstream process reports.
Criterion also runs isolated KLL stage measurements in the nightly workflow.

The logical topology is `traffic generator → ASAP sketch SUT → simulated
backend`. The generator uses reproducible SplitMix64 durations with a long tail
and OpenTelemetry `http.server.request.duration` semantic attributes. The local microbenchmark has
three SUT workloads: transmitting every raw value, collecting and sorting every
raw value for exact p50/p99, and creating then merging two self-describing
ASAPv1 KLL shards for approximate p50/p99. The official-orchestrated nightly
runs the same three workloads with real OTAP `RuntimePipeline` workers. The
backend checks raw cardinality, exact sort results, and KLL's 5% error bound.

Run the official-style nightly suite locally with Docker and Python 3.13 (the
runner fetches the pinned OTAP orchestrator revision on first use):

```sh
./benchmarks/run-file-nightly.sh
```

## Compared topologies

All scenarios generate the same reproducible semantic-convention-shaped
`http.server.request.duration` values. Nightly gives each of two sources one
native `OtapPdata` batch of 262,144 observations. The local Criterion workload
uses 4,096-observation batches. Both timed boundaries end after the simulated
backend decodes the scenario's output pdata.

```text
OTAP raw pass-through

source A -> pass-through A --\
                               -> pass-through merge -> pass-through final -> backend
source B -> pass-through B --/
```

This is the no-computation control. It measures the cost of retaining and
transmitting every raw value; it intentionally has a larger output than the two
quantile scenarios.

```text
OTAP exact quantile

source A -> sort A -> encoded sorted run A --\
                                               -> merge sorted runs
source B -> sort B -> encoded sorted run B --/          |
                                                         v
                                              exact p50/p99 -> backend
```

The exact path retains all N values until sorting completes. Its result must
equal the reference p50/p99 exactly.

```text
ASAP KLL quantile

source A -> KLL create A -> ASAPv1 KLL A --\
                                                -> KLL merge -> merged ASAPv1 KLL
source B -> KLL create B -> ASAPv1 KLL B --/                    |
                                                                  v
                                                     KLL p50/p99 -> backend
```

The KLL path uses two deterministic `k=400` creators. Merge operates on the
self-describing ASAPv1 representation supplied by `asap_sketchlib`; its p50/p99
must remain within 5% of the exact result.

In nightly, every topology has two generator processes, two branch workers, one merge worker, and one
final/estimate worker. Every worker is a separate OS process containing a real
OTAP `RuntimePipeline`; the generators run outside OTAP. Two available CPUs
form a configurable traffic-generator pool. Branch A, branch B, merge, and
estimate each receive one dedicated CPU with `taskset`/Docker `--cpuset-cpus`.
The validating backend receives all remaining host CPUs because it is benchmark
infrastructure. No component has a Docker memory limit; actual RSS is reported.
Each process writes elapsed time, CPU use, peak RSS, output bytes, and OTLP
file serialization time. The upstream Docker monitor includes the entire process
tree in container CPU and RSS measurements. All scenarios use the same input count and
the same warm-up and observation intervals.

The local Criterion microbenchmark records throughput and latency for three pipelines with identical
native pdata ingress and backend encode/decode boundaries: a no-computation OTAP
raw pass-through, an OTAP exact-quantile processor that sorts all raw values,
and the two-creator ASAP KLL path. Both quantile pipelines emit the same p50/p99
Gauge shape. Before timing, the backend validates values, metric labels,
resource labels, exact p50/p99 results, and the KLL 5% accuracy bound.

The nightly workflow compiles first, outside measurement. During its observation
window, OTAP's orchestrator samples CPU and RSS from the Docker cgroup. Each
component writes completed input count, elapsed time, and signals/s below
`benchmark-results/{raw,exact,kll}/throughput.env`. The rate uses only completed
batches; `run_wall_nanoseconds` records the full run duration. An interrupted
partial batch does not dilute the rate. The orchestrator writes three process
reports under `benchmark-results/`. They are uploaded by CI.
The per-role `stages.json` and `stages.jsonl` files report generator, create,
merge, and estimate resources separately. `kll-isolated-stages.txt` contains
Criterion throughput for KLL creation, merge, p50/p99 estimation, and ASAPv1
serialization.

## Nightly configuration

| Setting | Current value |
| --- | --- |
| Scenarios | raw, exact, KLL |
| Sources | 2 processes, 262,144 observations each |
| Generator | 2 threads per source; SplitMix64 long-tail durations, seed `0x9e3779b97f4a7c15` |
| KLL | `k=400`, Msgpack ASAPv1, p50/p99 |
| CPU placement | configurable generator pool; one dedicated CPU for each downstream component |
| Observation | 5 s warm-up, 30 s monitored window |
| Debug output | disabled in nightly measurements; enabled in the interactive demo |
| Output | config.json, throughput.env, stages.json, stages.jsonl, process reports, Criterion text |

Stage process rates include OTAP startup and file IO. `serialization_seconds`
measures final OTLP file conversion and write. Criterion's `serialize_asapv1`
result isolates sketch framing. Stages run sequentially after generation, so
these rates describe stage work rather than simultaneous streaming capacity.

## Input scale sweep

The nightly runner also repeats each raw, exact, and KLL pipeline twice at
32,768, 65,536, 262,144, and 524,288 observations **per source**. It uses the
same semantic values, generator threads, CPU placement, and disabled debug
output at every size. Run it locally after building `asap-otap-demo`:

```sh
python3 benchmarks/run-scale-sweep.py
```

Choose sizes and repetitions with `--points 65536 262144 524288` and
`--repetitions 3`. Results appear under `benchmark-results/scale-sweep/`:

- `throughput.svg` plots raw, exact, and KLL signals/s plus KLL speedup ratios.
- `{raw,exact,kll}-resources.svg` plots CPU seconds and peak RSS for each of the
  two traffic generators and four processor roles.
- `summary.csv` and `resources.csv` contain median measurements; `runs.json`
  retains every individual run, and `config.json` records CPU sets and sizes.

Each scale point measures one complete batch, including traffic generation,
OTAP startup, file boundaries, and backend decoding. Reference sorting and
quantile validation run after the timed pipeline; raw skips reference sorting.
`runs.json` also records process wall time and validation time. The charts describe
this file-backed demo's scaling. Peak RSS is per process and should not be
summed as a simultaneous container peak because merge and estimate run after
the creators finish.
The `branch_a` and `branch_b` roles pass values through in raw, sort values in
exact, and create KLL sketches in KLL.

## Viewing nightly results today

There is not yet a published ASAP dashboard. Results are available as GitHub
Actions artifacts:

1. Open the repository's **Actions** tab and select **ASAP sketch benchmark**.
2. Open a successful scheduled, manual, or pull-request run.
3. Download the `asap-sketch-benchmark-<run-id>` artifact from **Artifacts**.
4. Compare `raw/throughput.env`, `exact/throughput.env`, and
   `kll/throughput.env` for completed input throughput and serialized boundary
   bytes. Read `otap-raw-resources.md`, `otap-exact-quantile-resources.md`, and
   `asap-kll-resources.md` for the upstream orchestrator's CPU and memory
   summaries.

Artifacts expire after 30 days. Pull-request runs are useful for reviewing a
specific change; the scheduled run at 02:30 UTC is the consistent source for
day-over-day comparisons after this workflow is merged to the default branch.
GitHub-hosted runners are shared infrastructure, so small differences between
runs should not be treated as regressions without repeated measurements.

The serialized byte rates describe the current file-backed processor boundary.
They are not network-bandwidth measurements: the processors exchange files
inside one container, and Docker network RX/TX is therefore expected to remain
near zero.

## Future dashboard publication

A repository-owned dashboard can be added without changing the benchmark
topologies:

```text
nightly artifact
  -> extract one versioned summary.json per run
  -> append the commit, timestamp, correctness, throughput, CPU, and RSS history
  -> render a static comparison page
  -> publish the page and compact JSON history with GitHub Pages
```

Only compact summaries should be retained in the Pages history. Raw `.otlp`
files, debug logs, and complete intermediate artifacts should continue to use
the existing 30-day artifact retention instead of being committed or published.
Each dashboard point should link to its commit and Actions run and record the
benchmark schema version, OTAP revision, runner type, scenario parameters, and
observation duration so incompatible runs are not plotted as one series.

The first dashboard should remain informational and require manual review.
Automated regression thresholds should be introduced only after enough
scheduled runs establish normal GitHub-hosted-runner variance. If stable
cross-project numbers are required later, the same summary format can be fed by
an isolated runner or an upstream OTAP shared-server contribution.

The local comparison answers two separate questions. Raw pass-through measures the
cost of retaining and transmitting all observations, so its output cardinality
is intentionally much larger. Exact sort and KLL both emit two Gauge values and
are directly comparable for this fixed batch workload. At these input sizes,
OTAP pdata decode and harness costs can dominate both quantile implementations;
larger streams and bounded-memory tests are needed to expose their different
scaling behavior. Peak RSS is for the whole Criterion process, including input,
OTAP/Arrow dependencies, and the harness—not just the algorithm state.

This repository consumes the official framework rather than copying its Python
implementation. It pins the same OTAP revision as the Rust dependencies. It does
not yet run on OpenTelemetry's shared bare-metal host. The file-backed OTLP
boundary makes CPU, RSS, throughput, and artifact bytes comparable, but artifact
bytes are not a network-bandwidth measurement. A future upstream contribution
should replace it with the standard network traffic generator and backend nodes.

For performance debugging, bench profiles retain symbols (`strip = "none"`).
Criterion identifies regressions locally. A follow-up can use `samply`, or the
OTAP admin server's `/api/v1/debug/pprof/profile` and `/heap` endpoints after the
processor is hosted by the upstream engine binary.

## Upstream references

- [Nightly dashboard](https://open-telemetry.github.io/otel-arrow/benchmarks/nightly/)
  and [nightly suites](https://github.com/open-telemetry/otel-arrow/tree/main/tools/pipeline_perf_test/test_suites/integration/nightly)
- [Comparison dashboard](https://open-telemetry.github.io/otel-arrow/compare/)
  and [comparison suites](https://github.com/open-telemetry/otel-arrow/tree/main/tools/pipeline_perf_test/test_suites/comparison_dashboard)
- [Traffic generator](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/dev-nodes/src/receivers/traffic_generator/README.md)
- [Admin debug API](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/admin/README.md#debug),
  [Samply](https://github.com/mstange/samply), and
  [Criterion](https://github.com/bheisler/criterion.rs)
