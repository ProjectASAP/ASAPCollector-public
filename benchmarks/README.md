# ASAP sketch performance benchmarks

This benchmark adopts the OTAP pipeline performance methodology while the ASAP
processor is still maintained outside the upstream `otel-arrow` tree.

The logical topology is `traffic generator → ASAP sketch SUT → simulated
backend`. The generator uses a deterministic metric shaped like the OpenTelemetry
`http.server.request.duration` semantic convention. Three SUT workloads are
compared: transmitting every raw value, collecting and sorting every raw value
for exact p50/p99, and creating then merging four self-describing ASAPv1 KLL
shards for approximate p50/p99. The backend requires exact results from the sort
path and fails the KLL path above 5% relative error.

Run locally:

```sh
./benchmarks/run-nightly.sh
```

## Compared topologies

All scenarios start from the same pre-generated, semantic-convention-shaped
`http.server.request.duration` values and the same native `OtapPdata` batches
(4,096 observations per batch). The timed boundary begins with owned clones of
those batches and ends after the
simulated backend decodes the scenario's output pdata.

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

The KLL path uses two deterministic `k=200` creators. Merge operates on the
self-describing ASAPv1 representation supplied by `asap_sketchlib`; its p50/p99
must remain within 5% of the exact result.

These are currently in-process Criterion workloads over native `OtapPdata`, not
three deployed OTAP `RuntimePipeline` graphs. Moving each box into registered
receiver/processor/exporter nodes is the next step before claiming full engine
topology overhead.

Criterion records throughput and latency for three pipelines with identical
native pdata ingress and backend encode/decode boundaries: a no-computation OTAP
raw pass-through, an OTAP exact-quantile processor that sorts all raw values,
and the two-creator ASAP KLL path. Both quantile pipelines emit the same p50/p99
Gauge shape. Before timing, the backend validates values, metric labels,
resource labels, exact p50/p99 results, and the KLL 5% accuracy bound.

The runner compiles first, outside measurement, then starts a separate benchmark
process for every scenario and signal count. Each `resource-*.txt` therefore
contains `/usr/bin/time -v` CPU, wall-time, and peak-RSS measurements attributable
to one case rather than to Cargo/rustc and the entire suite. Results and the exact
scenario manifest are written to `benchmark-results/` and uploaded by CI.

The comparison answers two separate questions. Raw pass-through measures the
cost of retaining and transmitting all observations, so its output cardinality
is intentionally much larger. Exact sort and KLL both emit two Gauge values and
are directly comparable for this fixed batch workload. At these input sizes,
OTAP pdata decode and harness costs can dominate both quantile implementations;
larger streams and bounded-memory tests are needed to expose their different
scaling behavior. Peak RSS is for the whole Criterion process, including input,
OTAP/Arrow dependencies, and the harness—not just the algorithm state.

This is deliberately an internal replica, not an upstream OTAP nightly suite.
The three processor workloads operate on native `OtapPdata` inside Criterion;
they are not yet instantiated as complete OTAP `RuntimePipeline` graphs.
Once the ASAP processor is ready to move into `otel-arrow`, the manifest maps to
their three-component orchestrator and shared bare-metal runner. PR 3830 only
adds the declarative Weaver v2 registry today; switching the input generator to
its future generated Rust SDK is therefore deferred.

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
