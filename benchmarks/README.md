# ASAP sketch performance benchmarks

This benchmark adopts the OTAP pipeline performance methodology while the ASAP
processor is still maintained outside the upstream `otel-arrow` tree.

The logical topology is `traffic generator → ASAP sketch SUT → simulated
backend`. The generator uses a deterministic metric shaped like the OpenTelemetry
`http.server.request.duration` semantic convention. The SUT creates four KLL
shards, merges their self-describing ASAPv1 state, and estimates p50/p99. The
backend compares both estimates with exact sorted quantiles and fails above 5%
relative error.

Run locally:

```sh
./benchmarks/run-nightly.sh
```

Criterion records throughput and latency for two pipelines with identical
native pdata ingress and backend encode/decode boundaries: a no-sketch OTAP
control and the four-shard ASAP KLL path. Exact sorting is reported separately
as an algorithmic reference, not as an engine comparison. Before timing, the
backend validates values, metric labels, resource labels, p50/p99 presence, and
the 5% accuracy bound.

The runner compiles first, outside measurement, then starts a separate benchmark
process for every scenario and signal count. Each `resource-*.txt` therefore
contains `/usr/bin/time -v` CPU, wall-time, and peak-RSS measurements attributable
to one case rather than to Cargo/rustc and the entire suite. Results and the exact
scenario manifest are written to `benchmark-results/` and uploaded by CI.

This is deliberately an internal replica, not an upstream OTAP nightly suite.
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
