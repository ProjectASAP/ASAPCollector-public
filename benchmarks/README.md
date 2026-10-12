# ASAP sketch performance benchmarks

The nightly evaluation uses persistent OTLP/HTTP streaming across seven
containers, real data-interface RX/TX counters, bounded backpressure, and
backend-validated throughput. See [STREAMING.md](STREAMING.md) for the setup,
bandwidth profiles, commands, and artifacts, [CAPACITY_DESIGN.md](CAPACITY_DESIGN.md)
for the capacity definition, and [PROFILING.md](PROFILING.md) for CPU
profiling.

```sh
./benchmarks/run-nightly.sh
```

## Viewing nightly results today

There is not yet a published ASAP dashboard. Results are available as GitHub
Actions artifacts:

1. Open the repository's **Actions** tab and select **ASAP sketch benchmark**.
2. Open a successful scheduled, manual, or pull-request run.
3. Download the `asap-sketch-benchmark-<run-id>` artifact from **Artifacts**.
4. Read `streaming/summary.json`, `streaming/capacity.json`, and the
   `throughput.svg` / `resources.svg` charts.

Artifacts expire after 30 days. GitHub-hosted runners are shared
infrastructure, so small differences between runs should not be treated as
regressions without repeated measurements.

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

## Upstream references

- [Nightly dashboard](https://open-telemetry.github.io/otel-arrow/benchmarks/nightly/)
  and [nightly suites](https://github.com/open-telemetry/otel-arrow/tree/main/tools/pipeline_perf_test/test_suites/integration/nightly)
- [Comparison dashboard](https://open-telemetry.github.io/otel-arrow/compare/)
  and [comparison suites](https://github.com/open-telemetry/otel-arrow/tree/main/tools/pipeline_perf_test/test_suites/comparison_dashboard)
- [Traffic generator](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/dev-nodes/src/receivers/traffic_generator/README.md)
- [Admin debug API](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/admin/README.md#debug) and
  [Samply](https://github.com/mstange/samply)
