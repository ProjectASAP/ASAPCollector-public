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

## CI tiers

| Tier | Workflow | Trigger | What it checks |
|---|---|---|---|
| Per-merge smoke | `ci.yml` | every PR and push to `main` | `cargo fmt`, `clippy -D warnings`, `cargo test`, then `deploy/telemetry-functions/acceptance.py` on real collector containers. All stages share the runner's 4 vCPUs, so this is a correctness smoke, not a capacity measurement. |
| Nightly sweep | `asap-sketch-benchmark.yml` | 02:30 UTC daily, manual | Full `run-nightly.sh` sweep, then `compare-summaries.py` against the previous successful `main` run. The step fails (with an annotation and the table in the job summary) when any row's median signals/s drops more than `regression_margin` (default 10%). |
| On-demand comparison | `benchmark-on-demand.yml` | PR comment starting with `/benchmark` from an owner, member or collaborator | Reduced sweep (25k and 100k offered/s/source, 65,536-point windows, 1 repetition) on `main` and on the PR head; the comparison table is posted as a PR comment. |

The benchmark tiers run on the runner named by the repository variable
`BENCH_RUNNER` (a self-hosted runner label, which needs Docker, `gh` and
enough cores for exclusive placement) and fall back to `ubuntu-24.04` with
`--allow-shared-infrastructure` when it is unset. Shared runners are noisy, so
treat a regression flagged there as a prompt to re-run, not a verdict.

Compare any two runs locally:

```sh
benchmarks/compare-summaries.py BASE/streaming/summary.csv NEW/streaming/summary.csv --margin 0.10
```

## Viewing nightly results

There is not yet a published ASAP dashboard. Results are available as GitHub
Actions artifacts:

1. Open the repository's **Actions** tab and select **ASAP sketch benchmark**.
2. Open a successful scheduled, manual, or pull-request run.
3. Download the `asap-sketch-benchmark-<run-id>` artifact from **Artifacts**.
4. Read `streaming/summary.json`, `streaming/capacity.json`, and the
   `throughput.svg` / `resources.svg` charts.

Artifacts expire after 30 days, so the regression check compares against the
previous successful run rather than a long history.

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

The dashboard should remain informational; the nightly regression check
above is the only automated threshold. If stable cross-project numbers are
required later, the same summary format can be fed by an upstream OTAP
shared-server contribution.

## Upstream references

- [Nightly dashboard](https://open-telemetry.github.io/otel-arrow/benchmarks/nightly/)
  and [nightly suites](https://github.com/open-telemetry/otel-arrow/tree/main/tools/pipeline_perf_test/test_suites/integration/nightly)
- [Comparison dashboard](https://open-telemetry.github.io/otel-arrow/compare/)
  and [comparison suites](https://github.com/open-telemetry/otel-arrow/tree/main/tools/pipeline_perf_test/test_suites/comparison_dashboard)
- [Traffic generator](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/dev-nodes/src/receivers/traffic_generator/README.md)
- [Admin debug API](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/admin/README.md#debug) and
  [Samply](https://github.com/mstange/samply)
