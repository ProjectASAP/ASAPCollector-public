# CPU attribution for the three streaming baselines

This profiles the persistent Raw / Exact / KLL OTLP-over-TCP benchmark described
in [STREAMING.md](STREAMING.md). The system under test is **branch A + branch B +
merge + estimate**. Generators and the validating backend have separate process
CPU counters and are excluded from the worker totals.

Build the streaming image as described in STREAMING.md, then run:

```sh
python3 benchmarks/run-streaming.py \
  --image asap-stream-benchmark:local \
  --output benchmark-results/cpu-profile \
  --rates-mbit 0 --warmup 5 --duration 30 --repetitions 3 --profile-cpu

python3 benchmarks/analyze-profile.py benchmark-results/cpu-profile
```

Add `--docker-command 'sudo -n docker'` where required. No kernel settings need to
be changed. Profiling is opt-in; ordinary streaming runs do not read thread CPU
clocks inside the processor.

## What is timed

`--profile-cpu` adds exclusive, nested **CLOCK_THREAD_CPUTIME_ID** scopes around
synchronous work. They never cross `.await`, so suspension/backpressure and CPU
used by other threads cannot inflate a computation scope. Categories are:

| Category | Included work |
| --- | --- |
| `computation` | Numeric buffering, Exact sorting/merge/quantile selection; KLL update/merge/quantile query |
| `pdata_codec` | ASAP observation/envelope construction and conversion, including upstream OTAP/Arrow/protobuf conversion invoked by those adapters |
| `sketch_codec` | Sketch serialization/deserialization using the production KLL wrapper/Msgpack codec |
| `processor_bookkeeping` | Remaining synchronous processor work: benchmark labels/validation, window maps, object destruction, pass-through output allocation |
| `outside_scopes` | Process CPU minus the above: native OTLP receiver/exporter, runtime, channels, native transport conversion, allocator work outside the processor, control listener and probe overhead |

**The residual is not a measurement of pure OTAP scheduling overhead.** Likewise,
`pdata_codec` is not all OTAP engine time: it also includes the ASAP adapter's
per-point allocation and labels. Use the call-stack profiles to inspect owners.
Raw has no quantile computation and forwards pdata directly through the four
workers; its total worker CPU measures the no-computation transport/framework
path for this topology, including actual TCP/HTTP and allocation.

Warmup is drained before the CPU accounting interval, and final drain CPU is
included. The denominator is exactly the validated input count between those
boundaries. This prevents outstanding windows from distorting CPU per input.
The regular goodput interval still excludes the final drain. CPU is summed
across workers, not confused with elapsed time. `cpu_profile` in each result
contains these aligned counters; ordinary `resources` describes the observation
interval. `--account-cpu` uses the same drained boundaries without enabling
processor clock probes, for a control run.

The analyzer pools CPU and input counts across repeats, producing additive CPU
seconds per million original input signals (numerically equal to microseconds
per input), category percentages, per-role costs and per-run ranges. Each branch
processes half of the original inputs; role tables retain the **global input**
denominator. They are contributions to the total, not per-local-item costs.

## Sample actual CPU stacks

Install a recent Linux `perf` with DWARF unwinding support. This workflow was
verified with perf 6.8 on a 5.15 kernel. The older perf 5.15 on the test host
misresolved container libc inline frames and omitted user frames with
`--no-inline`; do not use those profiles. `c++filt -s rust` (binutils) is used by
the analyzer to demangle Rust v0 symbols.

```sh
python3 benchmarks/run-streaming.py \
  --image asap-stream-benchmark:local \
  --output benchmark-results/cpu-stacks \
  --rates-mbit 0 --warmup 5 --duration 30 --repetitions 1 \
  --account-cpu --perf-command 'sudo -n perf'

python3 benchmarks/analyze-profile.py benchmark-results/cpu-profile \
  --samples-root benchmark-results/cpu-stacks
```

Use the explicit path to a newer `perf` executable if the `perf` wrapper selects
an older kernel-matching binary. Sampling uses `cpu-clock` at 199 Hz per active
thread and 16 KiB DWARF stack captures, attached only to the four worker PIDs.
It captures CPU activity, not off-CPU waits or physical network capacity.

Each run saves `perf.data`, command/version, PID-to-role mapping, sampling logs,
resolved stacks, and exact container ELF files under `profile-symbols/<image-id>`.
The symbol tree is essential: host libc may differ from container libc. The
analyzer additionally writes folded stacks, top leaf functions by role, and
sample categories. Kernel samples and unresolved frames remain explicit. Native
pdata frames with a truncated application caller remain in a separate category;
never silently count them as scheduler overhead. Sampling categories are
heuristics, not additional exclusive clock scopes, and must not be added to the
scope percentages. Inlined or truncated computation is identified more reliably
by the clock scopes.

Stack decoding happens after container cleanup using the saved ELF tree. While
the target is alive, perf may enter its mount namespace, where the host's symbol
directory is not accessible. The runner rejects profiles with no resolved
worker frames instead of treating them as zero framework cost.

For a perturbation check, repeat the first command with `--account-cpu` instead
of `--profile-cpu`, without `--perf-command`, and pass its output directory as
`--control-root` to the analyzer. A difference across runs includes host/load
variation; it is not by itself the exact cost of the probes. Sampling runs also
have profiler overhead and should not replace unprofiled throughput results.
Clock reads and accounting themselves have a cost; some is charged to the
surrounding scopes and some to the residual. No estimated probe cost is silently
subtracted from the reported measurements.

## Scope of the conclusions

These are optimized release builds on a shared host, real same-host Docker TCP,
fixed-size count-aligned windows, batch size 1,024, 65,536 points per source per
window, and KLL k=400. The benchmark uses the production KLL implementation and
codec with a benchmark processor, **not the production processor's timer and
late-event policy**. Percentages describe this particular implementation and
workload; they are not a general constant for the OTAP framework. In particular,
Exact materializes and sends full sorted windows at intermediate hops, while KLL
sends bounded sketches. A faster Exact adapter could change the comparison.
