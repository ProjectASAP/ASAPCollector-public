# Sustained OTLP network benchmark

`./benchmarks/run-nightly.sh` now runs persistent streaming, not repeated file-backed demos.
The historical batch experiment is still available as `./benchmarks/run-file-nightly.sh`;
its numbers must not be mixed with this benchmark's results.

## Topology and transport

Seven long-lived containers run concurrently for each scenario:

```text
generator A -> branch A --\
                          -> merge -> estimate -> validating backend
generator B -> branch B --/
```

Every arrow is standard, uncompressed OTLP/HTTP protobuf over real TCP connections
between Docker network namespaces. Workers use upstream `urn:otel:receiver:otlp`
and `urn:otel:exporter:otlp_http` in actual OTAP `RuntimePipeline` instances.
HTTP clients reuse connections. There are no telemetry files, process restarts,
or per-window pipeline restarts during measurement. A separate control network
carries readiness, counters, flow-control credits, and shutdown commands.

The benchmark processor uses the production `KLLWrapper` and codec, but has
**count-aligned benchmark windows**, not the production processor's processing-time
timer. This ensures exact and approximate queries include identical observations
from both sources even when one source is slower. It does not test production
wall-clock rotation, late-event handling, or window-watermark policies.

- Raw forwards batches without artificial intermediate decoding. Backend checks
  every value, source, offset, and window, and counts completed windows.
- Exact retains and sorts each branch's window, sends sorted runs in bounded
  batches, merges the two runs, then returns exact p50/p99.
- KLL updates a `k=400` sketch incrementally per input batch, sends one ASAPv1
  Msgpack sketch per source/window, merges them, then returns p50/p99.
- Backend validates every quantile result. Exact must match; KLL tolerance is
  `max(abs(reference) * 0.05, 0.001)`. This is value error, not rank error.

The deterministic long-tail HTTP-duration corpus and fixed labels are the same
as the file benchmark. Each window replays that corpus; the sources use disjoint
index ranges. Reference quantiles are computed once at startup, outside timing.
Generation itself is live and allocates only one small batch at a time. Benchmark
window/source/offset/start-time attributes travel with OTLP data for correctness
and latency accounting; their overhead is included for all scenarios.

## Default experiment

| Setting | Value |
| --- | --- |
| Sources | 2 generator processes, one streaming sender each |
| Batch | 1,024 observations per OTLP request |
| Window | 65,536 observations per source; 131,072 combined |
| In-flight bound | 4 windows end-to-end, 8 pdata channel slots |
| Exporter concurrency | 1 request per worker, preserving per-source ordering |
| Warm-up / measured interval | 5 seconds / 30 seconds, uninterrupted traffic |
| Repetitions | 3 per scenario and network condition; rotating scenario order |
| Link conditions | Unlimited, and 100 Mbit/s egress per branch |
| Placement | Configurable generator CPU pool; one dedicated CPU each for branch A, branch B, merge, estimate, and backend |
| Container memory | 1 GiB each, swap disabled |

Linux and a working Docker daemon are required. `--generator-cores N` controls
the CPU pool shared by the two traffic generators (default 2). Five additional
CPUs are required: branch A, branch B, merge, estimate, and backend each receive
one exclusive `--cpuset-cpus` assignment. The exact mapping is recorded as
`generator_cores` and `component_cores` in every run's `config.json`. Change
`--window-points` to tune the data volume per source and `--batch-size` to tune
the OTLP request size.
Bandwidth shaping uses Linux `tc tbf` on each branch's **data** interface only
(`NET_ADMIN` only on those containers), with a 32 KiB burst and 100 ms queue.
Ingress, generator-to-branch traffic, and control traffic are not artificially
limited. This models constrained edge-to-aggregator links. It is a same-host
bridge experiment, not a physical WAN measurement; no WAN latency or packet
loss is claimed. To exercise other link capacities, use `--rates-mbit 0 10 100`.

## Measurement and correctness

The observed throughput is **backend-completed windows × observations per window
/ observed backend interval**. It never counts HTTP acceptance as completed work.
Both sources are credit-limited by backend completions, so queues cannot grow
without bound. The start and end snapshots occur during continuous traffic:
startup, initial reference sorting, and final drain are excluded. Quantile/raw
validation is included in backend work. Window latency runs from the first
source's start of that window through backend validation; p50/p99 use 1 ms bins.
It is window completion latency, not individual observation latency. Elapsed
time and latency use the host kernel's shared monotonic clock, not wall time.

At the end, generators pause at window boundaries, the lagging source completes
any missing partner windows, and the entire pipeline drains. Generated signal
count must equal backend-validated count and the expected count of complete
windows. Duplicates, corrupt values, missing batches, bad quantiles, failed
containers, HTTP failures, and drain timeouts fail the run. Fewer than two
observed completed windows also fail; increase duration for slow links/large windows.

Per-process CPU comes from `CLOCK_PROCESS_CPUTIME_ID` deltas. RSS is sampled
(default once per second); lifetime peak RSS is reported separately and includes
startup/warm-up. RX/TX are kernel byte counters on each container's data interface,
including HTTP/TCP overhead and retransmissions, excluding the separate control
network. They are real network byte rates, not serialized-file estimates.
Do not add RX and TX across all stages as unique application bytes: each hop
appears at sender and receiver. Do not sum lifetime process RSS peaks as a
simultaneous peak. Snapshot times are per process, so resource intervals have
small sampling skew; each rate uses its own elapsed interval.

## Running and artifacts

Full suite (builds release binary and Docker image; also runs isolated KLL Criterion):

```sh
./benchmarks/run-nightly.sh
```

A short smoke run, using that image:

```sh
python3 benchmarks/run-streaming.py --window-points 2048 --batch-size 256 \
  --warmup 1 --duration 5 --repetitions 1 --rates-mbit 0 10
```

If your local Docker socket needs sudo, set `DOCKER_COMMAND="sudo -n docker"`
for the shell runner, or add `--docker-command 'sudo -n docker'`
to the Python command. The driver removes only the uniquely named containers and
networks it creates, including on failure; it saves logs before removal.

`benchmark-results/streaming/` contains:

- `summary.csv`, `summary.json`, `throughput.svg`: repeated throughput and latency summaries.
- `runs.json`: individual results with per-component CPU/RSS/RX/TX.
- `resources.csv`, `resources.json`: per-component medians across repetitions.
- `<rate>mbit/<scenario>/<repeat>/config.json`: workload, CPU sets, image ID and interfaces.
- `snapshots.json`: raw before/interval/drained counters and latency histograms.
- `result.json` and seven component logs per run.

CI runs the full 18-run matrix, checks actual network traffic, completed windows,
zero loss after drain, and resource artifacts. It uploads results even on failure.
The old PR's 2.43× was measured with the file-backed batch harness and is not a
result of this streaming implementation. New speedups must be measured, not assumed.
