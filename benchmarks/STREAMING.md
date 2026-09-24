# Sustained OTLP network benchmark

`./benchmarks/run-nightly.sh` now runs persistent streaming, not repeated file-backed demos.
The historical batch experiment is still available as `./benchmarks/run-file-nightly.sh`;
its numbers must not be mixed with this benchmark's results.

For exclusive CPU cost attribution and worker call-stack sampling, see
[PROFILING.md](PROFILING.md). CPU profiling uses drained accounting boundaries
in addition to the ordinary steady observation interval.
The capacity definition, sustainable-point criteria, and speedup formula are in
[CAPACITY_DESIGN.md](CAPACITY_DESIGN.md).

## Topology and transport

Seven long-lived containers run concurrently for each scenario:

```text
generator A -> branch A --\
                          -> merge -> estimate -> validating backend
generator B -> branch B --/
```

The two sources use OTAP's upstream `urn:otel:receiver:traffic_generator` in
`synthetic` / `fresh` / `smooth` mode. Each configured generator worker is a
real OTAP `RuntimePipeline` on its own thread. Every arrow is standard,
uncompressed OTLP/HTTP protobuf over real TCP connections
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

The official generator emits generic synthetic metrics. Each branch maps the
received item count to the deterministic long-tail HTTP-duration corpus used by
the file benchmark, with disjoint source index ranges. This normalization is
part of the measured branch CPU. Normalized values remain an in-memory batch
inside the branch processor and feed raw/exact/KLL directly; they are not
encoded to pdata and immediately decoded again. Reference quantiles are computed once at
startup, outside timing. Generation is live. Benchmark
window/source/offset/start-time attributes travel with OTLP data for correctness
and latency accounting; their overhead is included for all scenarios.

## Default experiment

| Setting | Value |
| --- | --- |
| Sources | 2 OTAP traffic generator processes, 2 pipeline workers each |
| Batch | 1,024 observations per OTLP request |
| Window sweep | 16,384, 65,536, and 262,144 observations per source |
| In-flight bound | 4 windows end-to-end, 8 pdata channel slots |
| Exporter concurrency | 1 request per worker, preserving per-source ordering |
| Warm-up / measured interval | 5 seconds / 30 seconds, uninterrupted traffic |
| Offered traffic sweep | 10k, 25k, 50k, 100k, 200k, 300k, and 400k signals/s per source |
| Repetitions | 3 per scenario/configuration; rotating scenario order |
| Link conditions | Unlimited by default; optional per-branch caps with `--rates-mbit` |
| Placement | Configurable generator CPU pool; one dedicated CPU each for branch A, branch B, merge, estimate, and backend |
| Container memory | 1 GiB each, swap disabled |

Linux and a working Docker daemon are required. `--generator-cores N` controls
the OTAP pipeline workers and CPU pool used by each traffic generator (default
2; the two source containers share that pool). Five additional
CPUs are required: branch A, branch B, merge, estimate, and backend each receive
one exclusive `--cpuset-cpus` assignment. The exact mapping is recorded as
`generator_cores` and `component_cores` in every run's `config.json`. Change
`--traffic-rates 10000 25000 50000 100000 200000 300000 400000` controls offered traffic per source and
`--window-points 16384 65536 262144` controls aggregation volume per source.
Together they form the default two-dimensional ingestion-rate/window-size sweep.
`--batch-size` independently controls the OTLP request size.
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

At the end, both official generators stop concurrently and the pipeline drains.
Only windows completed by both independent sources are compared; the report
records source data left in the unmatched tail as `unpaired_tail_signals_after_stop`.
Backend-validated signal count must equal the expected count of paired complete
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
python3 benchmarks/run-streaming.py --traffic-rates 20000 --window-points 2048 \
  --batch-size 256 --warmup 1 --duration 5 --repetitions 1
```

If your local Docker socket needs sudo, set `DOCKER_COMMAND="sudo -n docker"`
for the shell runner, or add `--docker-command 'sudo -n docker'`
to the Python command. The driver removes only the uniquely named containers and
networks it creates, including on failure; it saves logs before removal.

`benchmark-results/streaming/` contains:

- `summary.csv`, `summary.json`, `throughput.svg`: offered-load throughput, delivery, backlog, sustainability, and latency summaries.
- `capacity.csv`, `capacity.json`: maximum sustainable capacity and KLL/Exact capacity speedup per window.
- `runs.json`: individual results with per-component CPU/RSS/RX/TX.
- `resources.csv`, `resources.json`, `resources.svg`: per-component CPU and memory medians.
- `traffic-<signals>-sps/window-<points>/<rate>mbit/<scenario>/<repeat>/config.json`: workload, CPU sets, image ID and interfaces.
- `snapshots.json`: raw before/interval/drained counters and latency histograms.
- `result.json` and seven component logs per run.

CI runs the full 189-run matrix, checks actual network traffic, completed windows,
zero loss after drain, and resource artifacts. It uploads results even on failure.
The old PR's 2.43× was measured with the file-backed batch harness and is not a
result of this streaming implementation. New speedups must be measured, not assumed.
