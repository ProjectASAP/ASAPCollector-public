# Top-k heavy hitters: Count-Min sketch with heap vs exact counting

The top-k demo runs the same multi-process topology as the
[KLL demo](./kll-windows.md): every processor is its own OS process in its own
container, pinned to one CPU, and every hop is OTLP/HTTP over TCP. Two
pipelines run side by side on identical keyed traffic:

```mermaid
flowchart LR
  G["generators A/B<br/>Zipf(1.1) keys"] --> B["branch A/B<br/>1 CPU each"]
  B -->|"cmsheap: one ASAPv1 sketch<br/>topk-exact: every (key, count)"| M["merge<br/>1 CPU"]
  M --> E["estimate<br/>1 CPU<br/>top 10 (tumbling + rolling)"]
  E --> V["validating backend<br/>recall@10, count error"]
```

- **`cmsheap`**: each branch inserts every key into a
  [`CMSHeapWrapper`](../asap-precompute-rs/src/sketches/cms_heap.rs), which
  wraps `asap_sketchlib::CountMinSketchWithHeap`: a 4 x 2048 Count-Min sketch plus a 10-slot
  heavy-hitter heap. One ASAPv1 envelope (`CountMinSketchWithHeap`, kind
  `0x03 0x00`) per source and window goes to merge. Merge adds the counters
  and re-ranks both heaps' candidates on the merged counters; estimate reads
  the top 10 from the heap.
- **`topk-exact`**: each branch counts keys in a hash map and ships every
  (key, count) entry; merge sums the maps; estimate sorts them for the top 10.

Keys are 64-bit values drawn from Zipf(s=1.1) over 2^20 ranks, deterministic
per corpus index, with disjoint index ranges per source. A 65,536-key window
has about 17,000 distinct keys per source and 30,000 after merging both. Every window replays the same
corpus, so the backend compares each answer with the exact top 10 computed
once at startup: `topk-exact` must match exactly, and `cmsheap` must reach
recall@10 of at least 0.9 in every window, or the run fails. With
`--rolling-windows N`, estimate also answers over the last N windows by
merging sketches or summing maps.

## Run

Linux with Docker and Docker Compose. From the repository root:

```sh
demos/kll-grafana/run-demo.sh --scenarios topk-exact cmsheap   # Ctrl+C stops
```

Open <http://localhost:3000/d/asap-topk-stream>. The dashboard shows
backend-validated throughput, processor CPU-seconds per million signals,
per-process CPU, egress and memory, both pipelines' current top 10 with
counts, recall@10, and mean relative count error over the true top 10.

![CMS with heap vs exact top-k dashboard](images/topk-streaming-grafana.png)

*56-core x86 server, 50,000 signals/s per source. Both pipelines keep up;
CMS + heap uses 1.7 processor CPU-seconds per million signals, exact 13.0,
and its top 10 matches the exact keys and order with counts within 0.6%.*

## Benchmark results

`benchmarks/run-streaming.py --scenarios raw topk-exact cmsheap
--window-points 65536 --repetitions 3` on a 56-core x86 server, four generator CPUs per source and one dedicated CPU
per processor. Median backend-validated signals/s over three runs (both
sources combined):

| Offered | Raw | Exact top-k | CMS + heap |
| ---: | ---: | ---: | ---: |
| 20,000 | 21,813 | 21,813 | 21,813 |
| 50,000 | 47,988 | 52,348 | 47,989 |
| 100,000 | 100,327 | 100,336 | 100,335 |
| 200,000 | 196,309 | 148,316 | 200,675 |
| 400,000 | 274,848 | 130,879 | 397,004 |
| 600,000 | 279,185 | 130,860 | 597,659 |
| 800,000 | 283,555 | 126,516 | 798,355 |

(Short runs round to whole windows, so low-rate rows can exceed the offer.)

![Validated throughput vs offered load](images/topk-streaming-throughput.png)

By the [capacity criteria](../benchmarks/CAPACITY_DESIGN.md), the largest
sustainable offered rate per source is 50,000 for exact top-k, 100,000 for
raw, and at least 400,000 (the highest tested) for CMS + heap. Exact top-k
plateaus near 130,000 signals/s; raw forwarding near 280,000.

Summed over branch A, branch B, merge and estimate, at the same offered load:

| Offered per source | Exact CPU | CMS + heap CPU | Exact egress | CMS + heap egress |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 0.29 cores | 0.039 cores | 43.9 Mbit/s | 0.1 Mbit/s |
| 25,000 | 0.66 cores | 0.084 cores | 102.9 Mbit/s | 0.3 Mbit/s |
| 50,000 | 1.29 cores | 0.165 cores | 201.1 Mbit/s | 0.5 Mbit/s |

Accuracy, checked by the backend in every window of every run: exact top-k
matched exactly; CMS + heap had **recall@10 = 1.0** (same ten keys, same
order) and a **0.62% mean relative count error** over the true top 10, in
tumbling windows. Count-Min only overestimates, so every reported count is
at or above the true count.

Verify the demo end to end (60 seconds):

```sh
python3 demos/kll-grafana/verify-streaming.py --scenarios topk-exact cmsheap
```

Benchmark sweep (see [STREAMING.md](../benchmarks/STREAMING.md)):

```sh
python3 benchmarks/run-streaming.py --scenarios raw topk-exact cmsheap \
  --window-points 65536 --repetitions 3
```

`summary.csv` adds `min_recall_at_k` and `median_mean_relative_count_error`
for top-k rows.
