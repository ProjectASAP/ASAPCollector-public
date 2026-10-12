# Sketch serialization and many-source merge cost

Measured with `asap-merge-bench` (`asap-precompute-rs/src/bin/asap_merge_bench.rs`, release build, rustc 1.99.0) on a 56-core x86 server (2.00 GHz, idle), pinned to one core with `taskset -c 4`, on 2026-10-12. Each source sketch holds 20,000 points: lognormal values for KLL (k=200) and DDSketch (alpha 0.01), distinct ids for HLL (default), and Zipf-like keys over 2^20 for Count-Min (4x2048), Count-Min with heap (4x2048, heap 10) and CountSketch (4x2048). `encode_us` and `decode_us` are per sketch (ASAPv1 `snapshot` / `decode_snapshot`). `merge_all_ms` is the `window_merge` path: decode the first payload, then `merge_snapshot` each of the rest. `direct_merge_all_ms` merges sketches that were already decoded with sketchlib's native `merge` (the timing includes one decode, for the accumulator). Every value is the median of 5 runs. Raw CSV: [`merge-bench.csv`](merge-bench.csv).

| sketch | sources | bytes/sketch | encode µs | decode µs | merge all ms | merge µs/sketch | direct merge all ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| kll | 10 | 2406 | 2.82 | 4.37 | 0.103 | 10.34 | 0.061 |
| kll | 100 | 2381 | 3.39 | 4.20 | 0.954 | 9.54 | 0.489 |
| kll | 1000 | 2388 | 3.48 | 4.26 | 9.332 | 9.33 | 4.699 |
| ddsketch | 10 | 701 | 4.41 | 4.28 | 0.049 | 4.91 | 0.011 |
| ddsketch | 100 | 684 | 4.42 | 4.18 | 0.494 | 4.94 | 0.080 |
| ddsketch | 1000 | 682 | 4.36 | 4.19 | 4.925 | 4.93 | 0.800 |
| hll | 10 | 16720 | 8.52 | 8.40 | 0.395 | 39.51 | 0.323 |
| hll | 100 | 16720 | 8.02 | 7.53 | 2.245 | 22.45 | 1.488 |
| hll | 1000 | 16720 | 8.27 | 8.31 | 17.876 | 17.88 | 10.746 |
| countminsketch | 10 | 8622 | 163.49 | 65.01 | 0.692 | 69.20 | 0.105 |
| countminsketch | 100 | 8623 | 163.17 | 65.59 | 7.002 | 70.02 | 0.478 |
| countminsketch | 1000 | 8622 | 166.17 | 65.44 | 69.446 | 69.45 | 6.737 |
| countminsketch_heap | 10 | 8766 | 165.86 | 68.79 | 0.798 | 79.81 | 0.154 |
| countminsketch_heap | 100 | 8767 | 165.66 | 69.31 | 7.999 | 79.99 | 0.952 |
| countminsketch_heap | 1000 | 8767 | 167.85 | 69.45 | 80.303 | 80.30 | 11.941 |
| countsketch | 10 | 8704 | 126.66 | 120.36 | 1.245 | 124.50 | 0.154 |
| countsketch | 100 | 8706 | 126.73 | 119.85 | 12.307 | 123.07 | 0.532 |
| countsketch | 1000 | 8706 | 129.55 | 121.28 | 124.293 | 124.29 | 6.825 |

Findings. On one core, merging 1,000 source sketches for one series and window through the `window_merge` path takes 9.3 ms for KLL, 4.9 ms for DDSketch and 17.9 ms for HLL, but 69 ms for Count-Min, 80 ms for Count-Min with heap and 124 ms for CountSketch. The cost per source is flat from 100 to 1,000 sources, so it grows linearly with the source count. Decoding the ASAPv1 payloads accounts for roughly 46% of the merge time for KLL and HLL (1,000 × 4.3 µs of 9.3 ms; 1,000 × 8.3 µs of 17.9 ms), 85% for DDSketch, and 86–98% for the three 4x2048 counter sketches: merging already-decoded Count-Min sketches takes 6.7 ms, against 69 ms from bytes. Encoding a 4x2048 counter sketch (127–168 µs) is also 15–50x slower than encoding KLL, DDSketch or HLL. These numbers cover single-threaded encode, decode and merge only, not network transfer or the rest of the pipeline.
