#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
result_dir=${ASAP_BENCH_RESULT_DIR:-"$repo_root/benchmark-results"}
mkdir -p "$result_dir"

cd "$repo_root/asap-precompute-rs"
command -v /usr/bin/time >/dev/null

# Compilation is deliberately outside every measurement. Each timed process
# executes exactly one scenario and signal count, making CPU/RSS attributable.
cargo bench --features otap-engine --bench asap_sketch_pipeline --no-run
target_root=${CARGO_TARGET_DIR:-"$repo_root/asap-precompute-rs/target"}
bench_bin=$(find "$target_root/release/deps" -maxdepth 1 -type f -perm -111 \
  -name 'asap_sketch_pipeline-*' | head -n 1)
test -n "$bench_bin"

: > "$result_dir/criterion-output.txt"
for scenario in otap_control_pipeline asap_kll_pipeline exact_sort_reference; do
  for count in 1024 16384 131072; do
    resource_file="$result_dir/resource-${scenario}-${count}.txt"
    /usr/bin/time -v -o "$resource_file" \
      "$bench_bin" "${scenario}/${count}" --bench --noplot \
        --sample-size 20 --warm-up-time 1 --measurement-time 5 \
      2>&1 | tee -a "$result_dir/criterion-output.txt"
  done
done

cp "$repo_root/benchmarks/nightly/asap-sketch.yaml" "$result_dir/scenario.yaml"
echo "results=$result_dir"
