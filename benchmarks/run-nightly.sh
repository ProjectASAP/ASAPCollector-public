#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
result_dir=${ASAP_BENCH_RESULT_DIR:-"$repo_root/benchmark-results"}
mkdir -p "$result_dir"

cd "$repo_root/asap-precompute-rs"
command -v /usr/bin/time >/dev/null

/usr/bin/time -v -o "$result_dir/resource-usage.txt" \
  cargo bench --features otap-engine --bench asap_sketch_pipeline -- --noplot \
  2>&1 | tee "$result_dir/criterion-output.txt"

cp "$repo_root/benchmarks/nightly/asap-sketch.yaml" "$result_dir/scenario.yaml"
echo "results=$result_dir"
