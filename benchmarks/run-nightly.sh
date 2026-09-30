#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
python_bin=${PYTHON:-python3}
cargo_target_dir=${CARGO_TARGET_DIR:-"$repo_root/asap-precompute-rs/target"}
read -r -a docker_command <<< "${DOCKER_COMMAND:-docker}"
mkdir -p benchmark-results
cargo build --manifest-path asap-precompute-rs/Cargo.toml \
  --release --features stream-benchmark --bin asap-stream-bench
cargo bench --manifest-path asap-precompute-rs/Cargo.toml \
  --features otap-engine --bench asap_sketch_pipeline -- \
  kll_isolated_stages --sample-size 10 --warm-up-time 1 --measurement-time 1 \
  | tee benchmark-results/kll-isolated-stages.txt
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
cp "$cargo_target_dir/release/asap-stream-bench" "$context/"
strip --strip-debug "$context/asap-stream-bench"
"${docker_command[@]}" build -f benchmarks/stream.Dockerfile -t asap-stream-benchmark:local "$context"
"$python_bin" benchmarks/run-streaming.py --docker-command "${DOCKER_COMMAND:-docker}" --output benchmark-results/streaming "$@"
