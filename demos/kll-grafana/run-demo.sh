#!/usr/bin/env bash
# Multi-process streaming demo: every processor of every scenario's pipeline is
# its own container pinned to one CPU, connected by OTLP/HTTP over TCP. Builds
# the benchmark image, starts Prometheus/Grafana, then runs until Ctrl+C.
# Extra arguments go to benchmarks/run-streaming.py (e.g. --demo-rate 200000).
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"
cargo_target_dir=${CARGO_TARGET_DIR:-"$repo_root/asap-precompute-rs/target"}
read -r -a docker_command <<< "${DOCKER_COMMAND:-docker}"
cargo build --manifest-path asap-precompute-rs/Cargo.toml \
  --release --features stream-benchmark --bin asap-stream-bench
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
cp "$cargo_target_dir/release/asap-stream-bench" "$context/"
strip --strip-debug "$context/asap-stream-bench"
"${docker_command[@]}" build -q -f benchmarks/stream.Dockerfile -t asap-stream-benchmark:local "$context"
"${docker_command[@]}" compose -f demos/kll-grafana/compose.yaml up -d
python3 benchmarks/run-streaming.py --demo --docker-command "${DOCKER_COMMAND:-docker}" \
  --output benchmark-results/demo --scenarios exact kll --window-points 65536 \
  --rolling-windows 6 --generator-cores 2 "$@"
