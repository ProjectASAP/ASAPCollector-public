#!/usr/bin/env bash
# Build asap-collector, package it, and start every stage plus the observers.
# DOCKER_COMMAND="sudo -n docker" when Docker needs sudo. Stop with:
#   $DOCKER_COMMAND compose -f deploy/telemetry-functions/compose.yaml down
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"
target=${CARGO_TARGET_DIR:-"$repo_root/asap-precompute-rs/target"}
read -r -a docker <<< "${DOCKER_COMMAND:-docker}"
cargo build --manifest-path asap-precompute-rs/Cargo.toml --release --features collector --bin asap-collector
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
cp "$target/release/asap-collector" "$context/"
strip --strip-debug "$context/asap-collector"
"${docker[@]}" build -q -f deploy/collector.Dockerfile -t asap-collector:local "$context"
# Every generator counts scenario time from this second, so the three
# pipelines get identical points.
export SCENARIO_START=${SCENARIO_START:-$(($(date +%s) + 10))}
"${docker[@]}" compose -f deploy/telemetry-functions/compose.yaml up -d
echo "Grafana: http://localhost:${GRAFANA_PORT:-3000}/d/asap-latency"
