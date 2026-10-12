#!/usr/bin/env bash
# Build asap-collector, package it, and start every stage plus the observers.
# USE_CASE=latency (default), cardinality, drift or topk picks the scenario.
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
export USE_CASE=${USE_CASE:-latency}
"${docker[@]}" compose -f deploy/telemetry-functions/compose.yaml up -d
echo "Grafana: http://localhost:${GRAFANA_PORT:-3000}/d/asap-$USE_CASE"
