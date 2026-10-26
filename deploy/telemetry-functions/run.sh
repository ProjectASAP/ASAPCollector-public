#!/usr/bin/env bash
# Build asap-collector, package it, and start every stage plus the observers.
# USE_CASE=latency (default), cardinality, drift or topk picks the scenario.
# Every demo is its own compose project (tf-<use case>); one Grafana (project
# tf-grafana, port 3000) shows all of them. One demo per use case (a second start
# of the same use case restarts it). DEMO_SLOT=N (default 0) runs another demo
# beside the others: host ports +N and CPUs +18*N (18 per demo).
# NO_BUILD=1 reuses the asap-collector:local image already built.
# DOCKER_COMMAND="sudo -n docker" when Docker needs sudo. Stop a demo, or the
# shared Grafana, with:
#   $DOCKER_COMMAND compose -p tf-<use case> -f deploy/telemetry-functions/compose.yaml down
#   $DOCKER_COMMAND compose -p tf-grafana -f deploy/telemetry-functions/grafana-compose.yaml down
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"
target=${CARGO_TARGET_DIR:-"$repo_root/asap-precompute-rs/target"}
read -r -a docker <<< "${DOCKER_COMMAND:-docker}"
if [ "${NO_BUILD:-}" != 1 ]; then
  cargo build --manifest-path asap-precompute-rs/Cargo.toml --release --features collector --bin asap-collector
  context=$(mktemp -d)
  trap 'rm -rf "$context"' EXIT
  cp "$target/release/asap-collector" "$context/"
  strip --strip-debug "$context/asap-collector"
  "${docker[@]}" build -q -f deploy/collector.Dockerfile -t asap-collector:local "$context"
fi
[ "${1:-}" = "--build-only" ] && exit 0
export USE_CASE=${USE_CASE:-latency}
export PIPELINES=${PIPELINES:-./$USE_CASE}
export COMMON=${COMMON:-./common}
# Every generator counts scenario time from this second, so the three
# pipelines get identical points.
export SCENARIO_START=${SCENARIO_START:-$(($(date +%s) + 10))}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-tf-$USE_CASE}
slot=${DEMO_SLOT:-0}
if [ "$slot" != 0 ]; then
  [[ $slot =~ ^[0-9]+$ ]] || { echo "DEMO_SLOT=$slot is not a number" >&2; exit 1; }
  slot=$((10#$slot))
  if [ $((18 * slot + 17)) -ge "$(nproc)" ]; then
    echo "DEMO_SLOT=$slot needs CPUs $((18 * slot))-$((18 * slot + 17)); this host has $(nproc) (slots 0-$(($(nproc) / 18 - 1)))" >&2
    exit 1
  fi
  export PROMETHEUS_PORT=${PROMETHEUS_PORT:-$((9090 + slot))}
  export LOKI_PORT=${LOKI_PORT:-$((3100 + 2 * slot))} LOKI_RAW_PORT=${LOKI_RAW_PORT:-$((3101 + 2 * slot))}
  cpu=$((18 * slot))
  for stage in GENERATOR_A GENERATOR_B BRANCH_A BRANCH_B MERGE DETECTOR EXACT_GENERATOR_A EXACT_GENERATOR_B \
      EXACT_BRANCH_A EXACT_BRANCH_B EXACT_MERGE EXACT_DETECTOR RAW_GENERATOR_A RAW_GENERATOR_B RAW_SINK_A \
      RAW_SINK_B QUERY_JOB HH_ACCURACY; do
    var=CPU_$stage
    export "$var=${!var:-$cpu}"
    cpu=$((cpu + 1))
  done
fi
"${docker[@]}" compose -f deploy/telemetry-functions/compose.yaml up -d
"${docker[@]}" compose -p tf-grafana -f deploy/telemetry-functions/grafana-compose.yaml up -d
# Grafana reads each demo's Prometheus and Loki by container name, so it joins every
# demo's network; this also repairs the connections of the other demos when its
# container was recreated.
for network in $("${docker[@]}" network ls --format '{{.Name}}' --filter 'name=^tf-.*_default$'); do
  members=$("${docker[@]}" network inspect -f '{{range .Containers}}{{.Name}} {{end}}' "$network")
  case " $members" in *" tf-grafana-grafana-1 "*) ;; *) "${docker[@]}" network connect "$network" tf-grafana-grafana-1 ;; esac
done
echo "Grafana: http://localhost:${GRAFANA_PORT:-3000}/d/asap-$USE_CASE"
