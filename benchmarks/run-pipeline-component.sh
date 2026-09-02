#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
result_dir=${ASAP_BENCH_RESULT_DIR:-"$repo_root/benchmark-results/kll-runtime-pipeline"}
points_per_source=${ASAP_BENCH_POINTS_PER_SOURCE:-65536}
scenario=${ASAP_BENCH_SCENARIO:-kll}
demo_bin=${ASAP_DEMO_BIN:-"$repo_root/asap-precompute-rs/target/release/asap-otap-demo"}

test -x "$demo_bin"
mkdir -p "$result_dir"
chmod 0777 "$result_dir"

iteration=0
completed_iterations=0
signals=0
transport_bytes=0
started_ns=$(date +%s%N)
stop_requested=0
current_child=

write_summary() {
  local finished_ns elapsed_ns
  finished_ns=$(date +%s%N)
  elapsed_ns=$((finished_ns - started_ns))
  {
    echo "iterations=$completed_iterations"
    echo "input_signals=$signals"
    echo "serialized_transport_bytes=$transport_bytes"
    echo "elapsed_nanoseconds=$elapsed_ns"
    if (( elapsed_ns > 0 )); then
      echo "signals_per_second=$((signals * 1000000000 / elapsed_ns))"
      echo "serialized_bytes_per_second=$((transport_bytes * 1000000000 / elapsed_ns))"
    else
      echo "signals_per_second=0"
      echo "serialized_bytes_per_second=0"
    fi
  } > "$result_dir/throughput.env"
}

request_stop() {
  stop_requested=1
  if [[ -n "$current_child" ]]; then
    kill -TERM "$current_child" 2>/dev/null || true
  fi
}
trap request_stop TERM INT
trap write_summary EXIT

while (( stop_requested == 0 )); do
  iteration=$((iteration + 1))
  run_dir="$result_dir/current"
  mkdir -p "$run_dir"
  chmod 0777 "$run_dir"
  "$demo_bin" \
    --scenario "$scenario" \
    --output-dir "$run_dir" \
    --result-manifest "$run_dir/result.json" \
    --points-per-source "$points_per_source" &
  current_child=$!
  if ! wait "$current_child"; then
    if (( stop_requested != 0 )); then
      break
    fi
    exit 1
  fi
  current_child=
  completed_iterations=$((completed_iterations + 1))
  signals=$((signals + points_per_source * 2))
  for artifact in a.otlp b.otlp sa.otlp sb.otlp merged.otlp out.otlp; do
    artifact_bytes=$(wc -c < "$run_dir/$artifact")
    transport_bytes=$((transport_bytes + artifact_bytes))
  done
  write_summary
done
