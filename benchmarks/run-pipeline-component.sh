#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
result_dir=${ASAP_BENCH_RESULT_DIR:-"$repo_root/benchmark-results/kll-runtime-pipeline"}
points_per_source=${ASAP_BENCH_POINTS_PER_SOURCE:-65536}
scenario=${ASAP_BENCH_SCENARIO:-kll}
generator_threads=${ASAP_BENCH_GENERATOR_THREADS:-2}
traffic=${ASAP_BENCH_TRAFFIC:-semantic}
demo_bin=${ASAP_DEMO_BIN:-"$repo_root/asap-precompute-rs/target/release/asap-otap-demo"}

test -x "$demo_bin"
mkdir -p "$result_dir"
chmod 0777 "$result_dir"
stage_log="$result_dir/stages.jsonl"
: > "$stage_log"
if [[ -z "${ASAP_GENERATOR_CORES:-}" || -z "${ASAP_PROCESSOR_CORES:-}" ]]; then
  core_sets=$(python3 -c 'import os,sys; cores=sorted(os.sched_getaffinity(0))[:4]; sys.exit("benchmark needs at least two available CPUs") if len(cores)<2 else None; split=max(1,len(cores)//2); print(",".join(map(str,cores[:split]))); print(",".join(map(str,cores[split:])))')
  export ASAP_GENERATOR_CORES=${ASAP_GENERATOR_CORES:-$(sed -n '1p' <<< "$core_sets")}
  export ASAP_PROCESSOR_CORES=${ASAP_PROCESSOR_CORES:-$(sed -n '2p' <<< "$core_sets")}
fi
python3 -c 'import json,sys; json.dump(dict(scenario=sys.argv[1], points_per_source=int(sys.argv[2]), traffic=sys.argv[3], generator_threads=int(sys.argv[4]), generator_cores=sys.argv[5], processor_cores=sys.argv[6], debug_disabled=sys.argv[7]=="1"), open(sys.argv[8], "w"), indent=2)' \
  "$scenario" "$points_per_source" "$traffic" "$generator_threads" "$ASAP_GENERATOR_CORES" "$ASAP_PROCESSOR_CORES" "${ASAP_BENCH_DISABLE_DEBUG:-0}" "$result_dir/config.json"

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
  python3 "$repo_root/benchmarks/summarize-stages.py" "$stage_log" "$result_dir/stages.json"
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
  if [[ "$iteration" == 1 && "${ASAP_BENCH_DISABLE_DEBUG:-0}" == 1 ]]; then
    python3 -c 'from pathlib import Path; import sys; [path.unlink() for path in Path(sys.argv[1]).glob("*.debug.log")]' "$run_dir"
  fi
  "$demo_bin" \
    --scenario "$scenario" \
    --output-dir "$run_dir" \
    --result-manifest "$run_dir/result.json" \
    --points-per-source "$points_per_source" \
    --traffic "$traffic" \
    --generator-threads "$generator_threads" &
  current_child=$!
  if ! wait "$current_child"; then
    if (( stop_requested != 0 )); then
      break
    fi
    exit 1
  fi
  current_child=
  for stage in a b sa sb merged out; do
    test -s "$run_dir/$stage.metrics.json"
    python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); data.update(stage=sys.argv[2], iteration=int(sys.argv[3]), scenario=sys.argv[4], points_per_source=int(sys.argv[5])); print(json.dumps(data))' \
      "$run_dir/$stage.metrics.json" "$stage" "$iteration" "$scenario" "$points_per_source" >> "$stage_log"
  done
  completed_iterations=$((completed_iterations + 1))
  signals=$((signals + points_per_source * 2))
  for artifact in a.otlp b.otlp sa.otlp sb.otlp merged.otlp out.otlp; do
    artifact_bytes=$(wc -c < "$run_dir/$artifact")
    transport_bytes=$((transport_bytes + artifact_bytes))
  done
  write_summary
done
