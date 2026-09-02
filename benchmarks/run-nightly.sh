#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
otap_rev=${OTAP_BENCH_REV:-3e85c3460361446ebfce99e9f35fffd2dd5ab740}
framework_dir=${OTAP_PIPELINE_PERF_DIR:-"$repo_root/.cache/otel-arrow-$otap_rev"}
python_deps=${OTAP_PIPELINE_PERF_PYTHON_DEPS:-"$repo_root/.cache/pipeline-perf-python"}
python_bin=${PYTHON:-python3}

if ! "$python_bin" -c 'import sys; raise SystemExit(sys.version_info < (3, 13))'; then
  echo "OTAP's pinned benchmark dependencies require Python 3.13 or newer." >&2
  echo "Set PYTHON to a compatible interpreter." >&2
  exit 2
fi

cargo build --manifest-path "$repo_root/asap-precompute-rs/Cargo.toml" \
  --release --features otap-engine --bin asap-otap-demo
docker build -f "$repo_root/benchmarks/Dockerfile" \
  -t asap-quantile-benchmark:local "$repo_root"

if [[ ! -f "$framework_dir/tools/pipeline_perf_test/orchestrator/run_orchestrator.py" ]]; then
  mkdir -p "$(dirname "$framework_dir")"
  git clone --filter=blob:none --no-checkout \
    https://github.com/open-telemetry/otel-arrow.git "$framework_dir"
  git -C "$framework_dir" checkout "$otap_rev"
fi

if ! PYTHONPATH="$python_deps${PYTHONPATH:+:$PYTHONPATH}" "$python_bin" -c \
  'import docker, duckdb, pandas, pyarrow, pydantic, yaml' 2>/dev/null; then
  "$python_bin" -m pip install --quiet --target "$python_deps" \
    -r "$framework_dir/tools/pipeline_perf_test/orchestrator/requirements.txt"
fi

cd "$repo_root"
PYTHONPATH="$python_deps${PYTHONPATH:+:$PYTHONPATH}" \
"$python_bin" "$framework_dir/tools/pipeline_perf_test/orchestrator/run_orchestrator.py" \
  --config "$repo_root/benchmarks/nightly/asap-sketch.yaml"
