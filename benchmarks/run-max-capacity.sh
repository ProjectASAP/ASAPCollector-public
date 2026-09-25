#!/usr/bin/env bash
set -euo pipefail
root=${1:-benchmark-results/max-capacity}
docker_command=${DOCKER_COMMAND:-docker}
mkdir -p "$root"
run_group() {
  local scenario=$1 cores=$2 rates=$3
  python3 benchmarks/run-streaming.py \
    --docker-command "$docker_command" \
    --output "$root/$scenario-generator-$cores" \
    --scenarios "$scenario" --generator-cores "$cores" \
    --traffic-rates $rates --window-points 16384 65536 262144 \
    --batch-size 1024 --warmup 5 --duration 30 \
    --minimum-observed-windows 3 --sample-interval 1 --repetitions 1
}
# Exact reaches its downstream limit at low offered load; four source cores are ample.
run_group exact 4 "5000 10000 25000 50000 100000 200000 400000"
# Raw needs a progressively stronger workload driver.
for cores in 4 8 16; do
  run_group raw "$cores" "100000 200000 400000 800000 1600000 3200000"
done
# Fixed-k KLL is cheap downstream, so continue scaling the source pool to 32
# cores and bracket its plateau with up to 6.4M offered signals/s per source.
for cores in 4 8 16 24 32; do
  run_group kll "$cores" "100000 200000 400000 800000 1600000 3200000 6400000"
done
python3 benchmarks/aggregate-max-capacity.py "$root" | tee "$root/max-capacity.txt"
