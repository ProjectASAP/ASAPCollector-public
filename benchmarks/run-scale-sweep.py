#!/usr/bin/env python3
"""Compare raw, exact, and KLL at increasing traffic volumes."""

import argparse
import csv
import datetime
import json
import os
import statistics
import subprocess
import time
from pathlib import Path


SCENARIOS = ("raw", "exact", "kll")
COLORS = {"raw": "#64748b", "exact": "#dc2626", "kll": "#2563eb"}
ROLES = ("traffic_a", "traffic_b", "branch_a", "branch_b", "merge", "estimate")
ROLE_COLORS = {"traffic_a": "#0891b2", "traffic_b": "#06b6d4", "branch_a": "#2563eb",
               "branch_b": "#818cf8", "merge": "#dc2626", "estimate": "#9333ea"}


def cpu_sets():
    cores = sorted(os.sched_getaffinity(0))[:4]
    if len(cores) < 2:
        raise RuntimeError("the scale sweep needs at least two available CPUs")
    split = max(1, len(cores) // 2)
    return ",".join(map(str, cores[:split])), ",".join(map(str, cores[split:]))


def measure(binary, output, points, scenario, repeat, generator_cores, processor_cores):
    run_dir = output / "runs" / str(points) / scenario / str(repeat)
    run_dir.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update({
        "ASAP_GENERATOR_CORES": generator_cores,
        "ASAP_PROCESSOR_CORES": processor_cores,
        "ASAP_BENCH_DISABLE_DEBUG": "1",
    })
    command = [str(binary), "--scenario", scenario, "--traffic", "semantic",
               "--generator-threads", "2", "--points-per-source", str(points),
               "--output-dir", str(run_dir), "--result-manifest", str(run_dir / "result.json")]
    start = time.monotonic_ns()
    completed = subprocess.run(command, env=environment, capture_output=True, text=True,
                               timeout=300, check=False)
    process_wall_seconds = (time.monotonic_ns() - start) / 1e9
    (run_dir / "stdout.txt").write_text(completed.stdout)
    (run_dir / "stderr.txt").write_text(completed.stderr)
    if completed.returncode:
        raise RuntimeError(f"{scenario}, {points} per source, repetition {repeat}: "
                           f"exit {completed.returncode}; {completed.stderr[-2000:]}")
    manifest = json.loads((run_dir / "result.json").read_text())
    elapsed = manifest["pipeline_elapsed_nanoseconds"] / 1e9
    if elapsed <= 0 or elapsed > process_wall_seconds:
        raise RuntimeError(f"invalid pipeline duration: {elapsed} seconds")
    stages = {}
    for name, role in (("a", "traffic_a"), ("b", "traffic_b"), ("sa", "branch_a"),
                       ("sb", "branch_b"), ("merged", "merge"), ("out", "estimate")):
        stages[role] = json.loads((run_dir / f"{name}.metrics.json").read_text())
    # Boundary sizes remain in the stage metrics; keep the sweep artifacts compact.
    for artifact in run_dir.glob("*.otlp"):
        artifact.unlink()
    return {
        "points_per_source": points,
        "input_signals": points * 2,
        "scenario": scenario,
        "repeat": repeat,
        "elapsed_seconds": elapsed,
        "process_wall_seconds": process_wall_seconds,
        "validation_seconds": manifest["validation_elapsed_nanoseconds"] / 1e9,
        "signals_per_second": points * 2 / elapsed,
        "stages": stages,
    }


def aggregate(runs):
    groups = {}
    for run in runs:
        groups.setdefault((run["points_per_source"], run["scenario"]), []).append(run)
    rows = []
    for (points, scenario), samples in sorted(groups.items()):
        rates = [sample["signals_per_second"] for sample in samples]
        row = {
            "points_per_source": points,
            "input_signals": points * 2,
            "scenario": scenario,
            "repetitions": len(samples),
            "median_signals_per_second": statistics.median(rates),
            "min_signals_per_second": min(rates),
            "max_signals_per_second": max(rates),
            "median_elapsed_seconds": statistics.median(sample["elapsed_seconds"] for sample in samples),
        }
        rows.append(row)
    by_points = {}
    for row in rows:
        by_points.setdefault(row["points_per_source"], {})[row["scenario"]] = row
    for row in rows:
        series = by_points[row["points_per_source"]]
        if row["scenario"] == "kll":
            row["kll_over_raw"] = row["median_signals_per_second"] / series["raw"]["median_signals_per_second"]
            row["kll_over_exact"] = row["median_signals_per_second"] / series["exact"]["median_signals_per_second"]
        else:
            row["kll_over_raw"] = ""
            row["kll_over_exact"] = ""
    return rows


def aggregate_resources(runs):
    groups = {}
    for run in runs:
        for role, metrics in run["stages"].items():
            groups.setdefault((run["points_per_source"], run["scenario"], role), []).append(metrics)
    rows = []
    for (points, scenario, role), samples in sorted(groups.items()):
        rows.append({
            "points_per_source": points,
            "input_signals": points * 2,
            "scenario": scenario,
            "component": role,
            "repetitions": len(samples),
            "median_cpu_seconds": statistics.median(sample["cpu_nanoseconds"] / 1e9 for sample in samples),
            "median_peak_rss_mib": statistics.median(sample["peak_rss_kib"] / 1024 for sample in samples),
            "median_elapsed_seconds": statistics.median(sample["elapsed_nanoseconds"] / 1e9 for sample in samples),
            "median_serialization_seconds": statistics.median(sample["serialization_nanoseconds"] / 1e9 for sample in samples),
        })
    return rows


def draw_resources(rows, scenario, path):
    selected = [row for row in rows if row["scenario"] == scenario]
    points = sorted({row["points_per_source"] for row in selected})
    by_key = {(row["points_per_source"], row["component"]): row for row in selected}
    width, height = 980, 690
    left, right = 90, 920
    top1, bottom1 = 80, 325
    top2, bottom2 = 405, 625
    max_cpu = max(row["median_cpu_seconds"] for row in selected) * 1.12
    max_rss = max(row["median_peak_rss_mib"] for row in selected) * 1.12
    x_at = lambda index: left + (right - left) * index / max(1, len(points) - 1)
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>text{font:14px sans-serif;fill:#1e293b}.title{font-size:20px;font-weight:700}.small{font-size:12px}.axis{stroke:#94a3b8;stroke-width:1}</style>',
           f'<text x="90" y="32" class="title">{scenario.upper()} resource usage by component</text>',
           '<text x="90" y="56">Median of repeated full pipeline runs; CPU seconds and process peak RSS</text>']
    for top, bottom, maximum, unit in ((top1, bottom1, max_cpu, "CPU seconds"),
                                       (top2, bottom2, max_rss, "Peak RSS MiB")):
        svg.append(f'<line class="axis" x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}"/>')
        svg.append(f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{bottom}"/>')
        svg.append(f'<text x="8" y="{top-12}">{unit}</text>')
        for tick in range(5):
            y = bottom - (bottom - top) * tick / 4
            svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="#e2e8f0"/>')
            svg.append(f'<text x="{left-9}" y="{y+5:.1f}" text-anchor="end" class="small">{maximum*tick/4:.1f}</text>')
    for role in ROLES:
        color = ROLE_COLORS[role]
        for top, bottom, maximum, field in ((top1, bottom1, max_cpu, "median_cpu_seconds"),
                                            (top2, bottom2, max_rss, "median_peak_rss_mib")):
            coords = [(x_at(i), bottom - by_key[(p, role)][field] / maximum * (bottom - top))
                      for i, p in enumerate(points)]
            svg.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="' +
                       " ".join(f"{x:.1f},{y:.1f}" for x, y in coords) + '"/>')
            for x, y in coords:
                svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{color}"/>')
    for i, role in enumerate(ROLES):
        x = left + i * 138
        svg.append(f'<line x1="{x}" y1="365" x2="{x+20}" y2="365" stroke="{ROLE_COLORS[role]}" stroke-width="3"/>')
        svg.append(f'<text x="{x+24}" y="370" class="small">{role}</text>')
    for i, points_per_source in enumerate(points):
        svg.append(f'<text x="{x_at(i):.1f}" y="{bottom2+25}" text-anchor="middle">{points_per_source*2:,}</text>')
    svg.append(f'<text x="{(left+right)/2}" y="{height-20}" text-anchor="middle">Total input signals per run</text>')
    svg.append('</svg>')
    path.write_text("\n".join(svg))


def draw_svg(rows, path):
    points = sorted({row["points_per_source"] for row in rows})
    by_key = {(row["points_per_source"], row["scenario"]): row for row in rows}
    max_rate = max(row["median_signals_per_second"] for row in rows) * 1.12
    max_ratio = max(max(by_key[(p, "kll")]["kll_over_raw"], by_key[(p, "kll")]["kll_over_exact"]) for p in points) * 1.12
    width, height = 980, 690
    left, right = 90, 920
    top1, bottom1 = 75, 345
    top2, bottom2 = 420, 625
    x_at = lambda index: left + (right - left) * index / max(1, len(points) - 1)
    y_rate = lambda value: bottom1 - value / max_rate * (bottom1 - top1)
    y_ratio = lambda value: bottom2 - value / max_ratio * (bottom2 - top2)
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>text{font:14px sans-serif;fill:#1e293b}.title{font-size:20px;font-weight:700}.small{font-size:12px} .axis{stroke:#94a3b8;stroke-width:1}</style>',
           '<text x="90" y="32" class="title">Throughput versus traffic volume</text>',
           '<text x="90" y="56">Two sources; deterministic semantic data; median of repeated full pipeline runs</text>']
    for top, bottom, maximum, unit in ((top1, bottom1, max_rate, "signals/s"), (top2, bottom2, max_ratio, "KLL speedup")):
        svg.append(f'<line class="axis" x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}"/>')
        svg.append(f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{bottom}"/>')
        svg.append(f'<text x="8" y="{top-12}">{unit}</text>')
        for tick in range(5):
            value = maximum * tick / 4
            y = bottom - (bottom - top) * tick / 4
            svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="#e2e8f0"/>')
            label = f"{value/1000:.0f}k" if unit == "signals/s" else f"{value:.1f}x"
            svg.append(f'<text x="{left-9}" y="{y+5:.1f}" text-anchor="end" class="small">{label}</text>')
    for index, p in enumerate(points):
        x = x_at(index)
        svg.append(f'<text x="{x:.1f}" y="{bottom2+25}" text-anchor="middle">{2*p:,}</text>')
    svg.append(f'<text x="{(left+right)/2}" y="{height-20}" text-anchor="middle">Total input signals per run</text>')
    for scenario in SCENARIOS:
        coords = [(x_at(i), y_rate(by_key[(p, scenario)]["median_signals_per_second"])) for i, p in enumerate(points)]
        svg.append(f'<polyline fill="none" stroke="{COLORS[scenario]}" stroke-width="3" points="' + " ".join(f"{x:.1f},{y:.1f}" for x, y in coords) + '"/>')
        for x, y in coords:
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{COLORS[scenario]}"/>')
    for field, color, label in (("kll_over_raw", "#0f766e", "KLL / raw"),
                                ("kll_over_exact", "#7c3aed", "KLL / exact")):
        coords = [(x_at(i), y_ratio(by_key[(p, "kll")][field])) for i, p in enumerate(points)]
        svg.append(f'<polyline fill="none" stroke="{color}" stroke-width="3" points="' + " ".join(f"{x:.1f},{y:.1f}" for x, y in coords) + '"/>')
        for x, y in coords:
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}"/>')
    for i, (label, color) in enumerate([("raw", COLORS["raw"]), ("exact", COLORS["exact"]), ("KLL", COLORS["kll"]),
                                        ("KLL / raw", "#0f766e"), ("KLL / exact", "#7c3aed")]):
        x = left + i * 160
        svg.append(f'<line x1="{x}" y1="390" x2="{x+25}" y2="390" stroke="{color}" stroke-width="3"/>')
        svg.append(f'<text x="{x+31}" y="395">{label}</text>')
    svg.append('</svg>')
    path.write_text("\n".join(svg))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("asap-precompute-rs/target/release/asap-otap-demo"))
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/scale-sweep"))
    parser.add_argument("--points", type=int, nargs="+", default=[32768, 65536, 262144, 524288])
    parser.add_argument("--repetitions", type=int, default=2)
    args = parser.parse_args()
    if args.repetitions < 1 or any(points < 1 for points in args.points):
        parser.error("repetitions and points must be positive")
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(f"binary does not exist: {binary}")
    args.output.mkdir(parents=True, exist_ok=True)
    generator_cores, processor_cores = cpu_sets()
    configuration = {"points_per_source": sorted(set(args.points)), "repetitions": args.repetitions,
                     "generator_cores": generator_cores, "processor_cores": processor_cores,
                     "generator_threads_per_source": 2, "traffic": "semantic", "debug_disabled": True,
                     "traffic_seed": "0x9e3779b97f4a7c15",
                     "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     "binary": str(binary)}
    (args.output / "config.json").write_text(json.dumps(configuration, indent=2) + "\n")
    runs = []
    for points in configuration["points_per_source"]:
        for repeat in range(1, args.repetitions + 1):
            for scenario in SCENARIOS:
                run = measure(binary, args.output, points, scenario, repeat, generator_cores, processor_cores)
                runs.append(run)
                (args.output / "runs.json").write_text(json.dumps(runs, indent=2) + "\n")
                print(f'{points*2:>9} signals {scenario:>5} #{repeat}: {run["signals_per_second"]:,.0f} signals/s', flush=True)
    rows = aggregate(runs)
    resources = aggregate_resources(runs)
    (args.output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    with (args.output / "summary.csv").open("w", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    draw_svg(rows, args.output / "throughput.svg")
    (args.output / "resources.json").write_text(json.dumps(resources, indent=2) + "\n")
    with (args.output / "resources.csv").open("w", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(resources[0]))
        writer.writeheader()
        writer.writerows(resources)
    for scenario in SCENARIOS:
        draw_resources(resources, scenario, args.output / f"{scenario}-resources.svg")


if __name__ == "__main__":
    main()
