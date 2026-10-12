#!/usr/bin/env python3
"""Tables and a figure from experiments.py output.

    report_experiments.py RESULTS_DIR OUT_DIR --machine "56-core x86 server ..."

Reads pipelines.csv and accuracy-size*.csv from RESULTS_DIR and writes
experiments.md and pipelines.png to OUT_DIR. Numbers are absolute.
"""
import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

NAMES = {"latency": "Latency quantiles (KLL)", "cardinality": "Attribute cardinality (HLL)",
         "topk": "Heavy hitters (Count-Min + heap)"}


def rows(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def num(r, k):
    try:
        return float(r[k])
    except (KeyError, ValueError):
        return float("nan")


PIPES = {"sketch": "#2563eb", "exact": "#ea580c", "raw": "#64748b"}


def pipelines(data, out):
    lines = ["## Sketch, exact and raw pipelines", "",
             "The three pipelines run side by side, fed identical points by their own generators, each stage "
             "on its own CPU. *Delivered* is points per second that reach the end of the pipeline (both "
             "sources). *Branch* is one source's first stage (the average of the two), *merge* the second "
             "stage, *end* the detector or the raw sink. *Delay* is the window completion delay: seconds from "
             "a window's end until the detector has emitted its answers (sketch, exact) or the raw store has "
             "acknowledged its last point (raw). *Function cost* is the sketch branch's CPU minus the raw "
             "branch's (which only forwards points), per point.", ""]
    cases = [c for c in NAMES if any(r["use_case"] == c for r in data)]
    fig, axes = plt.subplots(2, len(cases), figsize=(4 * len(cases), 6.4), dpi=150, squeeze=False)
    for col, case in enumerate(cases):
        rs = sorted((r for r in data if r["use_case"] == case), key=lambda r: int(r["offered_per_source"]))
        x = [int(r["offered_per_source"]) / 1000 for r in rs]
        for name, color in PIPES.items():
            axes[0][col].plot(x, [num(r, f"{name}_branch_cpu_cores") for r in rs], marker="o", color=color, label=name)
            axes[1][col].plot(x, [num(r, f"{name}_delay_s") for r in rs], marker="o", color=color, label=name)
        axes[0][col].set_title(NAMES[case], fontsize=9)
        axes[1][col].set_xlabel("offered points/s per source (thousands)")
        for ax in axes[:, col]:
            ax.grid(alpha=0.3)
        lines += [f"### {NAMES[case]}", "",
                  "| Offered per source | Pipeline | Delivered | Branch CPU | Merge CPU | End CPU | Memory (all stages) | Delay | Function cost |",
                  "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for r in rs:
            rate = int(r["offered_per_source"])
            per_branch = num(r, "raw_delivered_per_s") / 2
            for name in PIPES:
                cost = ""
                if name == "sketch":
                    added = num(r, "sketch_branch_cpu_cores") - num(r, "raw_branch_cpu_cores")
                    cost = f"{added / per_branch * 1e6:.1f} µs/point"
                lines.append(
                    f"| {rate:,} | {name} | {num(r, f'{name}_delivered_per_s'):,.0f}/s | "
                    f"{num(r, f'{name}_branch_cpu_cores'):.3f} cores | {num(r, f'{name}_merge_cpu_cores'):.3f} cores | "
                    f"{num(r, f'{name}_end_cpu_cores'):.3f} cores | {num(r, f'{name}_rss_mib'):.0f} MiB | "
                    f"{num(r, f'{name}_delay_s'):.1f} s | {cost} |")
        lines.append("")
    axes[0][0].set_ylabel("branch CPU cores")
    axes[1][0].set_ylabel("window completion delay (s)")
    axes[0][0].legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "pipelines.png")
    lines += ["![Branch CPU and window completion delay of each pipeline](pipelines.png)", ""]
    return lines


def accuracy_size(data):
    lines = ["## Accuracy versus sketch size", "",
             "One window per setting, at 20,000 points/s per source. KLL: the latency scenario (20 routes). "
             "Count-Min + heap: the top-k scenario's 100,000 Zipf routes. Merge bytes per window is what the "
             "merge stage receives from both branches.", "",
             "| Setting | p99 error, median / max (routes >= 20,000 points) | Recall@10 | Mean count error, top 10 | Merge bytes per window |",
             "| --- | ---: | ---: | ---: | ---: |"]
    for r in data:
        p99 = f"{num(r, 'p99_rel_error_median'):.2%} / {num(r, 'p99_rel_error_max'):.2%}" if r.get("p99_rel_error_median") else ""
        recall = f"{num(r, 'recall_at_10'):.2f}" if r.get("recall_at_10") else ""
        cerr = f"{num(r, 'count_rel_error_mean'):.2%}" if r.get("count_rel_error_mean") else ""
        lines.append(f"| {r['param']} | {p99} | {recall} | {cerr} | {num(r, 'merge_bytes_per_window'):,.0f} B |")
    return lines + [""]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("results", type=Path)
    p.add_argument("out", type=Path)
    p.add_argument("--machine", required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    md = ["# Telemetry-function experiments", "",
          f"{a.machine}. Every stage is an `asap-collector` container pinned to its own CPU "
          "(deploy/telemetry-functions), with 10-second windows (`experiments.py`); "
          "each point is one 60-second measurement after a 40-second warm-up.", ""]
    if (a.results / "pipelines.csv").exists():
        md += pipelines(rows(a.results / "pipelines.csv"), a.out)
    accuracy = [r for f in sorted(a.results.glob("accuracy-size*.csv")) for r in rows(f)]
    if accuracy:
        md += accuracy_size(accuracy)
    (a.out / "experiments.md").write_text("\n".join(md))
    for f in a.results.glob("*.csv"):
        (a.out / f.name).write_text(f.read_text())
    print(f"wrote {a.out / 'experiments.md'}")


if __name__ == "__main__":
    main()
