#!/usr/bin/env python3
"""Tables and a figure from experiments.py output.

    report_experiments.py RESULTS_DIR OUT_DIR --machine "56-core x86 server ..."

Reads from RESULTS_DIR: inline-cost.csv (exact stage only counting, so it
never throttles the branch), accuracy-size*.csv, and optionally
exact-baseline.csv (an inline-cost run whose exact stage computes full exact
answers). Writes experiments.md and inline-cost.png to OUT_DIR. Numbers are
absolute.
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


def inline_cost(data, out):
    lines = ["## Inline cost", "",
             "Each branch receives one source. *Without* forwards raw points only; *with* adds the fanout "
             "and the sketch. CPU is the branch pipeline's own CPU time (average of the two branches). "
             "The exact stage only counts points here, so it never slows the branches; "
             "*added per point* divides the extra branch CPU by the points each branch delivered.", ""]
    fig, axes = plt.subplots(1, len(NAMES), figsize=(12, 3.6), dpi=150, sharey=True)
    for ax, (case, name) in zip(axes, NAMES.items()):
        rs = [r for r in data if r["use_case"] == case]
        if not rs:
            continue
        rates = sorted({int(r["offered_per_source"]) for r in rs})
        get = lambda variant, k: [num(next(r for r in rs if r["variant"] == variant and int(r["offered_per_source"]) == x), k) for x in rates]
        without, with_ = get("without", "branch_cpu_cores"), get("with", "branch_cpu_cores")
        delivered = get("with", "delivered_points_per_s")
        x = [r / 1000 for r in rates]
        ax.plot(x, without, marker="o", color="#64748b", label="branch, without function")
        ax.plot(x, with_, marker="o", color="#2563eb", label="branch, with function")
        ax.set_title(name, fontsize=9)
        ax.set_xlabel("offered points/s per source (thousands)")
        ax.grid(alpha=0.3)
        lines += [f"### {name}", "",
                  "| Offered per source | Delivered per branch | Branch CPU without | Branch CPU with | Added per point | Branch memory with | Sketch answer delay |",
                  "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for rate, dv, a, b, m, d1 in zip(rates, delivered, without, with_, get("with", "branch_rss_mib"), get("with", "sketch_answer_delay_s")):
            per_branch = dv / 2
            lines.append(f"| {rate:,} | {per_branch:,.0f} | {a:.3f} cores | {b:.3f} cores | {(b - a) / per_branch * 1e6:.1f} µs | {m:.0f} MiB | {d1:.1f} s |")
        lines.append("")
    axes[0].set_ylabel("CPU cores")
    axes[0].legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "inline-cost.png")
    lines += ["![Branch CPU with and without each function](inline-cost.png)", ""]
    return lines


def exact_baseline(data):
    lines = ["## Exact baseline cost", "",
             "The alternative to inline sketches: ship every raw point to one place and compute exact answers. "
             "Here the exact stage (one pinned CPU) holds each window's raw values or distinct values; when it "
             "saturates, fanout back-pressure lowers the delivered rate. Delivered rates are approximate "
             "(5-6 ten-second windows per measurement).", "",
             "| Use case | Offered (both sources) | Exact stage CPU | Points delivered |",
             "| --- | ---: | ---: | ---: |"]
    for r in data:
        if r["variant"] != "with":
            continue
        lines.append(f"| {NAMES.get(r['use_case'], r['use_case'])} | {2 * int(r['offered_per_source']):,}/s | "
                     f"{num(r, 'exact_cpu_cores'):.2f} cores | {num(r, 'delivered_points_per_s'):,.0f}/s |")
    return lines + [""]


def accuracy_size(data):
    lines = ["## Accuracy versus sketch size", "",
             "One window per setting, at 20,000 points/s per source. KLL: the latency scenario (20 routes). "
             "Count-Min + heap: 100,000 Zipf routes so collisions matter. Merge bytes per window is what the "
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
    if (a.results / "inline-cost.csv").exists():
        md += inline_cost(rows(a.results / "inline-cost.csv"), a.out)
    if (a.results / "exact-baseline.csv").exists():
        md += exact_baseline(rows(a.results / "exact-baseline.csv"))
    accuracy = [r for f in sorted(a.results.glob("accuracy-size*.csv")) for r in rows(f)]
    if accuracy:
        md += accuracy_size(accuracy)
    (a.out / "experiments.md").write_text("\n".join(md))
    for f in a.results.glob("*.csv"):
        (a.out / f.name).write_text(f.read_text())
    print(f"wrote {a.out / 'experiments.md'}")


if __name__ == "__main__":
    main()
