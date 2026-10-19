#!/usr/bin/env python3
"""Compare two run-streaming.py summary.csv files.

Prints a Markdown table of median_signals_per_second for the rows present in
both (same scenario, offered rate, window and egress cap) and exits 1 when any
row dropped by more than --margin (a fraction; 0.10 = 10%).

    benchmarks/compare-summaries.py BASE.csv NEW.csv --margin 0.10
"""
import argparse
import csv
import sys

KEY = ("scenario", "target_signals_per_second_per_source", "window_points_per_source", "branch_egress_mbit_per_second")


def load(path):
    with open(path, newline="") as f:
        return {tuple(row.get(k, "") for k in KEY): float(row["median_signals_per_second"]) for row in csv.DictReader(f)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("base")
    parser.add_argument("new")
    parser.add_argument("--margin", type=float, default=0.10)
    args = parser.parse_args()
    base, new = load(args.base), load(args.new)
    print("| scenario | offered/s/source | window | base signals/s | new signals/s | change |")
    print("|---|---:|---:|---:|---:|---:|")
    regressed = 0
    for key in (k for k in base if k in new):
        scenario, offered, window, _ = key
        change = new[key] / base[key] - 1 if base[key] else 0.0
        flag = ""
        if change < -args.margin:
            regressed += 1
            flag = " **regression**"
        print(f"| {scenario} | {int(float(offered)):,} | {int(float(window)):,} | {base[key]:,.0f} | {new[key]:,.0f} | {change:+.1%}{flag} |")
    print(f"\n{regressed} of {len(base.keys() & new.keys())} rows dropped more than {args.margin:.0%}.")
    sys.exit(1 if regressed else 0)


if __name__ == "__main__":
    main()
