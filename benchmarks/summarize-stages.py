#!/usr/bin/env python3
"""Summarize per-process metrics from completed benchmark iterations."""

import json
import os
import sys
from collections import defaultdict


def summarize(records):
    groups = defaultdict(list)
    for record in records:
        groups[record["stage"]].append(record)
    result = {"schema_version": 1, "stages": {}}
    ticks = os.sysconf("SC_CLK_TCK")
    for stage, items in sorted(groups.items()):
        elapsed = sum(item["elapsed_nanoseconds"] for item in items) / 1e9
        if stage in ("a", "b", "sa", "sb"):
            unit = "signals"
            inputs = sum(item["points_per_source"] for item in items)
        else:
            unit = "sketches" if items[0]["scenario"] == "kll" else "batches"
            inputs = len(items) * (2 if stage == "merged" else 1)
        equivalent_signals = sum(item["points_per_source"] for item in items)
        if stage in ("merged", "out"):
            equivalent_signals *= 2
        result["stages"][stage] = {
            "role": {"a": "traffic_a", "b": "traffic_b", "sa": "create_a", "sb": "create_b", "merged": "merge", "out": "estimate"}[stage],
            "iterations": len(items),
            "input_unit": unit,
            "input_count": inputs,
            "input_per_second": round(inputs / elapsed, 2) if elapsed else 0,
            "equivalent_signals_per_second": round(equivalent_signals / elapsed, 2) if elapsed else 0,
            "process_seconds": round(elapsed, 6),
            "cpu_seconds": round(sum(item["cpu_jiffies"] for item in items) / ticks, 6),
            "peak_rss_kib": max(item["peak_rss_kib"] for item in items),
            "serialization_seconds": round(sum(item["serialization_nanoseconds"] for item in items) / 1e9, 6),
            "output_bytes": sum(item["output_bytes"] for item in items),
        }
    return result


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as source:
        records = [json.loads(line) for line in source if line.strip()]
    with open(sys.argv[2], "w", encoding="utf-8") as destination:
        json.dump(summarize(records), destination, indent=2)
        destination.write("\n")
