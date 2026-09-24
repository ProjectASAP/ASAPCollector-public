#!/usr/bin/env python3
"""Persistent, bounded streaming over standard OTLP/HTTP and isolated Docker networks."""
import argparse
import concurrent.futures
import csv
import json
import math
import os
import platform
from pathlib import Path
import shlex
import socket
import statistics
import subprocess
import time
import urllib.request
import uuid

ROLES = ("generator_a", "generator_b", "branch_a", "branch_b", "merge", "estimate", "backend")
WORKER_ROLES = ("branch_a", "branch_b", "merge", "estimate", "backend")
SCENARIOS = ("raw", "exact", "kll")
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(url, path="/stats", method="GET"):
    req = urllib.request.Request(url + path, method=method, data=b"" if method == "POST" else None)
    with HTTP.open(req, timeout=5) as response:
        return json.load(response)


def parallel(fn, items):
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool:
        return dict(zip(items, pool.map(fn, items)))


def percentile(histogram, fraction):
    total = sum(histogram.values())
    target = math.ceil(total * fraction)
    seen = 0
    for bucket, count in sorted(histogram.items()):
        seen += count
        if seen >= target:
            return bucket
    return None


def delta_histogram(before, after):
    return {int(k): v - before.get(k, 0) for k, v in after.items() if v > before.get(k, 0)}


class Run:
    def __init__(self, args, scenario, rate, repeat):
        self.args, self.scenario, self.rate = args, scenario, rate
        self.root = args.output / f"{rate:g}mbit" / scenario / str(repeat)
        self.root.mkdir(parents=True, exist_ok=True)
        self.prefix = "asap-stream-" + uuid.uuid4().hex[:10]
        self.networks, self.containers, self.urls, self.data_ifaces = [], {}, {}, {}
        self.docker = shlex.split(args.docker_command)
        self.samples = []

    def command(self, *args, check=True):
        result = subprocess.run(self.docker + list(args), capture_output=True, text=True)
        if check and result.returncode:
            raise RuntimeError(f"docker {args[0]} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def statuses(self):
        return parallel(lambda role: request(self.urls[role]), ROLES)

    def healthy(self):
        info = json.loads(self.command("inspect", *self.containers.values()))
        for container in info:
            if not container["State"]["Running"]:
                raise RuntimeError(f"container failed: {container['Name']}: {container['State']}")

    def wait_for(self, predicate, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.healthy()
            if predicate():
                return
            time.sleep(0.1)
        raise TimeoutError("streaming benchmark readiness/drain timeout")

    def setup(self):
        cores = sorted(os.sched_getaffinity(0))
        required = self.args.generator_cores + len(WORKER_ROLES)
        if len(cores) < required:
            raise RuntimeError(
                f"dedicated placement needs {required} CPUs: {self.args.generator_cores} "
                f"traffic-generator CPUs plus one CPU for each of {len(WORKER_ROLES)} components; "
                f"only {len(cores)} are available"
            )
        generators = cores[:self.args.generator_cores]
        component_cores = dict(zip(WORKER_ROLES, cores[self.args.generator_cores:required]))
        for suffix in ("data", "control"):
            network = self.prefix + "-" + suffix
            self.command("network", "create", "--internal", network)
            self.networks.append(network)
        downstream = {"generator_a": "branch_a", "generator_b": "branch_b", "branch_a": "merge", "branch_b": "merge", "merge": "estimate", "estimate": "backend"}
        # All processes are created once and remain alive through warmup, observation, drain.
        for role in reversed(ROLES):
            name = self.prefix + "-" + role
            cpu_set = generators if role.startswith("generator") else [component_cores[role]]
            env = {
                "ASAP_ROLE": role.split("_")[0], "ASAP_SCENARIO": self.scenario,
                "ASAP_WINDOW_POINTS": str(self.args.window_points), "ASAP_BATCH_SIZE": str(self.args.batch_size),
                "ASAP_SOURCE": "1" if role.endswith("_b") else "0",
                "ASAP_ENDPOINT": f"http://{downstream.get(role, 'backend')}:4318",
                "ASAP_BACKEND": "http://backend-control:4319",
            }
            cmd = ["create", "--name", name, "--network", self.networks[0], "--network-alias", role,
                   "--cpuset-cpus", ",".join(map(str, cpu_set)), "--memory", self.args.memory,
                   "--memory-swap", self.args.memory]
            if self.rate and role.startswith("branch"):
                cmd += ["--cap-add", "NET_ADMIN"]
            for key, value in env.items():
                cmd += ["-e", f"{key}={value}"]
            self.command(*cmd, self.args.image)
            self.containers[role] = name
            self.command("network", "connect", "--alias", role + "-control", self.networks[1], name)
            self.command("start", name)
            info = json.loads(self.command("inspect", name))[0]
            data_ip = info["NetworkSettings"]["Networks"][self.networks[0]]["IPAddress"]
            control_ip = info["NetworkSettings"]["Networks"][self.networks[1]]["IPAddress"]
            self.urls[role] = f"http://{control_ip}:4319"
            addresses = json.loads(self.command("exec", name, "ip", "-j", "addr"))
            self.data_ifaces[role] = next(x["ifname"] for x in addresses if any(a.get("local") == data_ip for a in x["addr_info"]))
            if self.rate and role.startswith("branch"):
                # Model a constrained edge-to-aggregator link; ingress and control are unaffected.
                self.command("exec", name, "tc", "qdisc", "replace", "dev", self.data_ifaces[role],
                             "root", "tbf", "rate", f"{self.rate:g}mbit", "burst", "32kb", "latency", "100ms")
            def ready():
                try:
                    request(self.urls[role])
                    if not role.startswith("generator"):
                        with socket.create_connection((data_ip, 4318), timeout=1):
                            pass
                    return True
                except (OSError, ValueError):
                    return False
            self.wait_for(ready)
        repo = str(Path(__file__).resolve().parents[1])
        revision = subprocess.check_output(["git", "-C", repo, "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output(["git", "-C", repo, "status", "--porcelain"], text=True).strip())
        cpu_model = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown")
        config = {"repository_head": revision, "working_tree_dirty": dirty, "host": platform.platform(), "cpu_model": cpu_model,
                  "scenario": self.scenario, "branch_egress_mbit_per_second": self.rate,
                  "window_points_per_source": self.args.window_points, "batch_size": self.args.batch_size,
                  "warmup_seconds": self.args.warmup, "observation_seconds": self.args.duration,
                  "generator_cores": generators, "component_cores": component_cores,
                  "measurement_clock": "CLOCK_MONOTONIC, shared host kernel", "max_inflight_windows": 4, "pdata_channel_capacity": 8, "exporter_max_in_flight": 1,
                  "transport": "standard OTLP/HTTP protobuf, uncompressed, persistent connections",
                  "data_interfaces": self.data_ifaces, "container_memory_limit": self.args.memory,
                  "image": self.args.image, "image_id": json.loads(self.command("image", "inspect", self.args.image))[0]["Id"],
                  "traffic_seed": "0x9e3779b97f4a7c15", "kll_k": 400,
                  "input": "same deterministic two-source corpus replayed per count-aligned window"}
        (self.root / "config.json").write_text(json.dumps(config, indent=2) + "\n")

    def drain(self):
        gens = ("generator_a", "generator_b")
        parallel(lambda role: request(self.urls[role], "/pause", "POST"), gens)
        self.wait_for(lambda: all(request(self.urls[g])["stats"]["generator_paused"] for g in gens))
        paused = {g: request(self.urls[g])["stats"] for g in gens}
        target = max(s["sent_windows"] for s in paused.values())
        # A source may be ahead by a window. Complete its partner before draining.
        parallel(lambda role: request(self.urls[role], f"/finish/{target}", "POST"), gens)
        self.wait_for(lambda: all(request(self.urls[g])["stats"]["generator_paused"] and
                                 request(self.urls[g])["stats"]["sent_windows"] == target for g in gens)
                      and request(self.urls["backend"])["stats"]["completed_windows"] == target)
        result = self.statuses()
        sent = sum(result[g]["stats"]["sent_signals"] for g in gens)
        received = result["backend"]["stats"]["received_signals"]
        if sent != received or sent != target * self.args.window_points * 2:
            raise RuntimeError(f"data loss/duplication: generated={sent}, validated={received}, windows={target}")
        return result

    def observe(self):
        for g in ("generator_a", "generator_b"):
            request(self.urls[g], "/run", "POST")
        warmup_end = time.monotonic() + self.args.warmup
        while time.monotonic() < warmup_end:
            self.healthy()
            time.sleep(min(0.5, max(0, warmup_end - time.monotonic())))
        # Continue running across this boundary: no pipeline restart or warmup drain.
        before = self.statuses()
        deadline = time.monotonic() + self.args.duration
        while time.monotonic() < deadline:
            time.sleep(min(self.args.sample_interval, max(0, deadline - time.monotonic())))
            self.healthy()
            self.samples.append(self.statuses())
        after = self.samples[-1]
        drained = self.drain()
        (self.root / "snapshots.json").write_text(json.dumps({"before": before, "samples": self.samples, "drained": drained}, indent=2) + "\n")
        b, a = before["backend"], after["backend"]
        seconds = (a["timestamp_ns"] - b["timestamp_ns"]) / 1e9
        # Window completions give the same unit for raw and both quantile scenarios.
        windows = a["stats"]["completed_windows"] - b["stats"]["completed_windows"]
        if windows < 2:
            raise RuntimeError("fewer than two completed observation windows; increase duration or reduce window size")
        histogram = delta_histogram(b["stats"]["window_latency_ms"], a["stats"]["window_latency_ms"])
        resources = {}
        for role in ROLES:
            start, end = before[role], after[role]
            elapsed = (end["timestamp_ns"] - start["timestamp_ns"]) / 1e9
            iface = self.data_ifaces[role]
            rx = end["interfaces"][iface]["rx_bytes"] - start["interfaces"][iface]["rx_bytes"]
            tx = end["interfaces"][iface]["tx_bytes"] - start["interfaces"][iface]["tx_bytes"]
            cpu = (end["cpu_nanoseconds"] - start["cpu_nanoseconds"]) / 1e9
            resources[role] = {"elapsed_seconds": elapsed, "cpu_seconds": cpu, "cpu_cores_used": cpu / elapsed,
                               "sampled_peak_rss_mib": max(s[role]["rss_kib"] for s in [before] + self.samples) / 1024,
                               "lifetime_peak_rss_mib": end["process_peak_rss_kib"] / 1024,
                               "data_rx_bytes": rx, "data_tx_bytes": tx, "data_rx_mbit_s": rx * 8 / elapsed / 1e6,
                               "data_tx_mbit_s": tx * 8 / elapsed / 1e6}
        def backlog(s):
            sent = sum(s[g]["stats"]["sent_signals"] for g in ("generator_a", "generator_b"))
            return max(0, sent - s["backend"]["stats"]["received_signals"])
        report = {"scenario": self.scenario, "branch_egress_mbit_per_second": self.rate,
                  "observation_seconds": seconds, "completed_windows": windows,
                  "completed_input_signals": windows * self.args.window_points * 2,
                  "signals_per_second": windows * self.args.window_points * 2 / seconds,
                  "window_latency_p50_ms": percentile(histogram, 0.5), "window_latency_p99_ms": percentile(histogram, 0.99),
                  "inflight_signals_at_start": backlog(before), "inflight_signals_at_end": backlog(after),
                  "generated_signals_after_drain": sum(drained[g]["stats"]["sent_signals"] for g in ("generator_a", "generator_b")),
                  "validated_signals_after_drain": drained["backend"]["stats"]["received_signals"],
                  "correctness": "passed", "resources": resources}
        (self.root / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        return report

    def close(self):
        for role, name in self.containers.items():
            try:
                request(self.urls[role], "/shutdown", "POST")
            except (OSError, ValueError, KeyError):
                pass
        for role, name in self.containers.items():
            (self.root / f"{role}-container.json").write_text(self.command("inspect", name, check=False) + "\n")
            self.command("stop", "-t", "8", name, check=False)
            logs = subprocess.run(self.docker + ["logs", name], capture_output=True, text=True)
            (self.root / f"{role}.log").write_text(logs.stdout + logs.stderr)
            self.command("rm", "-f", name, check=False)
        for network in reversed(self.networks):
            self.command("network", "rm", network, check=False)


def draw_summary(rows, path):
    colors = {"raw": "#64748b", "exact": "#dc2626", "kll": "#2563eb"}
    maximum = max(r["median_signals_per_second"] for r in rows)
    height = 100 + len(rows) * 55
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="{height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>text{font:15px sans-serif;fill:#172033}</style>',
           '<text x="25" y="28">Sustained OTLP/HTTP: backend-validated input signals/s</text>',
           '<text x="25" y="52">Median of repeated runs; branch egress cap per source; separate data/control networks</text>']
    for i, row in enumerate(rows):
        y = 78 + i * 55
        rate = row["branch_egress_mbit_per_second"]
        label = "unlimited" if rate == 0 else f"{rate:g} Mbit/s"
        width = row["median_signals_per_second"] / maximum * 540
        svg += [f'<text x="25" y="{y+21}">{row["scenario"]} / {label}</text>',
                f'<rect x="250" y="{y}" width="{width:.1f}" height="30" fill="{colors[row["scenario"]]}"/>',
                f'<text x="{260+width:.1f}" y="{y+21}">{row["median_signals_per_second"]:,.0f}</text>']
    svg.append('</svg>')
    path.write_text("\n".join(svg) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="asap-stream-benchmark:local")
    parser.add_argument("--docker-command", default="docker", help="e.g. 'sudo -n docker' when required locally")
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/streaming"))
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--rates-mbit", type=float, nargs="+", default=[0, 100], help="per-branch egress caps; 0 = unlimited")
    parser.add_argument("--window-points", type=int, default=65536)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--memory", default="1g")
    parser.add_argument("--generator-cores", type=int, default=2,
                        help="shared CPU pool size for the two traffic generators; every downstream component always gets one dedicated CPU")
    args = parser.parse_args()
    if args.window_points < 1 or not 1 <= args.batch_size <= args.window_points or args.generator_cores < 1 or not all(math.isfinite(t) for t in (args.warmup, args.duration, args.sample_interval)) or args.warmup < 0 or args.duration <= 0 or args.sample_interval <= 0 or args.repetitions < 1 or any(not math.isfinite(r) or r < 0 for r in args.rates_mbit):
        parser.error("invalid benchmark sizes, times, repetitions, or rates")
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    for rate in args.rates_mbit:
        for repeat in range(1, args.repetitions + 1):
            # Rotate scenario order to reduce systematic first/last-run bias.
            scenarios = args.scenarios[repeat % len(args.scenarios):] + args.scenarios[:repeat % len(args.scenarios)]
            for scenario in scenarios:
                run = Run(args, scenario, rate, repeat)
                try:
                    run.setup()
                    report = run.observe()
                    report["repeat"] = repeat
                    results.append(report)
                    (args.output / "runs.json").write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{scenario} cap={rate:g} Mbit/s repeat={repeat}: {report['signals_per_second']:,.0f} signals/s, p99={report['window_latency_p99_ms']} ms, loss=0", flush=True)
                finally:
                    run.close()
    rows = []
    for rate in args.rates_mbit:
        for scenario in args.scenarios:
            samples = [r for r in results if r["scenario"] == scenario and r["branch_egress_mbit_per_second"] == rate]
            rows.append({"scenario": scenario, "branch_egress_mbit_per_second": rate, "repetitions": len(samples),
                         "median_signals_per_second": statistics.median(s["signals_per_second"] for s in samples),
                         "min_signals_per_second": min(s["signals_per_second"] for s in samples),
                         "max_signals_per_second": max(s["signals_per_second"] for s in samples),
                         "median_window_latency_p99_ms": statistics.median(s["window_latency_p99_ms"] for s in samples)})
    by_key = {(row["branch_egress_mbit_per_second"], row["scenario"]): row for row in rows}
    for row in rows:
        for baseline in ("raw", "exact"):
            other = by_key.get((row["branch_egress_mbit_per_second"], baseline))
            row[f"kll_over_{baseline}"] = row["median_signals_per_second"] / other["median_signals_per_second"] if row["scenario"] == "kll" and other else ""
    resources = []
    for row in rows:
        samples = [r for r in results if r["scenario"] == row["scenario"] and r["branch_egress_mbit_per_second"] == row["branch_egress_mbit_per_second"]]
        for role in ROLES:
            resource = {"scenario": row["scenario"], "branch_egress_mbit_per_second": row["branch_egress_mbit_per_second"], "component": role, "repetitions": len(samples)}
            for field in ("cpu_cores_used", "sampled_peak_rss_mib", "data_rx_mbit_s", "data_tx_mbit_s"):
                resource["median_" + field] = statistics.median(sample["resources"][role][field] for sample in samples)
            resources.append(resource)
    (args.output / "resources.json").write_text(json.dumps(resources, indent=2) + "\n")
    with (args.output / "resources.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(resources[0]))
        writer.writeheader()
        writer.writerows(resources)
    draw_summary(rows, args.output / "throughput.svg")
    (args.output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    with (args.output / "summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
