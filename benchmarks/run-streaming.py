#!/usr/bin/env python3
"""Persistent, bounded streaming over standard OTLP/HTTP and isolated Docker networks."""
import argparse
import concurrent.futures
import csv
import http.client
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
PROCESSOR_ROLES = ("branch_a", "branch_b", "merge", "estimate")
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
    def __init__(self, args, scenario, rate, repeat, traffic_rate, window_points):
        self.args, self.scenario, self.rate = args, scenario, rate
        self.generator_core_count, self.traffic_rate, self.window_points = args.generator_cores, traffic_rate, window_points
        self.observation_seconds = max(
            args.duration,
            args.minimum_observed_windows * window_points / traffic_rate,
        )
        self.root = args.output / f"traffic-{traffic_rate}-sps" / f"window-{window_points}" / f"{rate:g}mbit" / scenario / str(repeat)
        self.root.mkdir(parents=True, exist_ok=True)
        self.prefix = "asap-stream-" + uuid.uuid4().hex[:10]
        self.networks, self.containers, self.urls, self.data_ifaces = [], {}, {}, {}
        self.docker = shlex.split(args.docker_command)
        self.samples = []
        self.generators_stopped = False
        self.profiler = None
        self.profile_log = None

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
        # The backend is benchmark infrastructure, so it receives every host CPU
        # left after the isolated generator and processor assignments. Keep at
        # least one CPU available for it. Memory is intentionally uncapped for
        # every role; the harness reports actual RSS instead.
        required = self.generator_core_count + len(PROCESSOR_ROLES) + 1
        if len(cores) < required:
            raise RuntimeError(
                f"dedicated placement needs {required} CPUs: {self.generator_core_count} "
                f"traffic-generator CPUs plus one CPU for each of {len(PROCESSOR_ROLES)} processors "
                "and at least one backend CPU; "
                f"only {len(cores)} are available"
            )
        generators = cores[:self.generator_core_count]
        component_cores = dict(zip(PROCESSOR_ROLES, cores[self.generator_core_count:self.generator_core_count + len(PROCESSOR_ROLES)]))
        backend_cores = cores[self.generator_core_count + len(PROCESSOR_ROLES):]
        for suffix in ("data", "control"):
            network = self.prefix + "-" + suffix
            self.command("network", "create", "--internal", network)
            self.networks.append(network)
        downstream = {"generator_a": "branch_a", "generator_b": "branch_b", "branch_a": "merge", "branch_b": "merge", "merge": "estimate", "estimate": "backend"}
        # All processes are created once and remain alive through warmup, observation, drain.
        for role in reversed(ROLES):
            name = self.prefix + "-" + role
            cpu_set = (generators if role.startswith("generator") else
                       backend_cores if role == "backend" else [component_cores[role]])
            env = {
                "ASAP_ROLE": role.split("_")[0], "ASAP_SCENARIO": self.scenario,
                "ASAP_WINDOW_POINTS": str(self.window_points), "ASAP_BATCH_SIZE": str(self.args.batch_size),
                "ASAP_SOURCE": "1" if role.endswith("_b") else "0",
                "ASAP_ENDPOINT": f"http://{downstream.get(role, 'backend')}:4318",
                "ASAP_BACKEND": "http://backend-control:4319",
                "ASAP_GENERATOR_WORKERS": str(self.generator_core_count),
                "ASAP_SIGNALS_PER_SECOND": str(self.traffic_rate),
                "ASAP_PROFILE_CPU": "1" if self.args.profile_cpu else "0",
            }
            cmd = ["create", "--name", name, "--network", self.networks[0], "--network-alias", role,
                   "--cpuset-cpus", ",".join(map(str, cpu_set))]
            if self.rate and role.startswith("branch"):
                cmd += ["--cap-add", "NET_ADMIN"]
            for key, value in env.items():
                cmd += ["-e", f"{key}={value}"]
            self.command(*cmd, self.args.image)
            self.containers[role] = name
            self.command("network", "connect", "--alias", role + "-control", self.networks[1], name)
            info = json.loads(self.command("inspect", name))[0]
            data_ip = info["NetworkSettings"]["Networks"][self.networks[0]]["IPAddress"]
            control_ip = info["NetworkSettings"]["Networks"][self.networks[1]]["IPAddress"]
            self.urls[role] = f"http://{control_ip}:4319"
            if role.startswith("generator"):
                continue
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
        generators_roles = ("generator_a", "generator_b")
        parallel(lambda role: self.command("start", self.containers[role]), generators_roles)
        for role in generators_roles:
            info = json.loads(self.command("inspect", self.containers[role]))[0]
            data_ip = info["NetworkSettings"]["Networks"][self.networks[0]]["IPAddress"]
            control_ip = info["NetworkSettings"]["Networks"][self.networks[1]]["IPAddress"]
            self.urls[role] = f"http://{control_ip}:4319"
            def generator_ready():
                try:
                    request(self.urls[role])
                    return True
                except (OSError, ValueError):
                    return False
            self.wait_for(generator_ready)
            addresses = json.loads(self.command("exec", self.containers[role], "ip", "-j", "addr"))
            self.data_ifaces[role] = next(x["ifname"] for x in addresses if any(a.get("local") == data_ip for a in x["addr_info"]))
        repo = str(Path(__file__).resolve().parents[1])
        revision = subprocess.check_output(["git", "-C", repo, "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output(["git", "-C", repo, "status", "--porcelain"], text=True).strip())
        cpu_model = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown")
        config = {"repository_head": revision, "working_tree_dirty": dirty, "host": platform.platform(), "cpu_model": cpu_model,
                  "scenario": self.scenario, "branch_egress_mbit_per_second": self.rate,
                  "target_signals_per_second_per_source": self.traffic_rate,
                  "window_points_per_source": self.window_points, "batch_size": self.args.batch_size,
                  "warmup_seconds": self.args.warmup, "observation_seconds": self.observation_seconds,
                  "generator_cores": generators, "component_cores": component_cores,
                  "backend_cores": backend_cores,
                  "measurement_clock": "CLOCK_MONOTONIC, shared host kernel", "max_source_skew_windows": 1024, "pdata_channel_capacity": 8, "exporter_max_in_flight": 1,
                  "transport": "standard OTLP/HTTP protobuf, uncompressed, persistent connections",
                  "data_interfaces": self.data_ifaces,
                  "container_memory_limit": "unlimited",
                  "image": self.args.image, "image_id": json.loads(self.command("image", "inspect", self.args.image))[0]["Id"],
                  "profile_cpu": self.args.profile_cpu, "account_cpu": self.args.account_cpu or self.args.profile_cpu,
                  "perf_command": self.args.perf_command,
                  "traffic_seed": "0x9e3779b97f4a7c15", "kll_k": 400,
                  "generator_strategy": "pre_generated",
                  "input": "same deterministic two-source corpus replayed per count-aligned window"}
        (self.root / "config.json").write_text(json.dumps(config, indent=2) + "\n")

    def start_profile(self):
        if not self.args.perf_command:
            return
        roles = ("branch_a", "branch_b", "merge", "estimate")
        pids = {role: json.loads(self.command("inspect", self.containers[role]))[0]["State"]["Pid"] for role in roles}
        (self.root / "profile-pids.json").write_text(json.dumps(pids, indent=2) + "\n")
        # Host libc can differ from container libc. Keep exact ELF files for DWARF
        # unwinding; resolving host paths produces misleading symbols/stacks.
        image_id = json.loads(self.command("image", "inspect", self.args.image))[0]["Id"].split(":")[-1]
        self.symfs = self.args.output.resolve() / "profile-symbols" / image_id
        (self.root / "perf-symbols.json").write_text(json.dumps({"symfs": str(self.symfs), "image_id": image_id}) + "\n")
        maps = self.command("exec", self.containers["branch_a"], "cat", "/proc/1/maps")
        for line in maps.splitlines():
            fields = line.split()
            if len(fields) < 6 or "x" not in fields[1] or not fields[5].startswith("/"):
                continue
            source = fields[5]
            destination = self.symfs / source.lstrip("/")
            if not destination.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                self.command("cp", self.containers["branch_a"] + ":" + source, str(destination))
        self.profile_log = (self.root / "perf-record.log").open("w")
        version = subprocess.check_output(shlex.split(self.args.perf_command) + ["--version"], text=True)
        (self.root / "perf-version.txt").write_text(version)
        command = shlex.split(self.args.perf_command) + ["record", "-e", "cpu-clock", "-F", "199",
                  "--call-graph", "dwarf,16384", "-p", ",".join(map(str, pids.values())),
                  "-o", str(self.root / "perf.data"), "--", "sleep", str(self.observation_seconds)]
        (self.root / "perf-command.json").write_text(json.dumps(command) + "\n")
        self.profiler = subprocess.Popen(command, stdout=self.profile_log, stderr=self.profile_log)

    def finish_profile(self):
        if self.profiler is None:
            return
        code = self.profiler.wait(timeout=self.observation_seconds + 15)
        self.profiler = None
        self.profile_log.close()
        if code:
            raise RuntimeError(f"perf record failed; see {self.root / 'perf-record.log'}")
    def decode_profile(self):
        # Decode after container removal: while a target is alive, perf may enter
        # its mount namespace where the host's saved symfs tree is inaccessible.
        with (self.root / "perf-stacks.txt").open("w") as output, (self.root / "perf-script.log").open("w") as error:
            subprocess.run(shlex.split(self.args.perf_command) + ["script", "-i", str(self.root / "perf.data"),
                           "--symfs", str(self.symfs), "--no-inline", "-F", "pid,ip,sym,dso"],
                           stdout=output, stderr=error, check=True, timeout=120)
        if "(/usr/local/bin/asap-stream-bench)" not in (self.root / "perf-stacks.txt").read_text():
            raise RuntimeError("perf produced no resolved worker frames; use a recent perf (tested 6.8) and inspect perf-script.log")

    def drain(self):
        gens = ("generator_a", "generator_b")
        generator_snapshots = {role: request(self.urls[role]) for role in gens}
        parallel(lambda role: self.command("stop", "-t", "5", self.containers[role], check=False), gens)
        self.generators_stopped = True
        previous = None
        stable = 0
        deadline = time.monotonic() + 60
        result = None
        while time.monotonic() < deadline:
            current = {role: request(self.urls[role]) for role in ROLES if role not in gens}
            counts = (current["branch_a"]["stats"]["received_signals"],
                      current["branch_b"]["stats"]["received_signals"])
            stable = stable + 1 if counts == previous else 0
            previous = counts
            target = min(count // self.window_points for count in counts)
            if stable >= 2 and current["backend"]["stats"]["completed_windows"] == target:
                result = {**generator_snapshots, **current}
                break
            time.sleep(0.2)
        if result is None:
            raise TimeoutError("official traffic generator drain timeout")
        return result

    def observe(self):
        for g in ("generator_a", "generator_b"):
            request(self.urls[g], "/run", "POST")
        warmup_end = time.monotonic() + self.args.warmup
        while time.monotonic() < warmup_end:
            self.healthy()
            time.sleep(min(0.5, max(0, warmup_end - time.monotonic())))
        # CPU accounting needs matching input/output boundaries: quiesce warmup,
        # then include the final drain in CPU cost, but not steady goodput.
        accounted = self.args.profile_cpu or self.args.account_cpu
        cpu_before = self.statuses() if accounted else None
        # Ordinary throughput runs continue without a warmup drain.
        self.start_profile()
        before = self.statuses()
        deadline = time.monotonic() + self.observation_seconds
        while time.monotonic() < deadline:
            time.sleep(min(self.args.sample_interval, max(0, deadline - time.monotonic())))
            self.healthy()
            self.samples.append(self.statuses())
        after = self.samples[-1]
        drained = self.drain()
        self.finish_profile()
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
            if self.args.profile_cpu:
                scopes = {key: (value - start["profile_cpu_ns"][key]) / 1e9 for key, value in end["profile_cpu_ns"].items()}
                resources[role]["scoped_cpu_seconds"] = scopes
                # This includes OTAP/runtime/transport and unscoped harness work, not pure framework CPU.
                resources[role]["outside_scopes_cpu_seconds"] = cpu - sum(scopes.values())
        def backlog(s):
            ingested = sum(s[g]["stats"]["received_signals"] for g in ("branch_a", "branch_b"))
            completed = s["backend"]["stats"]["completed_windows"] * self.window_points * 2
            return max(0, ingested - completed)
        backlog_start, backlog_end = backlog(before), backlog(after)
        offered = self.traffic_rate * 2
        report = {"scenario": self.scenario, "branch_egress_mbit_per_second": self.rate,
                  "target_signals_per_second_per_source": self.traffic_rate,
                  "observation_seconds": seconds, "completed_windows": windows,
                  "generator_core_count": self.generator_core_count,
                  "window_points_per_source": self.window_points,
                  "completed_input_signals": windows * self.window_points * 2,
                  "signals_per_second": windows * self.window_points * 2 / seconds,
                  "offered_signals_per_second": offered,
                  "delivery_ratio": windows * self.window_points * 2 / seconds / offered,
                  "delivery_ratio_with_window_tolerance": min(
                      1.0, (windows + 1) * self.window_points * 2 / seconds / offered),
                  "window_latency_p50_ms": percentile(histogram, 0.5), "window_latency_p99_ms": percentile(histogram, 0.99),
                  "inflight_signals_at_start": backlog_start, "inflight_signals_at_end": backlog_end,
                  "backlog_growth_signals": backlog_end - backlog_start,
                  "backlog_growth_windows": (backlog_end - backlog_start) / (self.window_points * 2),
                  "ingested_signals_after_stop": sum(drained[g]["stats"]["received_signals"] for g in ("branch_a", "branch_b")),
                  "unpaired_tail_signals_after_stop": sum(
                      drained[g]["stats"]["received_signals"] for g in ("branch_a", "branch_b"))
                      - drained["backend"]["stats"]["completed_windows"] * self.window_points * 2,
                  "generated_signals_after_drain": drained["backend"]["stats"]["completed_windows"] * self.window_points * 2,
                  "validated_signals_after_drain": drained["backend"]["stats"]["completed_windows"] * self.window_points * 2,
                  "correctness": "passed", "resources": resources}
        if accounted:
            inputs = drained["backend"]["stats"]["received_signals"] - cpu_before["backend"]["stats"]["received_signals"]
            cpu_profile = {}
            for role in ROLES:
                start, end = cpu_before[role], drained[role]
                cpu = (end["cpu_nanoseconds"] - start["cpu_nanoseconds"]) / 1e9
                scopes = {key: (value - start["profile_cpu_ns"][key]) / 1e9 for key, value in end["profile_cpu_ns"].items()}
                cpu_profile[role] = {"cpu_seconds": cpu, "scoped_cpu_seconds": scopes,
                                     "outside_scopes_cpu_seconds": cpu - sum(scopes.values())}
            report["cpu_profile"] = {"completed_input_signals": inputs, "roles": cpu_profile,
                                     "boundary": "post-warmup snapshot to final drain, includes in-flight start and tail CPU"}
            (self.root / "cpu-before.json").write_text(json.dumps(cpu_before, indent=2) + "\n")
        (self.root / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        return report

    def close(self):
        if self.profiler is not None:
            try:
                self.finish_profile()
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                (self.root / "profile-cleanup-error.txt").write_text(str(error) + "\n")
        for role, name in self.containers.items():
            try:
                request(self.urls[role], "/shutdown", "POST")
            except (OSError, ValueError, KeyError, http.client.HTTPException):
                pass
        for role, name in self.containers.items():
            (self.root / f"{role}-container.json").write_text(self.command("inspect", name, check=False) + "\n")
            if not (self.generators_stopped and role.startswith("generator")):
                self.command("stop", "-t", "8", name, check=False)
            logs = subprocess.run(self.docker + ["logs", name], capture_output=True, text=True)
            (self.root / f"{role}.log").write_text(logs.stdout + logs.stderr)
            self.command("rm", "-f", name, check=False)
        for network in reversed(self.networks):
            self.command("network", "rm", network, check=False)
        if hasattr(self, "symfs") and (self.root / "perf.data").exists():
            self.decode_profile()


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
        cap = "unlimited" if rate == 0 else f"{rate:g} Mbit/s"
        label = f"traffic={row['target_signals_per_second_per_source']:,}/s/source, w={row['window_points_per_source']:,}, {cap}"
        width = row["median_signals_per_second"] / maximum * 540
        svg += [f'<text x="25" y="{y+21}">{row["scenario"]} / {label}</text>',
                f'<rect x="250" y="{y}" width="{width:.1f}" height="30" fill="{colors[row["scenario"]]}"/>',
                f'<text x="{260+width:.1f}" y="{y+21}">{row["median_signals_per_second"]:,.0f}</text>']
    svg.append('</svg>')
    path.write_text("\n".join(svg) + "\n")


def draw_resources(rows, path):
    roles = list(ROLES)
    width = 1180
    panel_height = 245
    height = 70 + len(rows) * panel_height
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>text{font:13px sans-serif;fill:#172033}.title{font:bold 16px sans-serif}</style>',
           '<text class="title" x="25" y="28">Per-component CPU and sampled peak RSS</text>',
           '<text x="25" y="50">CPU is average cores used during observation; RSS is median sampled peak across repetitions.</text>']
    for index, row in enumerate(rows):
        y0 = 70 + index * panel_height
        label = (f"{row['scenario']} · {row['target_signals_per_second_per_source']:,} signals/s/source · "
                 f"window {row['window_points_per_source']:,}")
        svg.append(f'<text class="title" x="25" y="{y0 + 18}">{label}</text>')
        maximum_rss = max(component["median_sampled_peak_rss_mib"] for component in row["components"])
        for role_index, component in enumerate(row["components"]):
            y = y0 + 34 + role_index * 27
            cpu_width = min(component["median_cpu_cores_used"], 1.0) * 300
            rss_width = component["median_sampled_peak_rss_mib"] / maximum_rss * 300 if maximum_rss else 0
            svg += [f'<text x="25" y="{y + 15}">{component["component"]}</text>',
                    f'<rect x="135" y="{y}" width="{cpu_width:.1f}" height="18" fill="#2563eb"/>',
                    f'<text x="{145 + cpu_width:.1f}" y="{y + 14}">{component["median_cpu_cores_used"]:.2f} CPU</text>',
                    f'<rect x="650" y="{y}" width="{rss_width:.1f}" height="18" fill="#16a34a"/>',
                    f'<text x="{660 + rss_width:.1f}" y="{y + 14}">{component["median_sampled_peak_rss_mib"]:.1f} MiB</text>']
    svg.append('</svg>')
    path.write_text("\n".join(svg) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="asap-stream-benchmark:local")
    parser.add_argument("--docker-command", default="docker", help="e.g. 'sudo -n docker' when required locally")
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/streaming"))
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--rates-mbit", type=float, nargs="+", default=[0], help="optional per-branch egress caps; 0 = unlimited")
    parser.add_argument("--window-points", type=int, nargs="+", default=[16384, 65536, 262144],
                        help="aggregation window sizes per source")
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--minimum-observed-windows", type=float, default=3,
                        help="extend observation time to cover at least this many windows at the offered rate")
    parser.add_argument("--sample-interval", type=float, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--traffic-rates", type=int, nargs="+", default=[10000, 25000, 50000, 100000, 200000, 300000, 400000],
                        help="target signals/s per source to sweep using OTAP's traffic_generator receiver")
    parser.add_argument("--generator-cores", type=int, default=4,
                        help="OTAP traffic_generator pipeline workers per source; each measured processor gets one dedicated CPU and the backend gets all remaining CPUs")
    parser.add_argument("--profile-cpu", action="store_true", help="exclusive synchronous processor thread CPU scopes")
    parser.add_argument("--account-cpu", action="store_true", help="match CPU/input boundaries with warmup and final drains; implied by --profile-cpu")
    parser.add_argument("--perf-command", default="", help="optional host sampler, e.g. 'sudo -n perf'; records four worker processes")
    parser.add_argument("--sustainable-min-delivery", type=float, default=0.95)
    parser.add_argument("--sustainable-max-backlog-growth-windows", type=float, default=1.0)
    parser.add_argument("--sustainable-max-p99-ms", type=float, default=5000)
    parser.add_argument("--resume", action="store_true", help="reuse completed runs from output/runs.json")
    args = parser.parse_args()
    if any(points < 1 for points in args.window_points) or args.batch_size < 1 or any(args.batch_size > points for points in args.window_points) or args.generator_cores < 1 or any(rate < 1 for rate in args.traffic_rates) or not all(math.isfinite(t) for t in (args.warmup, args.duration, args.sample_interval)) or args.warmup < 0 or args.duration <= 0 or args.minimum_observed_windows < 2 or args.sample_interval <= 0 or args.repetitions < 1 or any(not math.isfinite(r) or r < 0 for r in args.rates_mbit) or not 0 < args.sustainable_min_delivery <= 1 or args.sustainable_max_backlog_growth_windows < 0 or args.sustainable_max_p99_ms <= 0:
        parser.error("invalid benchmark sizes, times, repetitions, or rates")
    args.output.mkdir(parents=True, exist_ok=True)
    runs_path = args.output / "runs.json"
    results = json.loads(runs_path.read_text()) if args.resume and runs_path.exists() else []
    completed_keys = {(r["target_signals_per_second_per_source"], r["window_points_per_source"],
                       r["branch_egress_mbit_per_second"], r["scenario"], r["repeat"])
                      for r in results}
    for traffic_rate in args.traffic_rates:
        for window_points in args.window_points:
            for rate in args.rates_mbit:
                for repeat in range(1, args.repetitions + 1):
                    # Rotate scenario order to reduce systematic first/last-run bias.
                    scenarios = args.scenarios[repeat % len(args.scenarios):] + args.scenarios[:repeat % len(args.scenarios)]
                    for scenario in scenarios:
                        key = (traffic_rate, window_points, rate, scenario, repeat)
                        if key in completed_keys:
                            print(f"resume: skipping completed traffic={traffic_rate} w={window_points} {scenario} repeat={repeat}", flush=True)
                            continue
                        run = Run(args, scenario, rate, repeat, traffic_rate, window_points)
                        try:
                            run.setup()
                            report = run.observe()
                            report["repeat"] = repeat
                            results.append(report)
                            runs_path.write_text(json.dumps(results, indent=2) + "\n")
                            print(f"traffic={traffic_rate}/s/source w={window_points} {scenario} cap={rate:g} Mbit/s repeat={repeat}: {report['signals_per_second']:,.0f} signals/s, p99={report['window_latency_p99_ms']} ms, loss=0", flush=True)
                        finally:
                            run.close()
    rows = []
    for traffic_rate in args.traffic_rates:
        for window_points in args.window_points:
            for rate in args.rates_mbit:
                for scenario in args.scenarios:
                    samples = [r for r in results if r["scenario"] == scenario and r["branch_egress_mbit_per_second"] == rate and r["target_signals_per_second_per_source"] == traffic_rate and r["window_points_per_source"] == window_points]
                    rows.append({"scenario": scenario, "target_signals_per_second_per_source": traffic_rate, "generator_core_count": args.generator_cores, "window_points_per_source": window_points, "branch_egress_mbit_per_second": rate, "repetitions": len(samples),
                                 "median_signals_per_second": statistics.median(s["signals_per_second"] for s in samples),
                                 "median_delivery_ratio": statistics.median(s["delivery_ratio"] for s in samples),
                                 "median_delivery_ratio_with_window_tolerance": statistics.median(s["delivery_ratio_with_window_tolerance"] for s in samples),
                                 "median_backlog_growth_windows": statistics.median(s["backlog_growth_windows"] for s in samples),
                                 "min_signals_per_second": min(s["signals_per_second"] for s in samples),
                                 "max_signals_per_second": max(s["signals_per_second"] for s in samples),
                                 "median_window_latency_p99_ms": statistics.median(s["window_latency_p99_ms"] for s in samples)})
                    latency_limit_ms = max(
                        args.sustainable_max_p99_ms,
                        2000 * window_points / traffic_rate,
                    )
                    rows[-1]["sustainable_p99_limit_ms"] = latency_limit_ms
                    rows[-1]["sustainable"] = (
                        statistics.median(s["delivery_ratio_with_window_tolerance"] for s in samples) >= args.sustainable_min_delivery
                        and rows[-1]["median_backlog_growth_windows"] <= args.sustainable_max_backlog_growth_windows
                        and rows[-1]["median_window_latency_p99_ms"] <= latency_limit_ms
                        and all(s["correctness"] == "passed" for s in samples))
    by_key = {(row["target_signals_per_second_per_source"], row["window_points_per_source"], row["branch_egress_mbit_per_second"], row["scenario"]): row for row in rows}
    for row in rows:
        for baseline in ("raw", "exact"):
            other = by_key.get((row["target_signals_per_second_per_source"], row["window_points_per_source"], row["branch_egress_mbit_per_second"], baseline))
            row[f"kll_over_{baseline}"] = row["median_signals_per_second"] / other["median_signals_per_second"] if row["scenario"] == "kll" and other else ""
    resources = []
    for row in rows:
        samples = [r for r in results if r["scenario"] == row["scenario"] and r["target_signals_per_second_per_source"] == row["target_signals_per_second_per_source"] and r["window_points_per_source"] == row["window_points_per_source"] and r["branch_egress_mbit_per_second"] == row["branch_egress_mbit_per_second"]]
        for role in ROLES:
            resource = {"scenario": row["scenario"], "target_signals_per_second_per_source": row["target_signals_per_second_per_source"], "generator_core_count": row["generator_core_count"], "window_points_per_source": row["window_points_per_source"], "branch_egress_mbit_per_second": row["branch_egress_mbit_per_second"], "component": role, "repetitions": len(samples)}
            for field in ("cpu_cores_used", "sampled_peak_rss_mib", "data_rx_mbit_s", "data_tx_mbit_s"):
                resource["median_" + field] = statistics.median(sample["resources"][role][field] for sample in samples)
            resources.append(resource)
    (args.output / "resources.json").write_text(json.dumps(resources, indent=2) + "\n")
    with (args.output / "resources.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(resources[0]))
        writer.writeheader()
        writer.writerows(resources)
    draw_summary(rows, args.output / "throughput.svg")
    resource_panels = []
    for row in rows:
        matching = [resource for resource in resources
                    if all(resource[key] == row[key] for key in ("scenario", "target_signals_per_second_per_source", "window_points_per_source", "branch_egress_mbit_per_second"))]
        resource_panels.append({**row, "components": matching})
    draw_resources(resource_panels, args.output / "resources.svg")
    capacities = []
    for window_points in args.window_points:
        for rate in args.rates_mbit:
            capacity = {"window_points_per_source": window_points,
                        "branch_egress_mbit_per_second": rate}
            for scenario in args.scenarios:
                candidates = [row for row in rows if row["scenario"] == scenario
                              and row["window_points_per_source"] == window_points
                              and row["branch_egress_mbit_per_second"] == rate
                              and row["sustainable"]]
                best = max(candidates, key=lambda row: row["median_signals_per_second"], default=None)
                capacity[f"{scenario}_maximum_sustainable_signals_per_second"] = (
                    best["median_signals_per_second"] if best else None)
                capacity[f"{scenario}_maximum_sustainable_offered_per_source"] = (
                    best["target_signals_per_second_per_source"] if best else None)
            exact = capacity.get("exact_maximum_sustainable_signals_per_second")
            kll = capacity.get("kll_maximum_sustainable_signals_per_second")
            capacity["kll_over_exact_capacity_speedup"] = kll / exact if exact and kll else None
            capacities.append(capacity)
    (args.output / "capacity.json").write_text(json.dumps(capacities, indent=2) + "\n")
    with (args.output / "capacity.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(capacities[0]))
        writer.writeheader()
        writer.writerows(capacities)
    (args.output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    with (args.output / "summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
