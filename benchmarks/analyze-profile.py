#!/usr/bin/env python3
"""Summarize drained CPU scopes and optional perf samples, without equating residual with OTAP."""
import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import re
import statistics
import subprocess

WORKERS = ('branch_a', 'branch_b', 'merge', 'estimate')
CATEGORIES = ('computation', 'pdata_codec', 'sketch_codec', 'processor_bookkeeping', 'outside_scopes')


def write_csv(path, rows):
    if not rows:
        return
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def samples(path):
    """perf script -F pid,ip,sym,dso; frames are leaf first. Preserve missing stacks."""
    pid, frames = None, []
    for line in path.read_text().splitlines() + ['']:
        if not line.strip():
            if pid is not None:
                yield pid, frames
            pid, frames = None, []
        elif line.strip().isdigit():
            pid = int(line.strip())
        else:
            match = re.match(r'\s*[0-9a-f]+ (.*) \((.*)\)$', line)
            if match:
                frames.append(match.groups())


def demangle_names(names):
    mangled = sorted(n for n in names if n.startswith('_R') or n.startswith('_ZN'))
    if not mangled:
        return {}
    # Older perf cannot demangle Rust v0, especially LLVM clone suffixes.
    clean = [re.sub(r'\.llvm\.\d+$', '', n) for n in mangled]
    result = subprocess.check_output(['c++filt', '-s', 'rust'], input='\n'.join(clean) + '\n', text=True)
    return dict(zip(mangled, [re.sub(r'\[[0-9a-f]+\]', '', n) for n in result.splitlines()]))


def family(frames):
    if not frames:
        return 'unresolved'
    if frames[0][1] == '[kernel.kallsyms]':
        return 'kernel'  # Includes network, allocator syscalls, scheduling and probe clocks.
    symbols = [s for s, _ in frames]
    if any('asap_stream_bench::profile::' in s for s in symbols):
        return 'profiling_probe'
    if any('asap_precompute_rs::otap::codec::' in s or
           re.search(r'asap_stream_bench::model::(?:values_pdata|sketch_pdata|quantiles_pdata)', s) for s in symbols):
        return 'application_pdata_conversion'
    if any('asap_sketchlib::' in s or 'asap_precompute_rs::sketches::' in s for s in symbols):
        return 'sketch_computation_or_serialization'
    if any(re.search(r'core::slice::sort::.*(?:<f64|::<f64)', s) for s in symbols):
        return 'exact_sort'
    if any('asap_stream_bench::model::' in s for s in symbols):
        return 'processor_other_or_inlined_computation'
    # Nearest identifiable owner: do not count an engine ancestor's inclusive
    # stack as framework work when its descendant is doing application work.
    for symbol in symbols:
        owner = symbol.lstrip('<')
        if owner.startswith(('otel_arrow_dfe_pdata::', 'otel_arrow_dfe_otap::pdata::', 'arrow_','prost::')):
            return 'upstream_pdata_or_wire_codec'
        if owner.startswith(('otel_arrow_dfe_engine::', 'otel_arrow_dfe_channel::', 'tokio::', 'futures_', 'mio::')):
            return 'engine_channels_async_runtime'
        if owner.startswith(('hyper::', 'hyper_util::', 'reqwest::', 'h2::', 'tonic::', 'tower::')) or \
                re.search(r'otel_arrow_dfe_otap::(?:otlp|exporters::otlp|receivers::otlp)', owner):
            return 'otlp_http_transport'
        if owner.startswith('asap_stream_bench::'):
            return 'harness_or_inlined_processor'
    if any(s.startswith(('malloc', 'cfree', 'realloc', '__rustc::', '<alloc::', 'alloc::')) for s in symbols):
        return 'allocation_without_resolved_owner'
    return 'other_or_unresolved'


def analyze_stacks(root):
    rows = []
    for path in sorted(root.glob('*mbit/*/*/perf-stacks.txt')):
        parsed = list(samples(path))
        mapping = demangle_names({s for _, frames in parsed for s, _ in frames})
        roles = {int(v): k for k, v in json.loads((path.parent / 'profile-pids.json').read_text()).items()}
        counts, leaves, folded = defaultdict(Counter), defaultdict(Counter), Counter()
        for pid, frames in parsed:
            frames = [(mapping.get(s, s), d) for s, d in frames]
            role = roles.get(pid, f'unknown_pid_{pid}')
            counts[role][family(frames)] += 1
            leaves[role][frames[0][0] if frames else '[unresolved]'] += 1
            stack = [s.replace(';', ':') for s, _ in reversed(frames)]
            folded[';'.join([role] + (stack or ['[unresolved]']))] += 1
        (path.parent / 'stacks.folded').write_text(''.join(f'{s} {count}\n' for s, count in folded.most_common()))
        tops = []
        for role, counts_for_role in leaves.items():
            for symbol, count in counts_for_role.most_common(40):
                tops.append({'role': role, 'samples': count, 'percent': 100 * count / sum(counts_for_role.values()), 'leaf_symbol': symbol})
        write_csv(path.parent / 'top-functions.csv', tops)
        for role, categories in counts.items():
            for category, count in categories.items():
                rows.append({'scenario': path.parent.parent.name, 'repeat': path.parent.name,
                             'role': role, 'category': category, 'samples': count,
                             'percent_of_role_samples': 100 * count / sum(categories.values())})
    write_csv(root / 'sample-categories.csv', rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--samples-root', type=Path)
    parser.add_argument('--control-root', type=Path)
    args = parser.parse_args()
    reports = []
    for path in sorted(args.root.glob('*mbit/*/*/result.json')):
        report = json.loads(path.read_text())
        if 'cpu_profile' not in report:
            raise ValueError(f'{path}: requires --profile-cpu or --account-cpu')
        reports.append(report)
    if not reports:
        parser.error('no completed CPU profile runs')
    if len({r['branch_egress_mbit_per_second'] for r in reports}) != 1:
        parser.error('analyze one bandwidth setting at a time')
    summary, per_role, per_run = [], [], []
    for scenario in ('raw', 'exact', 'kll'):
        runs = [r for r in reports if r['scenario'] == scenario]
        if not runs:
            continue
        total_inputs = sum(r['cpu_profile']['completed_input_signals'] for r in runs)
        components = Counter()
        for role in (*WORKERS, 'generator_a', 'generator_b', 'backend'):
            cpu = 0
            scopes = Counter()
            for r in runs:
                entry = r['cpu_profile']['roles'][role]
                cpu += entry['cpu_seconds']
                scopes.update(entry['scoped_cpu_seconds'])
                scopes['outside_scopes'] += entry['outside_scopes_cpu_seconds']
            if role in WORKERS:
                components.update(scopes)
            per_role.append({'scenario': scenario, 'role': role, 'group': 'worker' if role in WORKERS else 'harness',
                             'cpu_seconds_per_million_inputs': cpu / total_inputs * 1e6,
                             **{k + '_seconds_per_million_inputs': scopes[k] / total_inputs * 1e6 for k in CATEGORIES}})
        total = sum(components.values())
        costs = []
        for index, r in enumerate(runs, 1):
            cost = sum(r['cpu_profile']['roles'][role]['cpu_seconds'] for role in WORKERS) / r['cpu_profile']['completed_input_signals'] * 1e6
            costs.append(cost)
            per_run.append({'scenario': scenario, 'run': index, 'completed_inputs': r['cpu_profile']['completed_input_signals'],
                            'worker_cpu_seconds_per_million_inputs': cost, 'signals_per_second': r['signals_per_second'],
                            'correctness': r['correctness']})
        summary.append({'scenario': scenario, 'runs': len(runs), 'completed_inputs': total_inputs,
                        'cpu_seconds_per_million_inputs': total / total_inputs * 1e6,
                        'min_run_cpu_seconds_per_million_inputs': min(costs), 'max_run_cpu_seconds_per_million_inputs': max(costs),
                        'median_signals_per_second': statistics.median(r['signals_per_second'] for r in runs),
                        **{k + '_seconds_per_million_inputs': components[k] / total_inputs * 1e6 for k in CATEGORIES},
                        **{k + '_percent': 100 * components[k] / total for k in CATEGORIES}})
    for name, data in [('cpu-summary', summary), ('cpu-by-role', per_role), ('cpu-by-run', per_run)]:
        write_csv(args.root / (name + '.csv'), data)
        (args.root / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')
    if args.samples_root:
        analyze_stacks(args.samples_root)
    if args.control_root:
        comparison = []
        for row in summary:
            runs = [json.loads(p.read_text()) for p in args.control_root.glob(f'*mbit/{row["scenario"]}/*/result.json')]
            if not runs:
                continue
            inputs = sum(r['cpu_profile']['completed_input_signals'] for r in runs)
            control = sum(r['cpu_profile']['roles'][role]['cpu_seconds'] for r in runs for role in WORKERS) / inputs * 1e6
            comparison.append({'scenario': row['scenario'], 'control_runs': len(runs), 'control_cpu_seconds_per_million_inputs': control,
                               'profile_cpu_seconds_per_million_inputs': row['cpu_seconds_per_million_inputs'],
                               'difference_percent': (row['cpu_seconds_per_million_inputs'] / control - 1) * 100})
        write_csv(args.root / 'probe-comparison.csv', comparison)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
