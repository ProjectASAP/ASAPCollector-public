#!/usr/bin/env python3
"""Run the multi-process demo briefly and verify its metrics in Prometheus and Grafana.

Requires the asap-stream-benchmark:local image (built by run-demo.sh) and the
compose stack. Extra arguments are passed to benchmarks/run-streaming.py.
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PROCESSORS = ('branch_a', 'branch_b', 'merge', 'estimate')


def query(base, expression, grafana=False):
    path = '/api/v1/query?' + urllib.parse.urlencode({'query': expression})
    if grafana:
        path = '/api/datasources/proxy/uid/asap-prometheus' + path
    with urllib.request.urlopen(base.rstrip('/') + path, timeout=10) as response:
        payload = json.load(response)
    assert payload['status'] == 'success', payload
    return payload['data']['result']


def values(base, expression, keys, grafana=False):
    return {tuple(row['metric'].get(k, '') for k in keys): float(row['value'][1])
            for row in query(base, expression, grafana)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prometheus', default='http://localhost:9090')
    parser.add_argument('--grafana', default='http://localhost:3000')
    parser.add_argument('--docker-command', default='docker')
    parser.add_argument('--scenarios', nargs='+', default=['exact', 'kll'])
    parser.add_argument('--tolerance', type=float, default=0.05,
                        help='maximum relative error for sketch scenarios')
    args, extra = parser.parse_known_args()
    demo = subprocess.Popen([
        sys.executable, str(REPO / 'benchmarks/run-streaming.py'), '--demo',
        '--docker-command', args.docker_command, '--output', str(REPO / 'benchmark-results/demo-verify'),
        '--scenarios', *args.scenarios, '--window-points', '65536', '--rolling-windows', '3',
        '--generator-cores', '1', '--demo-rate', '50000', '--run-seconds', '60', *extra])
    try:
        deadline = time.monotonic() + 240
        while True:
            assert demo.poll() is None, 'demo exited before metrics were verified'
            assert time.monotonic() < deadline, 'demo metrics did not appear'
            windows = values(args.prometheus, 'asap_demo_completed_windows', ['scenario'])
            rolling = values(args.prometheus, 'asap_demo_estimate{window_kind="rolling"}', ['scenario'])
            if all(windows.get((s,), 0) >= 4 for s in args.scenarios) and len(rolling) == len(args.scenarios):
                break
            time.sleep(2)
        throughput = values(args.prometheus, 'asap_demo_validated_signals_per_second', ['scenario'])
        assert all(throughput[(s,)] > 0 for s in args.scenarios), throughput
        cpu = values(args.prometheus, 'asap_demo_cpu_cores', ['scenario', 'component'])
        assert all((s, c) in cpu for s in args.scenarios for c in PROCESSORS), cpu
        errors = values(args.prometheus, 'asap_demo_relative_error',
                        ['scenario', 'window_kind', 'quantile'])
        assert {(s, k, q) for s in args.scenarios for k in ('tumbling', 'rolling')
                for q in ('0.5', '0.99')} <= set(errors), errors
        for (scenario, _, _), error in errors.items():
            assert error <= (0.0 if scenario == 'exact' else args.tolerance), errors
        expression = 'asap_demo_estimate'
        keys = ['scenario', 'window_kind', 'quantile']
        assert set(values(args.prometheus, expression, keys)) == set(
            values(args.grafana, expression, keys, grafana=True))
        with urllib.request.urlopen(args.grafana.rstrip('/') + '/api/dashboards/uid/asap-kll-stream',
                                    timeout=10) as response:
            dashboard = json.load(response)['dashboard']
        assert any('asap_demo_validated_signals_per_second' in t['expr']
                   for p in dashboard['panels'] for t in p['targets'])
        print('throughput (signals/s):', {k[0]: round(v) for k, v in throughput.items()})
        print('relative error:', {'/'.join(k): round(v, 5) for k, v in sorted(errors.items())})
        print('Verified per-process throughput/CPU, tumbling and rolling accuracy, and Grafana queries.')
    finally:
        demo.wait(timeout=180)
    assert demo.returncode == 0, 'demo returned an error'


if __name__ == '__main__':
    main()
