#!/usr/bin/env python3
"""Verify new tumbling/rolling samples continuously reach Prometheus and Grafana."""
import argparse
import json
import os
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


def query(base, expression, grafana=False):
    path = '/api/v1/query?' + urllib.parse.urlencode({'query': expression})
    if grafana:
        path = '/api/datasources/proxy/uid/asap-prometheus' + path
    with urllib.request.urlopen(base.rstrip('/') + path, timeout=10) as response:
        payload = json.load(response)
    assert payload['status'] == 'success', payload
    return payload['data']['result']


def counts(base, metric):
    return {row['metric']['quantile']: int(float(row['value'][1])) for row in query(
        base, 'count_over_time(' + metric + '{quantile=~"0.5|0.99"}[5m])')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', help='path to asap-kll-stream-demo')
    parser.add_argument('--prometheus', default='http://localhost:9090')
    parser.add_argument('--grafana', default='http://localhost:3000')
    args = parser.parse_args()
    metrics = ['request_duration_estimate', 'request_duration_rolling_estimate']
    initial = {metric: counts(args.prometheus, metric) for metric in metrics}
    observed = []
    with tempfile.TemporaryFile(mode='w+') as trace:
        process = subprocess.Popen([
            os.path.abspath(args.binary), '--window-seconds', '6', '--slide-seconds', '1',
            '--run-seconds', '16', '--prometheus-otlp-endpoint',
            args.prometheus.rstrip('/') + '/api/v1/otlp/v1/metrics',
        ], stdout=trace, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 40
            while process.poll() is None:
                assert time.monotonic() < deadline, 'streaming demo failed to stop'
                for metric in metrics:
                    rows = query(args.prometheus, metric + '{quantile=~"0.5|0.99"}')
                    if not rows:
                        continue
                    assert len(rows) == 2, ('expected stable series, not new window labels', rows)
                    assert {row['metric']['quantile'] for row in rows} == {'0.5', '0.99'}
                    assert all('sketch_window_start_ms' not in row['metric'] and
                               'sketch_window_end_ms' not in row['metric'] for row in rows)
                    if metric.endswith('rolling_estimate'):
                        observed.append(float(next(row['value'][1] for row in rows
                                                   if row['metric']['quantile'] == '0.5')))
                time.sleep(1)
            assert process.returncode == 0, 'streaming demo returned an error'
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            trace.seek(0)
            print(trace.read(), end='')
    for metric in metrics:
        after = counts(args.prometheus, metric)
        assert all(after.get(q, 0) >= initial[metric].get(q, 0) + 6 for q in ['0.5', '0.99']), (initial, after)
        expression = metric + '{quantile=~"0.5|0.99"}'
        prometheus = {row['metric']['quantile']: float(row['value'][1])
                      for row in query(args.prometheus, expression)}
        grafana = {row['metric']['quantile']: float(row['value'][1])
                   for row in query(args.grafana, expression, grafana=True)}
        assert prometheus == grafana, (prometheus, grafana)
    assert max(observed) - min(observed) > 5, 'rolling median did not follow the changing input'
    print('Verified repeated tumbling/rolling writes, stable series, changing rolling values, and matching Grafana queries.')


if __name__ == '__main__':
    main()
