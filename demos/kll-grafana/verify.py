#!/usr/bin/env python3
"""Run the real demo and verify its estimates through Prometheus and Grafana."""
import argparse
import json
import re
import subprocess
import time
import urllib.parse
import urllib.request


def get(base, path):
    with urllib.request.urlopen(base.rstrip('/') + path, timeout=10) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', help='path to the built asap-otap-demo binary')
    parser.add_argument('--prometheus', default='http://localhost:9090')
    parser.add_argument('--grafana', default='http://localhost:3000')
    args = parser.parse_args()
    output = subprocess.check_output([
        args.binary, '--prometheus-otlp-endpoint',
        args.prometheus.rstrip('/') + '/api/v1/otlp/v1/metrics',
    ], text=True)
    print(output, end='')
    expected = {q: float(v) for q, v in re.findall(
        r'result metric=request.duration.estimate quantile=(\S+) value=(\S+)', output)}
    assert set(expected) == {'0.5', '0.99'}, expected
    query = '/api/v1/query?' + urllib.parse.urlencode({'query': 'request_duration_estimate{quantile=~"0.5|0.99"}'})
    for base, path in [
        (args.prometheus, query),
        (args.grafana, '/api/datasources/proxy/uid/asap-prometheus' + query),
    ]:
        for attempt in range(30):
            response = get(base, path)
            assert response['status'] == 'success', response
            actual = {row['metric']['quantile']: float(row['value'][1])
                      for row in response['data']['result']}
            if set(actual) == set(expected):
                break
            time.sleep(1)
        assert set(actual) == set(expected), actual
        for quantile, value in expected.items():
            assert abs(actual[quantile] - value) <= 0.001, (actual, expected)
    dashboard = get(args.grafana, '/api/dashboards/uid/asap-kll')['dashboard']
    assert dashboard['panels'][0]['targets'][0]['expr'].startswith('request_duration_estimate')
    print('Verified p50/p99 in Prometheus, Grafana datasource queries, and provisioned dashboard.')


if __name__ == '__main__':
    main()
