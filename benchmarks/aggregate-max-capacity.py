#!/usr/bin/env python3
"""Aggregate per-trial run-streaming outputs into downstream capacity maxima."""
import argparse, csv, json
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument('root', type=Path)
p.add_argument('--min-delivery', type=float, default=.95)
p.add_argument('--max-backlog-windows', type=float, default=1.0)
a=p.parse_args()
runs=[]
for path in a.root.glob('**/runs.json'):
    if path.parent == a.root:
        continue
    runs.extend(json.loads(path.read_text()))
for r in runs:
    latency_limit=max(5000, 2000*r['window_points_per_source']/r['target_signals_per_second_per_source'])
    delivery=r.get('delivery_ratio_with_window_tolerance', r['delivery_ratio'])
    r['_sustainable']=(delivery >= a.min_delivery and
                       r['backlog_growth_windows'] <= a.max_backlog_windows and
                       r['window_latency_p99_ms'] <= latency_limit and
                       r['correctness']=='passed')
    downstream=('branch_a','branch_b','merge','estimate','backend')
    r['_max_downstream_cpu']=max(r['resources'][role]['cpu_cores_used'] for role in downstream)
rows=[]
windows=sorted({r['window_points_per_source'] for r in runs})
for window in windows:
    row={'window_points_per_source':window}
    for scenario in ('raw','exact','kll'):
        observed=[r for r in runs if r['scenario']==scenario and r['window_points_per_source']==window]
        peak=max(observed,key=lambda r:r['signals_per_second'],default=None)
        peak_prefix=scenario+'_maximum_observed_'
        row[peak_prefix+'signals_per_second']=peak['signals_per_second'] if peak else None
        row[peak_prefix+'offered_per_source']=peak['target_signals_per_second_per_source'] if peak else None
        row[peak_prefix+'generator_cores']=peak['generator_core_count'] if peak else None
        row[peak_prefix+'max_downstream_cpu']=peak['_max_downstream_cpu'] if peak else None
        candidates=[r for r in runs if r['scenario']==scenario and r['window_points_per_source']==window and r['_sustainable']]
        best=max(candidates,key=lambda r:r['signals_per_second'],default=None)
        prefix=scenario+'_'
        row[prefix+'maximum_sustainable_signals_per_second']=best['signals_per_second'] if best else None
        row[prefix+'offered_per_source']=best['target_signals_per_second_per_source'] if best else None
        row[prefix+'generator_cores']=best['generator_core_count'] if best else None
        row[prefix+'max_downstream_cpu']=best['_max_downstream_cpu'] if best else None
    if row['kll_maximum_sustainable_signals_per_second'] and row['exact_maximum_sustainable_signals_per_second']:
        row['kll_over_exact_capacity_speedup']=row['kll_maximum_sustainable_signals_per_second']/row['exact_maximum_sustainable_signals_per_second']
    else: row['kll_over_exact_capacity_speedup']=None
    if row['kll_maximum_sustainable_signals_per_second'] and row['raw_maximum_sustainable_signals_per_second']:
        row['kll_over_raw_ceiling']=row['kll_maximum_sustainable_signals_per_second']/row['raw_maximum_sustainable_signals_per_second']
    else: row['kll_over_raw_ceiling']=None
    exact_peak=row['exact_maximum_observed_signals_per_second']
    raw_peak=row['raw_maximum_observed_signals_per_second']
    kll_peak=row['kll_maximum_observed_signals_per_second']
    row['kll_over_exact_maximum_throughput']=kll_peak/exact_peak if kll_peak and exact_peak else None
    row['kll_over_raw_maximum_throughput']=kll_peak/raw_peak if kll_peak and raw_peak else None
    rows.append(row)
(a.root/'max-capacity.json').write_text(json.dumps(rows,indent=2)+'\n')
with (a.root/'max-capacity.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
print(json.dumps(rows,indent=2))
