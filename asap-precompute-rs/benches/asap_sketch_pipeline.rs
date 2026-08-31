//! Criterion layer for the nightly end-to-end benchmark.
//!
//! The input models the stable `http.server.request.duration` metric and its
//! standard route/method/status dimensions. Values use a deterministic mixed
//! latency distribution so correctness can be checked independently of timing.

use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::observation::KeyValue;
use asap_precompute_rs::otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata};
use asap_precompute_rs::precompute::{QuantileSketch, Sketch};
use asap_precompute_rs::sketches::KLLWrapper;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;

const SIGNAL_COUNTS: [usize; 3] = [1_024, 16_384, 131_072];
const SHARDS: usize = 4;

fn semantic_http_durations(count: usize) -> Vec<f64> {
    (0..count)
        .map(|i| {
            // 95% normal traffic plus deterministic slow requests. Seconds,
            // matching the OTel HTTP metric semantic convention.
            if i % 20 == 0 {
                1.0 + (i % 17) as f64 * 0.025
            } else {
                0.005 + (i % 200) as f64 * 0.0005
            }
        })
        .collect()
}

fn exact_quantiles(values: &[f64]) -> (f64, f64) {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    (at(0.5), at(0.99))
}

fn sketch_quantiles(values: &[f64]) -> (f64, f64) {
    let mut shards = (0..SHARDS)
        .map(|seed| {
            KLLWrapper::new(200, Some(seed as u64 + 1)).with_wire_encoding(Encoding::Msgpack)
        })
        .collect::<Vec<_>>();
    for (index, value) in values.iter().copied().enumerate() {
        shards[index % SHARDS].update(value);
    }
    let mut merged = KLLWrapper::new(200, Some(99)).with_wire_encoding(Encoding::Msgpack);
    for shard in &shards {
        merged.merge(shard).expect("merge KLL shard");
    }
    (merged.quantile(0.5), merged.quantile(0.99))
}

fn otap_passthrough(values: &[f64]) -> usize {
    let input = values
        .iter()
        .copied()
        .map(|value| SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 0,
            resource_labels: vec![KeyValue::new("service.name", "checkout")],
            labels: vec![
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", "/checkout"),
                KeyValue::new("http.response.status_code", "200"),
            ],
            window_start_ms: 0,
            window_end_ms: 1,
            encoding: Encoding::Unspecified,
            payload: vec![],
            hash_spec: None,
            metric_name: "http.server.request.duration".to_owned(),
            count: 0,
            aggregation_temporality: 0,
            value,
        })
        .collect::<Vec<_>>();
    let pdata = encode_envelopes_to_pdata(&input).expect("encode OTAP baseline");
    decode_pdata_to_observations(pdata)
        .expect("decode OTAP baseline")
        .observations
        .len()
}

fn assert_accuracy(values: &[f64]) {
    let exact = exact_quantiles(values);
    let sketch = sketch_quantiles(values);
    for (name, got, want) in [("p50", sketch.0, exact.0), ("p99", sketch.1, exact.1)] {
        let relative_error = (got - want).abs() / want.max(f64::EPSILON);
        assert!(
            relative_error <= 0.05,
            "{name}: got {got}, want {want}, relative error {relative_error}"
        );
    }
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("http.server.request.duration");
    for count in SIGNAL_COUNTS {
        let values = semantic_http_durations(count);
        assert_accuracy(&values);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("exact_sort", count),
            &values,
            |b, input| b.iter(|| black_box(exact_quantiles(black_box(input)))),
        );
        group.bench_with_input(
            BenchmarkId::new("otap_pdata_roundtrip", count),
            &values,
            |b, input| b.iter(|| black_box(otap_passthrough(black_box(input)))),
        );
        group.bench_with_input(
            BenchmarkId::new("asap_kll_4way_merge", count),
            &values,
            |b, input| b.iter(|| black_box(sketch_quantiles(black_box(input)))),
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
