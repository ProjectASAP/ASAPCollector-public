//! Nightly traffic -> SUT -> backend benchmark.
//!
//! All three paths consume the same pre-generated
//! native `OtapPdata`, decode it at ingress, encode their results as native
//! pdata, and decode/validate those results in the simulated backend.

use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::observation::{KeyValue, Observation};
use asap_precompute_rs::otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata};
use asap_precompute_rs::precompute::{QuantileSketch, Sketch};
use asap_precompute_rs::sketches::KLLWrapper;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use std::hint::black_box;

const SIGNAL_COUNTS: [usize; 3] = [1_024, 16_384, 131_072];
const SHARDS: usize = 4;

fn semantic_http_durations(count: usize) -> Vec<f64> {
    (0..count)
        .map(|i| {
            if i % 20 == 0 {
                1.0 + (i % 17) as f64 * 0.025
            } else {
                0.005 + (i % 200) as f64 * 0.0005
            }
        })
        .collect()
}

fn scalar_envelope(value: f64) -> SketchEnvelope {
    SketchEnvelope {
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
    }
}

fn generated_pdata(values: &[f64]) -> OtapPdata {
    let envelopes = values
        .iter()
        .copied()
        .map(scalar_envelope)
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&envelopes).expect("traffic generator pdata")
}

fn exact_quantiles(values: &[f64]) -> (f64, f64) {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    (at(0.5), at(0.99))
}

fn quantile_output(p50: f64, p99: f64) -> Vec<Observation> {
    let output = [("0.5", p50), ("0.99", p99)]
        .into_iter()
        .map(|(quantile, value)| {
            let mut envelope = scalar_envelope(value);
            envelope.metric_name = "http.server.request.duration.estimate".into();
            envelope.labels.push(KeyValue::new("quantile", quantile));
            envelope
        })
        .collect::<Vec<_>>();
    backend_decode(encode_envelopes_to_pdata(&output).expect("quantile output pdata"))
}

fn backend_decode(pdata: OtapPdata) -> Vec<Observation> {
    decode_pdata_to_observations(pdata)
        .expect("simulated backend decode")
        .observations
}

fn control_pipeline(input: OtapPdata) -> Vec<Observation> {
    let observations = backend_decode(input);
    let output = observations
        .iter()
        .map(|observation| {
            let mut envelope = scalar_envelope(observation.value.float);
            envelope.resource_labels = observation.resource_labels.clone();
            envelope.labels = observation.labels.clone();
            envelope.metric_name = observation.metric.clone();
            envelope
        })
        .collect::<Vec<_>>();
    backend_decode(encode_envelopes_to_pdata(&output).expect("control output pdata"))
}

fn otap_exact_quantile_pipeline(input: OtapPdata) -> Vec<Observation> {
    let observations = backend_decode(input);
    let values = observations
        .iter()
        .map(|observation| observation.value.float)
        .collect::<Vec<_>>();
    let (p50, p99) = exact_quantiles(&values);
    quantile_output(p50, p99)
}

fn asap_pipeline(input: OtapPdata) -> Vec<Observation> {
    let observations = backend_decode(input);
    let mut shards = (0..SHARDS)
        .map(|seed| {
            KLLWrapper::new(200, Some(seed as u64 + 1)).with_wire_encoding(Encoding::Msgpack)
        })
        .collect::<Vec<_>>();
    for (index, observation) in observations.iter().enumerate() {
        shards[index % SHARDS].update(observation.value.float);
    }
    let mut merged = KLLWrapper::new(200, Some(99)).with_wire_encoding(Encoding::Msgpack);
    for shard in &shards {
        merged.merge(shard).expect("ASAPv1 KLL shard merge");
    }
    quantile_output(merged.quantile(0.5), merged.quantile(0.99))
}

fn assert_control(input: &OtapPdata, values: &[f64]) {
    let output = control_pipeline(input.clone());
    assert_eq!(output.len(), values.len());
    assert_eq!(output[0].value.float, values[0]);
    assert!(output[0]
        .resource_labels
        .contains(&KeyValue::new("service.name", "checkout")));
    assert!(output[0]
        .labels
        .contains(&KeyValue::new("http.route", "/checkout")));
}

fn assert_quantile_pipelines(input: &OtapPdata, values: &[f64]) {
    let exact = exact_quantiles(values);
    for (pipeline, output, maximum_error) in [
        (
            "OTAP exact",
            otap_exact_quantile_pipeline(input.clone()),
            0.0,
        ),
        ("ASAP KLL", asap_pipeline(input.clone()), 0.05),
    ] {
        assert_eq!(output.len(), 2, "{pipeline} output shape");
        for (name, quantile, want) in [("p50", "0.5", exact.0), ("p99", "0.99", exact.1)] {
            let observation = output
                .iter()
                .find(|observation| {
                    observation
                        .labels
                        .contains(&KeyValue::new("quantile", quantile))
                })
                .unwrap_or_else(|| panic!("{pipeline} missing {name}"));
            assert_eq!(observation.metric, "http.server.request.duration.estimate");
            assert!(observation
                .resource_labels
                .contains(&KeyValue::new("service.name", "checkout")));
            let got = observation.value.float;
            let relative_error = (got - want).abs() / want.max(f64::EPSILON);
            assert!(
                relative_error <= maximum_error,
                "{pipeline} {name}: got {got}, want {want}"
            );
        }
    }
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("http.server.request.duration");
    for count in SIGNAL_COUNTS {
        let values = semantic_http_durations(count);
        let input = generated_pdata(&values);
        assert_control(&input, &values);
        assert_quantile_pipelines(&input, &values);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("otap_exact_quantile_pipeline", count),
            &input,
            |b, p| b.iter(|| black_box(otap_exact_quantile_pipeline(black_box(p.clone())))),
        );
        group.bench_with_input(
            BenchmarkId::new("otap_control_pipeline", count),
            &input,
            |b, p| b.iter(|| black_box(control_pipeline(black_box(p.clone())))),
        );
        group.bench_with_input(
            BenchmarkId::new("asap_kll_pipeline", count),
            &input,
            |b, p| b.iter(|| black_box(asap_pipeline(black_box(p.clone())))),
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
