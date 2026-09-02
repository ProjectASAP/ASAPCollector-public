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
const INPUT_BATCH_SIZE: usize = 4_096;

fn semantic_http_durations(count: usize) -> Vec<f64> {
    (0..count)
        .map(|i| {
            // Deterministic SplitMix64 output gives the duration field the
            // high-diversity continuous distribution produced by a traffic
            // generator, without making benchmark runs non-reproducible.
            let mut mixed = (i as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^= mixed >> 31;
            let unit = (mixed >> 11) as f64 / (1_u64 << 53) as f64;
            if i % 20 == 0 {
                1.0 + unit * 0.4
            } else {
                0.005 + unit * 0.1
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

fn generated_pdata(values: &[f64]) -> Vec<OtapPdata> {
    values
        .chunks(INPUT_BATCH_SIZE)
        .map(|batch| {
            let envelopes = batch
                .iter()
                .copied()
                .map(scalar_envelope)
                .collect::<Vec<_>>();
            encode_envelopes_to_pdata(&envelopes).expect("traffic generator pdata")
        })
        .collect()
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

fn control_pipeline(input: Vec<OtapPdata>) -> Vec<Observation> {
    let mut backend = Vec::new();
    for batch in input {
        let observations = backend_decode(batch);
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
        backend.extend(backend_decode(
            encode_envelopes_to_pdata(&output).expect("control output pdata"),
        ));
    }
    backend
}

fn otap_exact_quantile_pipeline(input: Vec<OtapPdata>) -> Vec<Observation> {
    let mut values = Vec::new();
    for batch in input {
        values.extend(
            backend_decode(batch)
                .into_iter()
                .map(|observation| observation.value.float),
        );
    }
    let (p50, p99) = exact_quantiles(&values);
    quantile_output(p50, p99)
}

fn asap_pipeline(input: Vec<OtapPdata>) -> Vec<Observation> {
    let mut shards = (0..SHARDS)
        .map(|seed| {
            KLLWrapper::new(200, Some(seed as u64 + 1)).with_wire_encoding(Encoding::Msgpack)
        })
        .collect::<Vec<_>>();
    let mut index = 0;
    for batch in input {
        for observation in backend_decode(batch) {
            shards[index % SHARDS].update(observation.value.float);
            index += 1;
        }
    }
    let mut merged = KLLWrapper::new(200, Some(99)).with_wire_encoding(Encoding::Msgpack);
    for shard in &shards {
        let bytes = shard.snapshot().expect("ASAPv1 KLL shard snapshot");
        let mut decoded = KLLWrapper::new(200, Some(99)).with_wire_encoding(Encoding::Msgpack);
        decoded
            .apply_delta_encoded(&bytes, Encoding::Msgpack)
            .expect("ASAPv1 KLL shard decode");
        merged.merge(&decoded).expect("ASAPv1 KLL shard merge");
    }
    quantile_output(merged.quantile(0.5), merged.quantile(0.99))
}

fn assert_control(input: &[OtapPdata], values: &[f64]) {
    let output = control_pipeline(input.to_vec());
    assert_eq!(output.len(), values.len());
    assert_eq!(output[0].value.float, values[0]);
    assert!(output[0]
        .resource_labels
        .contains(&KeyValue::new("service.name", "checkout")));
    assert!(output[0]
        .labels
        .contains(&KeyValue::new("http.route", "/checkout")));
}

fn assert_quantile_output(
    pipeline: &str,
    output: Vec<Observation>,
    exact: (f64, f64),
    maximum_error: f64,
) {
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

fn requested(scenario: &str, count: usize) -> bool {
    let filters = std::env::args()
        .skip(1)
        .filter(|arg| !arg.starts_with('-'))
        .collect::<Vec<_>>();
    filters.is_empty()
        || filters
            .iter()
            .any(|filter| filter.contains(&format!("{scenario}/{count}")))
}

fn assert_exact(input: &[OtapPdata], values: &[f64]) {
    let exact = exact_quantiles(values);
    assert_quantile_output(
        "OTAP exact",
        otap_exact_quantile_pipeline(input.to_vec()),
        exact,
        0.0,
    );
}

fn assert_kll(input: &[OtapPdata], values: &[f64]) {
    assert_quantile_output(
        "ASAP KLL",
        asap_pipeline(input.to_vec()),
        exact_quantiles(values),
        0.05,
    );
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("http.server.request.duration");
    for count in SIGNAL_COUNTS {
        let run_control = requested("otap_control_pipeline", count);
        let run_exact = requested("otap_exact_quantile_pipeline", count);
        let run_kll = requested("asap_kll_pipeline", count);
        if !run_control && !run_exact && !run_kll {
            continue;
        }
        let values = semantic_http_durations(count);
        let input = generated_pdata(&values);
        group.throughput(Throughput::Elements(count as u64));
        if run_exact {
            assert_exact(&input, &values);
            group.bench_with_input(
                BenchmarkId::new("otap_exact_quantile_pipeline", count),
                &input,
                |b, p| b.iter(|| black_box(otap_exact_quantile_pipeline(black_box(p.clone())))),
            );
        }
        if run_control {
            assert_control(&input, &values);
            group.bench_with_input(
                BenchmarkId::new("otap_control_pipeline", count),
                &input,
                |b, p| b.iter(|| black_box(control_pipeline(black_box(p.clone())))),
            );
        }
        if run_kll {
            assert_kll(&input, &values);
            group.bench_with_input(
                BenchmarkId::new("asap_kll_pipeline", count),
                &input,
                |b, p| b.iter(|| black_box(asap_pipeline(black_box(p.clone())))),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
