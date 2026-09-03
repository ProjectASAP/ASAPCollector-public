#![cfg(feature = "otap-engine")]

use asap_precompute_rs::otap::codec::decode_pdata_to_observations;
use asap_sketchlib::sketches::KLL;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::OtlpProtoBytes;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const POINTS_PER_SOURCE: u64 = 25_000;

fn unique_dir() -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("asap-otap-e2e-{}-{id}", std::process::id()))
}

fn read_observations(path: &Path) -> Vec<asap_precompute_rs::observation::Observation> {
    let bytes = fs::read(path).expect("read OTLP artifact");
    let pdata = OtapPdata::new_todo_context(
        OtlpProtoBytes::new_from_bytes(SignalType::Metrics, bytes).into(),
    );
    decode_pdata_to_observations(pdata)
        .expect("decode OTLP artifact")
        .observations
}

#[test]
fn four_process_pipeline_preserves_asapv1_data_and_estimates_quantiles() {
    let dir = unique_dir();
    fs::create_dir(&dir).expect("create E2E directory");
    let manifest_path = dir.join("result.json");
    let output = Command::new(env!("CARGO_BIN_EXE_asap-otap-demo"))
        .args([
            "--output-dir",
            dir.to_str().unwrap(),
            "--result-manifest",
            manifest_path.to_str().unwrap(),
            "--points-per-source",
            &POINTS_PER_SOURCE.to_string(),
        ])
        .output()
        .expect("run multi-process demo");
    assert!(
        output.status.success(),
        "demo failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest: Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("result manifest exists"))
            .expect("valid result manifest");
    assert_eq!(manifest["points_per_source"], POINTS_PER_SOURCE);
    let parent_pid = manifest["parent_pid"].as_u64().expect("parent pid");
    let processors = manifest["processors"].as_array().expect("processor runs");
    assert_eq!(processors.len(), 4);
    let roles = processors
        .iter()
        .map(|p| p["role"].as_str().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(
        roles,
        HashSet::from(["create_a", "create_b", "merge", "estimate"])
    );
    let pids = processors
        .iter()
        .map(|p| p["pid"].as_u64().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(pids.len(), 4, "each DF processor must have its own PID");
    assert!(!pids.contains(&parent_pid));

    for name in ["sa.otlp", "sb.otlp", "merged.otlp"] {
        let observations = read_observations(&dir.join(name));
        assert_eq!(observations.len(), 1, "{name} contains one sketch");
        let observation = &observations[0];
        let sketch = observation
            .value
            .envelope
            .as_ref()
            .expect("sketch envelope");
        assert!(sketch.payload.starts_with(b"ASAPv1"), "{name} uses ASAPv1");
        KLL::<f64>::deserialize_from_bytes(&sketch.payload)
            .unwrap_or_else(|error| panic!("{name} contains a valid ASAPv1 KLL: {error}"));
        assert_eq!(
            observation
                .resource_labels
                .iter()
                .find(|kv| kv.key == "service.name")
                .map(|kv| kv.value.as_str()),
            Some("checkout")
        );
    }

    let estimates = read_observations(&dir.join("out.otlp"));
    assert_eq!(estimates.len(), 2);
    for (quantile, expected) in [("0.5", 25_000.0), ("0.99", 49_500.0)] {
        let value = estimates
            .iter()
            .find(|o| {
                o.labels
                    .iter()
                    .any(|kv| kv.key == "quantile" && kv.value == quantile)
            })
            .expect("quantile output");
        assert_eq!(value.metric, "request.duration.estimate");
        assert!((value.value.float - expected).abs() <= expected * 0.05);
        assert!(value
            .resource_labels
            .iter()
            .any(|kv| kv.key == "service.name" && kv.value == "checkout"));
    }

    for (trace, data_type) in [
        ("create-a.debug.log", "DataType: Summary"),
        ("create-b.debug.log", "DataType: Summary"),
        ("merge.debug.log", "DataType: Summary"),
        ("estimate.debug.log", "DataType: Gauge"),
    ] {
        let trace = fs::read_to_string(dir.join(trace)).expect("official debug trace");
        assert!(trace.contains(data_type));
        assert!(trace.contains("service.name: checkout"));
    }
}
