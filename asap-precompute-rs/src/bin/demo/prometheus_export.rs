//! Optional demo-only OTLP/HTTP export. Curl supplies HTTP/TLS without adding
//! an HTTP client dependency to the collector runtime.
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric::Data;
use prost_otap::Message;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp_gauges(bytes: &[u8], now: u64) -> Result<Vec<u8>, String> {
    let mut request = ExportMetricsServiceRequest::decode(bytes).map_err(|e| e.to_string())?;
    let mut points = 0;
    for resource in &mut request.resource_metrics {
        for scope in &mut resource.scope_metrics {
            let scope_attributes = scope
                .scope
                .as_ref()
                .map(|s| s.attributes.as_slice())
                .unwrap_or_default();
            for metric in &mut scope.metrics {
                let Some(Data::Gauge(gauge)) = &mut metric.data else {
                    return Err("Prometheus demo export expects only estimated gauges".into());
                };
                for point in &mut gauge.data_points {
                    // The OTAP codec stores series labels on the scope. Prometheus
                    // identifies series by point attributes, so materialize them here.
                    for attribute in scope_attributes {
                        if !point.attributes.iter().any(|a| a.key == attribute.key) {
                            point.attributes.push(attribute.clone());
                        }
                    }
                    // Synthetic windows stay in the trace files. Export is a current
                    // snapshot of the completed estimate, not historical telemetry.
                    point.start_time_unix_nano = 0;
                    point.time_unix_nano = now;
                    points += 1;
                }
            }
        }
    }
    if points == 0 {
        return Err("no estimated gauge points to export".into());
    }
    Ok(request.encode_to_vec())
}

pub fn export(path: &Path, endpoint: &str) -> Result<(), String> {
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        return Err("Prometheus OTLP endpoint must be an http:// or https:// URL".into());
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_nanos() as u64;
    let body = timestamp_gauges(&std::fs::read(path).map_err(|e| e.to_string())?, now)?;
    let mut child = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--fail-with-body",
            "--max-time",
            "30",
            "--header",
            "Content-Type: application/x-protobuf",
            "--header",
            "Accept: application/x-protobuf",
            "--data-binary",
            "@-",
            "--url",
            endpoint,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("start curl for Prometheus OTLP export: {e}"))?;
    let write_result = child.stdin.take().expect("piped stdin").write_all(&body);
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "Prometheus OTLP export failed: {} {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        ));
    }
    write_result.map_err(|e| format!("write OTLP request: {e}"))?;
    let response = ExportMetricsServiceResponse::decode(output.stdout.as_slice())
        .map_err(|e| format!("invalid Prometheus OTLP response: {e}"))?;
    if let Some(partial) = response.partial_success {
        if partial.rejected_data_points != 0 || !partial.error_message.is_empty() {
            return Err(format!(
                "Prometheus OTLP partial success: {} rejected: {}",
                partial.rejected_data_points, partial.error_message
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
        any_value::Value, AnyValue, InstrumentationScope, KeyValue,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };

    #[test]
    fn export_uses_current_timestamp_and_preserves_estimate() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope { attributes: vec![KeyValue { key: "quantile".into(), value: Some(AnyValue { value: Some(Value::StringValue("0.99".into())) }) }], ..Default::default() }),
                    metrics: vec![Metric {
                        name: "request.duration.estimate".into(),
                        data: Some(Data::Gauge(Gauge { data_points: vec![NumberDataPoint {
                            time_unix_nano: 2_000_000_000,
                            value: Some(otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::number_data_point::Value::AsDouble(198.0)),
                            ..Default::default()
                        }] })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let updated = ExportMetricsServiceRequest::decode(
            timestamp_gauges(&request.encode_to_vec(), 123456)
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let metric = &updated.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "request.duration.estimate");
        let Some(Data::Gauge(gauge)) = &metric.data else {
            panic!("expected gauge")
        };
        assert_eq!(gauge.data_points[0].time_unix_nano, 123456);
        let Some(Data::Gauge(original)) =
            &request.resource_metrics[0].scope_metrics[0].metrics[0].data
        else {
            panic!("expected gauge")
        };
        assert_eq!(gauge.data_points[0].value, original.data_points[0].value);
        assert_eq!(
            gauge.data_points[0].attributes,
            request.resource_metrics[0].scope_metrics[0]
                .scope
                .as_ref()
                .unwrap()
                .attributes
        );
        assert!(
            timestamp_gauges(&ExportMetricsServiceRequest::default().encode_to_vec(), 1).is_err()
        );
    }
}
