//! Answers to the three telemetry questions, from sketches or from exact data,
//! in one output shape so dashboards can overlay `method="sketch"` and
//! `method="exact"`.
//!
//! | metric | labels (besides the series labels) | value |
//! | --- | --- | --- |
//! | `asap.quantile` | `quantile`, `bound` | quantile of the window's values |
//! | `asap.distinct` | `bound` | distinct values of the series' attribute |
//! | `asap.points` | | data points in the window |
//! | `asap.topk` | `key`, `rank`, `bound` | count of a heavy-hitter value |
//!
//! Every answer also carries `metric` (the source metric) and `method`.
//! `bound` is `estimate`, `lower` or `upper`; exact answers only carry
//! `estimate`. Bounds come from each sketch's configuration:
//!
//! * HLL with 2^14 registers: standard error 1.04/sqrt(2^14) (0.81%), shown at
//!   95% (±1.96 standard errors).
//! * KLL: normalized rank error 2.296 / k^0.9723 (the DataSketches KLL
//!   formula, 99% confidence; 1.3% at k = 200), turned into the quantiles at
//!   q ± error.
//! * Count-Min: counts never underestimate; with width w and N points the
//!   overestimate is at most e/w * N with probability 1 - e^-depth.
use std::collections::{HashMap, HashSet};

use crate::envelope::{SketchEnvelope, SketchType};
use crate::observation::KeyValue;
use crate::precompute::{CardinalitySketch, FrequencySketch, PrecomputeError, QuantileSketch};
use crate::sketches::{CMSHeapWrapper, HLLWrapper, KLLWrapper};

/// HLL standard error with 2^14 registers.
pub const HLL_RELATIVE_ERROR: f64 = 1.04 / 128.0;
/// Two-sided 95% normal quantile.
pub const Z95: f64 = 1.96;

/// KLL normalized rank error for `k` (99% confidence).
pub fn kll_rank_error(k: usize) -> f64 {
    2.296 / (k as f64).powf(0.9723)
}

/// One scalar answer for a window, ready for the OTAP encoder.
pub fn gauge(
    template: &SketchEnvelope,
    metric: &str,
    method: &str,
    extra: &[(&str, String)],
    value: f64,
) -> SketchEnvelope {
    let mut labels = template.labels.clone();
    labels.push(KeyValue::new("metric", template.metric_name.clone()));
    labels.push(KeyValue::new("method", method));
    labels.extend(extra.iter().map(|(k, v)| KeyValue::new(*k, v.clone())));
    SketchEnvelope {
        schema_version: 1,
        sketch_type: SketchType::Unspecified,
        agg_id: template.agg_id,
        resource_labels: template.resource_labels.clone(),
        labels,
        window_start_ms: template.window_start_ms,
        window_end_ms: template.window_end_ms,
        payload: Vec::new(),
        metric_name: metric.into(),
        count: template.count,
        aggregation_temporality: 0,
        value,
    }
}

fn bounded(
    template: &SketchEnvelope,
    metric: &str,
    extra: &[(&str, String)],
    [estimate, lower, upper]: [f64; 3],
) -> Vec<SketchEnvelope> {
    [("estimate", estimate), ("lower", lower), ("upper", upper)]
        .into_iter()
        .map(|(bound, v)| {
            let mut labels = extra.to_vec();
            labels.push(("bound", bound.into()));
            gauge(template, metric, "sketch", &labels, v)
        })
        .collect()
}

/// Answers from one merged sketch window. `quantiles` applies to KLL, `k` to
/// top-k. Other sketch types produce no answers.
pub fn sketch_answers(
    env: &SketchEnvelope,
    quantiles: &[f64],
    k: usize,
) -> Result<Vec<SketchEnvelope>, PrecomputeError> {
    let mut out = vec![gauge(env, "asap.points", "sketch", &[], env.count as f64)];
    match env.sketch_type {
        SketchType::KLLSketch => {
            let sk = KLLWrapper::from_snapshot(&env.payload)?;
            let eps = kll_rank_error(sk.inner().k());
            for &q in quantiles {
                let at = |p: f64| sk.quantile(p.clamp(0.0, 1.0));
                out.extend(bounded(
                    env,
                    "asap.quantile",
                    &[("quantile", q.to_string())],
                    [at(q), at(q - eps), at(q + eps)],
                ));
            }
        }
        SketchType::HLLSketch => {
            let d = HLLWrapper::from_snapshot(&env.payload)?.estimate_cardinality();
            let e = Z95 * HLL_RELATIVE_ERROR * d;
            out.extend(bounded(env, "asap.distinct", &[], [d, d - e, d + e]));
        }
        SketchType::CountMinSketchWithHeap => {
            let sk = CMSHeapWrapper::from_snapshot(&env.payload)?;
            let over = std::f64::consts::E / sk.inner().cols() as f64 * env.count as f64;
            for (rank, entry) in sk.top_k(k).into_iter().enumerate() {
                let key = String::from_utf8_lossy(&entry.key).into_owned();
                out.extend(bounded(
                    env,
                    "asap.topk",
                    &[("key", key), ("rank", (rank + 1).to_string())],
                    [entry.count, (entry.count - over).max(0.0), entry.count],
                ));
            }
        }
        _ => {}
    }
    Ok(out)
}

/// Exact data for one series and window.
#[derive(Default)]
pub struct ExactWindow {
    /// Every value, for quantiles.
    pub values: Vec<f64>,
    /// Distinct attribute values.
    pub distinct: HashSet<String>,
    /// Occurrences per attribute value.
    pub counts: HashMap<String, u64>,
    /// Data points.
    pub points: u64,
}

/// Exact answers in the same shape as [`sketch_answers`].
pub fn exact_answers(
    template: &SketchEnvelope,
    window: &mut ExactWindow,
    quantiles: &[f64],
    k: usize,
) -> Vec<SketchEnvelope> {
    let e = |metric: &str, extra: &[(&str, String)], v: f64| {
        let mut labels = extra.to_vec();
        labels.push(("bound", "estimate".into()));
        gauge(template, metric, "exact", &labels, v)
    };
    let mut out = vec![gauge(
        template,
        "asap.points",
        "exact",
        &[],
        window.points as f64,
    )];
    if !window.values.is_empty() {
        window.values.sort_unstable_by(f64::total_cmp);
        for &q in quantiles {
            let i = ((window.values.len() - 1) as f64 * q).round() as usize;
            out.push(e(
                "asap.quantile",
                &[("quantile", q.to_string())],
                window.values[i],
            ));
        }
    }
    if !window.distinct.is_empty() {
        out.push(e("asap.distinct", &[], window.distinct.len() as f64));
    }
    let mut top: Vec<_> = window.counts.iter().collect();
    top.sort_unstable_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    for (rank, (key, count)) in top.into_iter().take(k).enumerate() {
        out.push(e(
            "asap.topk",
            &[("key", key.clone()), ("rank", (rank + 1).to_string())],
            *count as f64,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute::Sketch;

    fn find<'a>(
        out: &'a [SketchEnvelope],
        metric: &str,
        labels: &[(&str, &str)],
    ) -> &'a SketchEnvelope {
        out.iter()
            .find(|e| {
                e.metric_name == metric
                    && labels
                        .iter()
                        .all(|(k, v)| e.labels.iter().any(|kv| kv.key == *k && kv.value == *v))
            })
            .unwrap_or_else(|| panic!("missing {metric} {labels:?}"))
    }

    #[test]
    fn kll_bounds_contain_the_exact_quantile() {
        let mut sk = KLLWrapper::new(200, Some(1));
        let mut exact = ExactWindow::default();
        for i in 0..100_000u64 {
            let v = ((i * 7919) % 100_000) as f64;
            sk.update(v);
            exact.values.push(v);
        }
        let env = SketchEnvelope {
            sketch_type: SketchType::KLLSketch,
            payload: sk.snapshot().unwrap(),
            count: 100_000,
            ..Default::default()
        };
        let out = sketch_answers(&env, &[0.5, 0.99], 10).unwrap();
        let truth = exact_answers(&env, &mut exact, &[0.5, 0.99], 10);
        for q in ["0.5", "0.99"] {
            let t = find(&truth, "asap.quantile", &[("quantile", q)]).value;
            let lo = find(
                &out,
                "asap.quantile",
                &[("quantile", q), ("bound", "lower")],
            )
            .value;
            let hi = find(
                &out,
                "asap.quantile",
                &[("quantile", q), ("bound", "upper")],
            )
            .value;
            assert!(lo <= t && t <= hi, "{q}: {lo} <= {t} <= {hi}");
        }
    }

    #[test]
    fn hll_and_topk_answers_carry_bounds() {
        let mut hll = HLLWrapper::new();
        let mut heap = CMSHeapWrapper::new(4, 2048, 3);
        for i in 0..20_000u32 {
            hll.update(&i.to_le_bytes());
            heap.update(format!("k{}", i % 7 % 3).as_bytes(), 1);
        }
        let base = SketchEnvelope {
            count: 20_000,
            ..Default::default()
        };
        let hll_env = SketchEnvelope {
            sketch_type: SketchType::HLLSketch,
            payload: hll.snapshot().unwrap(),
            ..base.clone()
        };
        let out = sketch_answers(&hll_env, &[], 3).unwrap();
        let lo = find(&out, "asap.distinct", &[("bound", "lower")]).value;
        let hi = find(&out, "asap.distinct", &[("bound", "upper")]).value;
        assert!(lo < 20_000.0 && 20_000.0 < hi, "{lo} {hi}");

        let heap_env = SketchEnvelope {
            sketch_type: SketchType::CountMinSketchWithHeap,
            payload: heap.snapshot().unwrap(),
            ..base
        };
        let out = sketch_answers(&heap_env, &[], 3).unwrap();
        assert_eq!(
            find(&out, "asap.topk", &[("rank", "1"), ("bound", "estimate")])
                .labels
                .iter()
                .find(|kv| kv.key == "key")
                .unwrap()
                .value,
            "k0"
        );
        assert!(out.iter().all(|e| e
            .labels
            .iter()
            .any(|kv| kv.key == "method" && kv.value == "sketch")));
    }
}
