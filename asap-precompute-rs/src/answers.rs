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

/// Exact data for one series and window: what an exact branch builds and an
/// exact merge adds up.
#[derive(Debug, Default, PartialEq)]
pub struct ExactWindow {
    /// Every value, for quantiles.
    pub values: Vec<f64>,
    /// Occurrences per attribute value; its size is the distinct count.
    pub counts: HashMap<String, u64>,
    /// Data points.
    pub points: u64,
}

const EXACT_MAGIC: &[u8; 4] = b"EXv1";

impl ExactWindow {
    /// Adds another source's window.
    pub fn merge(&mut self, other: ExactWindow) {
        self.values.extend(other.values);
        for (k, c) in other.counts {
            *self.counts.entry(k).or_default() += c;
        }
        self.points += other.points;
    }

    /// `EXv1`, points, values, then (key, count) pairs; little-endian.
    pub fn encode(&self) -> Vec<u8> {
        let keys: usize = self.counts.keys().map(|k| 12 + k.len()).sum();
        let mut out = Vec::with_capacity(28 + 8 * self.values.len() + keys);
        out.extend_from_slice(EXACT_MAGIC);
        out.extend_from_slice(&self.points.to_le_bytes());
        out.extend_from_slice(&(self.values.len() as u64).to_le_bytes());
        for v in &self.values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&(self.counts.len() as u64).to_le_bytes());
        for (k, c) in &self.counts {
            out.extend_from_slice(&(k.len() as u32).to_le_bytes());
            out.extend_from_slice(k.as_bytes());
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    /// Inverse of [`Self::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        struct Cursor<'a>(&'a [u8]);
        impl<'a> Cursor<'a> {
            fn take(&mut self, n: usize) -> Result<&'a [u8], PrecomputeError> {
                if self.0.len() < n {
                    return Err(PrecomputeError::Other(
                        "truncated exact window payload".into(),
                    ));
                }
                let (head, tail) = self.0.split_at(n);
                self.0 = tail;
                Ok(head)
            }
            fn u64(&mut self) -> Result<u64, PrecomputeError> {
                Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
        }
        let rest = bytes
            .strip_prefix(EXACT_MAGIC)
            .ok_or_else(|| PrecomputeError::Other("not an exact window payload".into()))?;
        let mut c = Cursor(rest);
        let points = c.u64()?;
        let n = c.u64()? as usize;
        let values = (0..n)
            .map(|_| c.u64().map(f64::from_bits))
            .collect::<Result<Vec<_>, _>>()?;
        let n = c.u64()? as usize;
        let mut counts = HashMap::with_capacity(n.min(1 << 24));
        for _ in 0..n {
            let len = u32::from_le_bytes(c.take(4)?.try_into().unwrap()) as usize;
            let key = std::str::from_utf8(c.take(len)?)
                .map_err(|_| PrecomputeError::Other("exact window key is not UTF-8".into()))?;
            counts.insert(key.to_owned(), c.u64()?);
        }
        Ok(Self {
            values,
            counts,
            points,
        })
    }
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
    if !window.counts.is_empty() {
        out.push(e("asap.distinct", &[], window.counts.len() as f64));
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

/// Thresholds for [`DriftTracker`].
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriftRules {
    /// Windows merged into the baseline.
    #[serde(default = "default_drift_baseline")]
    pub baseline_windows: usize,
    /// Kolmogorov-Smirnov distance above which a series has drifted.
    #[serde(default = "default_ks")]
    pub ks_threshold: f64,
    /// Points needed in the current window before testing.
    #[serde(default = "default_drift_points")]
    pub min_points: u64,
}

fn default_drift_baseline() -> usize {
    5
}
fn default_ks() -> f64 {
    0.1
}
fn default_drift_points() -> u64 {
    1000
}

impl Default for DriftRules {
    fn default() -> Self {
        Self {
            baseline_windows: default_drift_baseline(),
            ks_threshold: default_ks(),
            min_points: default_drift_points(),
        }
    }
}

/// Largest gap between two CDFs over the points `xs` (Kolmogorov-Smirnov).
pub fn ks_distance(current: impl Fn(f64) -> f64, baseline: impl Fn(f64) -> f64, xs: &[f64]) -> f64 {
    xs.iter()
        .map(|&x| (current(x) - baseline(x)).abs())
        .fold(0.0, f64::max)
}

/// Turns each window's KS distance into a drift gauge and an event when a
/// series starts or stops drifting. Sketch and exact use the same rules.
#[derive(Default)]
pub struct DriftTracker {
    /// Thresholds.
    pub rules: DriftRules,
    drifting: HashMap<String, bool>,
}

impl DriftTracker {
    /// Tracker with the given thresholds.
    pub fn new(rules: DriftRules) -> Self {
        Self {
            rules,
            drifting: HashMap::new(),
        }
    }

    /// Records one window's distance from its baseline.
    pub fn observe(
        &mut self,
        template: &SketchEnvelope,
        method: &str,
        ks: f64,
        baseline_p50: f64,
        current_p50: f64,
    ) -> (Vec<SketchEnvelope>, Option<Event>) {
        let drift = ks > self.rules.ks_threshold;
        let out = vec![
            gauge(template, "asap.drift.ks", method, &[], ks),
            gauge(
                template,
                "asap.drift",
                method,
                &[],
                if drift { 1.0 } else { 0.0 },
            ),
        ];
        let key = format!("{}|{:?}", template.metric_name, template.labels);
        let previous = self.drifting.insert(key, drift);
        let series = template
            .labels
            .iter()
            .map(|kv| format!("{}={}", kv.key, kv.value))
            .collect::<Vec<_>>()
            .join(",");
        let event = (previous.unwrap_or(false) != drift).then(|| Event {
            window_end_ms: template.window_end_ms,
            method: method.into(),
            kind: "drift".into(),
            attributes: template
                .labels
                .iter()
                .cloned()
                .chain([
                    KeyValue::new("drift", drift.to_string()),
                    KeyValue::new("ks", format!("{ks:.3}")),
                ])
                .collect(),
            body: if drift {
                format!(
                    "{} {{{series}}} drifted from its baseline: KS distance {ks:.3}, median {baseline_p50:.1} -> {current_p50:.1}",
                    template.metric_name
                )
            } else {
                format!(
                    "{} {{{series}}} is back within its baseline: KS distance {ks:.3}",
                    template.metric_name
                )
            },
        });
        (out, event)
    }
}

/// The `(key, count)` of every `asap.topk` estimate in `answers`, by rank.
pub fn topk_of(answers: &[SketchEnvelope]) -> Vec<(String, f64)> {
    let label = |e: &SketchEnvelope, k: &str| {
        e.labels
            .iter()
            .find(|kv| kv.key == k)
            .map(|kv| kv.value.clone())
    };
    let mut top: Vec<(u32, String, f64)> = answers
        .iter()
        .filter(|e| {
            e.metric_name == "asap.topk" && label(e, "bound").as_deref() == Some("estimate")
        })
        .filter_map(|e| Some((label(e, "rank")?.parse().ok()?, label(e, "key")?, e.value)))
        .collect();
    top.sort_by_key(|t| t.0);
    top.into_iter().map(|(_, k, v)| (k, v)).collect()
}

/// How well a sketch's top-k finds and counts one window's heavy hitters: the
/// keys with at least `phi` of the window's points.
#[derive(Clone, Debug, PartialEq)]
pub struct HeavyHitterAccuracy {
    /// Heavy hitters in the exact counts.
    pub truth: usize,
    /// Keys whose sketch estimate reaches the threshold.
    pub reported: usize,
    /// Reported keys that are heavy hitters.
    pub true_positives: usize,
    /// True positives / reported (1 when nothing is reported).
    pub precision: f64,
    /// True positives / heavy hitters (1 when there are none).
    pub recall: f64,
    /// Harmonic mean of precision and recall.
    pub f1: f64,
    /// Mean of |estimate - exact| / exact over the true positives (`None`
    /// without any): the sketch has no count for a heavy hitter it missed.
    pub average_relative_error: Option<f64>,
}

/// Compares a window's sketch top-k with its exact top-k at threshold
/// `phi * points`. Both lists are `(key, count)` as reported, at most `k`
/// long. Returns `None` when either list is full and its smallest count still
/// reaches the threshold: the window may have more heavy hitters (exact) or
/// more reported keys (sketch) than the list shows.
pub fn heavy_hitter_accuracy(
    phi: f64,
    points: f64,
    k: usize,
    exact: &[(String, f64)],
    sketch: &[(String, f64)],
) -> Option<HeavyHitterAccuracy> {
    let threshold = phi * points;
    let cut_short = |list: &[(String, f64)]| {
        list.len() >= k && list.iter().map(|e| e.1).fold(f64::INFINITY, f64::min) >= threshold
    };
    if cut_short(exact) || cut_short(sketch) {
        return None;
    }
    let truth: HashMap<&str, f64> = exact
        .iter()
        .filter(|e| e.1 >= threshold)
        .map(|(key, c)| (key.as_str(), *c))
        .collect();
    let reported: Vec<&(String, f64)> = sketch.iter().filter(|e| e.1 >= threshold).collect();
    let errors: Vec<f64> = reported
        .iter()
        .filter_map(|(key, estimate)| truth.get(key.as_str()).map(|c| (estimate - c).abs() / c))
        .collect();
    let tp = errors.len();
    let ratio = |n: usize, d: usize| if d == 0 { 1.0 } else { n as f64 / d as f64 };
    let (precision, recall) = (ratio(tp, reported.len()), ratio(tp, truth.len()));
    Some(HeavyHitterAccuracy {
        truth: truth.len(),
        reported: reported.len(),
        true_positives: tp,
        precision,
        recall,
        f1: if precision + recall == 0.0 {
            0.0
        } else {
            2.0 * precision * recall / (precision + recall)
        },
        average_relative_error: (tp > 0).then(|| errors.iter().sum::<f64>() / tp as f64),
    })
}

/// Flags heavy hitters that were not among the top keys of the previous
/// windows. Sketch and exact use the same rule.
#[derive(Default)]
pub struct NoveltyTracker {
    /// Windows whose top keys form the baseline.
    pub baseline_windows: usize,
    history: HashMap<String, std::collections::VecDeque<HashSet<String>>>,
}

impl NoveltyTracker {
    /// Tracker remembering `baseline_windows` windows.
    pub fn new(baseline_windows: usize) -> Self {
        Self {
            baseline_windows,
            history: HashMap::new(),
        }
    }

    /// New heavy hitters in this window's `top` (key, count), as gauges and events.
    pub fn observe(
        &mut self,
        template: &SketchEnvelope,
        method: &str,
        top: &[(String, f64)],
    ) -> (Vec<SketchEnvelope>, Vec<Event>) {
        let key = format!("{}|{:?}", template.metric_name, template.labels);
        let window = self.history.entry(key).or_default();
        let (mut out, mut events) = (Vec::new(), Vec::new());
        if window.len() >= 3 {
            let attribute = template
                .labels
                .iter()
                .find(|kv| kv.key == crate::matchers::ATTRIBUTE_KEY_LABEL)
                .map(|kv| kv.value.clone())
                .unwrap_or_default();
            for (rank, (k, count)) in top.iter().enumerate() {
                if window.iter().any(|seen| seen.contains(k)) {
                    continue;
                }
                out.push(gauge(
                    template,
                    "asap.topk.new",
                    method,
                    &[("key", k.clone())],
                    *count,
                ));
                events.push(Event {
                    window_end_ms: template.window_end_ms,
                    method: method.into(),
                    kind: "novelty".into(),
                    attributes: vec![
                        KeyValue::new("attribute.key", attribute.clone()),
                        KeyValue::new("key", k.clone()),
                        KeyValue::new("rank", (rank + 1).to_string()),
                        KeyValue::new("count", format!("{count:.0}")),
                    ],
                    body: format!(
                        "new heavy hitter {attribute}={k}: rank {} with {count:.0} points, absent from the top {} of the previous {} windows",
                        rank + 1,
                        top.len(),
                        window.len()
                    ),
                });
            }
        }
        window.push_back(top.iter().map(|(k, _)| k.clone()).collect());
        if window.len() > self.baseline_windows {
            window.pop_front();
        }
        (out, events)
    }
}

#[cfg(test)]
mod classifier_tests {
    use super::*;

    #[test]
    fn novelty_after_three_windows() {
        let mut t = NoveltyTracker::new(5);
        let s = SketchEnvelope {
            metric_name: "m".into(),
            ..Default::default()
        };
        let usual = vec![("a".to_string(), 10.0), ("b".to_string(), 5.0)];
        for _ in 0..3 {
            assert!(t.observe(&s, "sketch", &usual).1.is_empty());
        }
        let now = vec![("new".to_string(), 20.0), ("a".to_string(), 10.0)];
        let (out, events) = t.observe(&s, "sketch", &now);
        assert_eq!(out.len(), 1);
        assert!(events[0].body.contains("new heavy hitter =new: rank 1"));
        // The next window, the key is part of the baseline.
        assert!(t.observe(&s, "sketch", &now).1.is_empty());
    }

    #[test]
    fn ks_and_drift_events() {
        let uniform = |x: f64| x.clamp(0.0, 1.0);
        let shifted = |x: f64| (x - 0.3).clamp(0.0, 1.0);
        let xs: Vec<f64> = (0..=100).map(|i| i as f64 / 100.0).collect();
        let ks = ks_distance(shifted, uniform, &xs);
        assert!((ks - 0.3).abs() < 1e-9, "{ks}");
        let mut t = DriftTracker::default();
        let s = SketchEnvelope {
            metric_name: "m".into(),
            ..Default::default()
        };
        assert!(t.observe(&s, "sketch", 0.02, 1.0, 1.0).1.is_none());
        let (out, event) = t.observe(&s, "sketch", 0.3, 1.0, 2.0);
        assert_eq!(out[1].value, 1.0);
        assert!(event.unwrap().body.contains("drifted"));
        assert!(t
            .observe(&s, "sketch", 0.03, 1.0, 1.0)
            .1
            .unwrap()
            .body
            .contains("back"));
    }

    fn attr(key: &str) -> SketchEnvelope {
        SketchEnvelope {
            metric_name: "m".into(),
            labels: vec![KeyValue::new("attribute.key", key)],
            ..Default::default()
        }
    }

    fn state(out: &[SketchEnvelope]) -> String {
        out.iter()
            .find(|e| e.metric_name == "asap.cardinality.state" && e.value == 1.0)
            .and_then(|e| e.labels.iter().find(|kv| kv.key == "state"))
            .map(|kv| kv.value.clone())
            .unwrap()
    }

    #[test]
    fn identifier_growing_and_stable() {
        let mut c = CardinalityClassifier::default();
        let (out, event) = c.observe(&attr("user.id"), "sketch", 980.0, 1000);
        assert_eq!(state(&out), "identifier-like");
        assert!(event.unwrap().body.contains("identifier-like"));
        for _ in 0..4 {
            let (out, _) = c.observe(&attr("pod"), "sketch", 50.0, 1000);
            assert_eq!(state(&out), "stable");
        }
        let (out, event) = c.observe(&attr("pod"), "sketch", 70.0, 1000);
        assert_eq!(state(&out), "growing");
        assert_eq!(event.unwrap().attributes[1].value, "growing");
        // No event while the state does not change.
        let (_, event) = c.observe(&attr("pod"), "sketch", 90.0, 1000);
        assert!(event.is_none());
        // Too few points: no answer.
        assert!(c.observe(&attr("rare"), "sketch", 5.0, 5).0.is_empty());
    }
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

/// A detection result worth telling a person about, emitted as a log record.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// Window the event refers to.
    pub window_end_ms: u64,
    /// `sketch` or `exact`.
    pub method: String,
    /// Event kind, e.g. `cardinality`.
    pub kind: String,
    /// Series labels plus event fields.
    pub attributes: Vec<KeyValue>,
    /// One readable sentence.
    pub body: String,
}

/// Cardinality class of an attribute key in one window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CardinalityState {
    /// Distinct values close to the baseline.
    Stable,
    /// Distinct values well above the baseline mean.
    Growing,
    /// Almost every point carries a new value, like an identifier.
    IdentifierLike,
}

impl CardinalityState {
    /// Name used in labels and events.
    pub fn name(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Growing => "growing",
            Self::IdentifierLike => "identifier-like",
        }
    }
}

/// Thresholds for [`CardinalityClassifier`].
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CardinalityRules {
    /// Windows kept as the baseline.
    #[serde(default = "default_baseline")]
    pub baseline_windows: usize,
    /// distinct / points at or above which a key is identifier-like.
    #[serde(default = "default_identifier_ratio")]
    pub identifier_ratio: f64,
    /// Fraction above the baseline mean at which a key is growing.
    #[serde(default = "default_growth")]
    pub growth: f64,
    /// Points needed before classifying.
    #[serde(default = "default_min_points")]
    pub min_points: u64,
}

fn default_baseline() -> usize {
    10
}
fn default_identifier_ratio() -> f64 {
    0.9
}
fn default_growth() -> f64 {
    0.2
}
fn default_min_points() -> u64 {
    100
}

impl Default for CardinalityRules {
    fn default() -> Self {
        Self {
            baseline_windows: default_baseline(),
            identifier_ratio: default_identifier_ratio(),
            growth: default_growth(),
            min_points: default_min_points(),
        }
    }
}

/// Classifies each attribute key against its own recent windows. The same
/// rules run on sketch and exact answers so their labels can be compared.
#[derive(Default)]
pub struct CardinalityClassifier {
    rules: CardinalityRules,
    history: HashMap<String, (std::collections::VecDeque<f64>, Option<CardinalityState>)>,
}

impl CardinalityClassifier {
    /// Classifier with the given thresholds.
    pub fn new(rules: CardinalityRules) -> Self {
        Self {
            rules,
            history: HashMap::new(),
        }
    }

    /// Classifies one window of `series` (its distinct count and points), and
    /// returns its state gauge plus an event when the state changed.
    pub fn observe(
        &mut self,
        template: &SketchEnvelope,
        method: &str,
        distinct: f64,
        points: u64,
    ) -> (Vec<SketchEnvelope>, Option<Event>) {
        if points < self.rules.min_points {
            return (Vec::new(), None);
        }
        let key = format!("{}|{:?}", template.metric_name, template.labels);
        let (window, previous) = self.history.entry(key).or_default();
        let baseline =
            (window.len() >= 3).then(|| window.iter().sum::<f64>() / window.len() as f64);
        let ratio = distinct / points as f64;
        let state = if ratio >= self.rules.identifier_ratio {
            CardinalityState::IdentifierLike
        } else if baseline.is_some_and(|b| distinct > b * (1.0 + self.rules.growth)) {
            CardinalityState::Growing
        } else {
            CardinalityState::Stable
        };
        window.push_back(distinct);
        if window.len() > self.rules.baseline_windows {
            window.pop_front();
        }
        let mut out = vec![gauge(template, "asap.distinct.ratio", method, &[], ratio)];
        out.extend(
            [
                CardinalityState::Stable,
                CardinalityState::Growing,
                CardinalityState::IdentifierLike,
            ]
            .into_iter()
            .map(|s| {
                let v = if s == state { 1.0 } else { 0.0 };
                gauge(
                    template,
                    "asap.cardinality.state",
                    method,
                    &[("state", s.name().into())],
                    v,
                )
            }),
        );
        let changed = *previous != Some(state);
        *previous = Some(state);
        let attribute = template
            .labels
            .iter()
            .find(|kv| kv.key == crate::matchers::ATTRIBUTE_KEY_LABEL)
            .map(|kv| kv.value.clone())
            .unwrap_or_default();
        let event = changed.then(|| Event {
            window_end_ms: template.window_end_ms,
            method: method.into(),
            kind: "cardinality".into(),
            attributes: vec![
                KeyValue::new("attribute.key", attribute.clone()),
                KeyValue::new("state", state.name()),
                KeyValue::new("distinct", format!("{distinct:.0}")),
                KeyValue::new("points", points.to_string()),
            ],
            body: format!(
                "attribute {attribute} of {} is {}: {distinct:.0} distinct values in {points} points ({:.0}%){}",
                template.metric_name,
                state.name(),
                ratio * 100.0,
                baseline.map(|b| format!(", baseline {b:.0}")).unwrap_or_default()
            ),
        });
        (out, event)
    }
}

#[cfg(test)]
mod heavy_hitter_tests {
    use super::*;

    #[test]
    fn heavy_hitter_accuracy_counts_false_positives_and_misses() {
        let list = |v: &[(&str, f64)]| {
            v.iter()
                .map(|(k, c)| (k.to_string(), *c))
                .collect::<Vec<_>>()
        };
        // 1,000 points, phi 10%: a, b, c are heavy; the sketch overestimates
        // d past the threshold and misses c.
        let exact = list(&[("a", 400.0), ("b", 200.0), ("c", 100.0), ("d", 90.0)]);
        let sketch = list(&[("a", 410.0), ("b", 220.0), ("d", 105.0)]);
        let acc = heavy_hitter_accuracy(0.1, 1000.0, 20, &exact, &sketch).unwrap();
        assert_eq!((acc.truth, acc.reported, acc.true_positives), (3, 3, 2));
        assert!((acc.precision - 2.0 / 3.0).abs() < 1e-12);
        assert!((acc.recall - 2.0 / 3.0).abs() < 1e-12);
        assert!((acc.f1 - 2.0 / 3.0).abs() < 1e-12);
        // (10/400 + 20/200) / 2
        assert!((acc.average_relative_error.unwrap() - 0.0625).abs() < 1e-12);
        // A full exact list whose last count is still heavy may be cut short.
        assert!(heavy_hitter_accuracy(0.05, 1000.0, 4, &exact, &sketch).is_none());
        assert!(heavy_hitter_accuracy(0.1, 1000.0, 4, &exact, &sketch).is_some());
        // So may a full sketch list whose last estimate is still reported.
        assert!(heavy_hitter_accuracy(0.1, 1000.0, 3, &exact[3..], &sketch).is_none());
    }
}
