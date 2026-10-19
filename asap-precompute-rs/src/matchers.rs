//! [`LabelMatcher`] and series-key helpers.
//!
//! The type surface is final. The exact byte layout of
//! [`series_key`] / [`attributes_key`] is locked by the byte-parity
//! tests.

use crate::config::AggId;
use crate::observation::{KeyValue, Observation};

/// Picks the comparison operator for a [`LabelMatcher`].
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum MatchOp {
    /// Requires `v == matcher.value`.
    #[default]
    Equal,
    /// Requires `v != matcher.value`.
    NotEqual,
}

impl MatchOp {
    /// Returns the operator's debug name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
        }
    }
}

/// Selects observations by metric name (`name == ""`) or label key
/// (`name != ""`).
///
/// Today's per-processor `LabelMatchers` config uses `(key, value)`
/// equality only; the `op` field is forward-compat for regex / glob.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct LabelMatcher {
    /// Label key to match against, or the empty string to match the
    /// metric-name field.
    pub name: String,
    /// Exact target value.
    pub value: String,
    /// Picks `Equal` vs `NotEqual`.
    pub op: MatchOp,
}

impl LabelMatcher {
    /// Constructs a new equality matcher. Convenience constructor;
    /// callers that want `NotEqual` build the struct literal.
    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            op: MatchOp::Equal,
        }
    }

    /// Returns [`MatchOp::Equal`] — the default operator.
    ///
    /// Spelled out as a function (rather than only relying on
    /// `<MatchOp as Default>::default()`) so callers and tests can
    /// reference the canonical default explicitly.
    pub fn default_op() -> MatchOp {
        MatchOp::Equal
    }

    /// Returns whether the observation satisfies all matchers.
    ///
    /// Empty matcher list returns `true`. For each matcher:
    ///   - For `Equal`: missing key fails the match. Mismatched value
    ///     fails. Match-equal-empty-string against missing key
    ///     behaves like the existing OTel processor: missing key
    ///     returns false.
    ///   - For `NotEqual`: missing key passes (no value to disagree
    ///     with); present-and-equal fails.
    pub fn matches_all(matchers: &[LabelMatcher], obs: &Observation) -> bool {
        if matchers.is_empty() {
            return true;
        }
        for m in matchers {
            let (val, present) = if m.name.is_empty() {
                (obs.metric.as_str(), !obs.metric.is_empty())
            } else {
                lookup_label(&obs.labels, &m.name)
                    .map(|v| (v, true))
                    .unwrap_or(("", false))
            };
            match m.op {
                MatchOp::Equal => {
                    if !present || val != m.value {
                        return false;
                    }
                }
                MatchOp::NotEqual => {
                    if present && val == m.value {
                        return false;
                    }
                }
            }
        }
        true
    }
}

/// Returns the value for `key` in `labels`. Linear scan; label sets
/// are small (typically < 10) so this beats building a map for
/// ephemeral matching.
pub(crate) fn lookup_label<'a>(labels: &'a [KeyValue], key: &str) -> Option<&'a str> {
    labels
        .iter()
        .find(|kv| kv.key == key)
        .map(|kv| kv.value.as_str())
}

/// Produces a stable string identifying the `(agg_id, label_key)`
/// series for storage and snapshot lookup.
///
/// Output format MUST stay byte-identical to today's per-processor
/// `seriesKey` / `attributesKey` output for the same input — that's
/// how the snapshot cache survives the refactor.
///
/// Today's format:
///
/// ```text
/// with aggregateBy:    "k1=v1;k2=v2;"  for the listed keys, missing keys skipped
/// without aggregateBy: "k1=v1;k2=v2;"  for ALL labels, sorted by key
/// ```
///
/// The `agg_id` is prepended as a stable per-Precompute prefix so
/// the same global series-key namespace can hold series from
/// multiple Precompute instances.
pub fn series_key(
    agg_id: AggId,
    resource_labels: &[KeyValue],
    labels: &[KeyValue],
    aggregate_by: &[String],
) -> String {
    let mut buf = String::new();
    write_series_key(&mut buf, agg_id, resource_labels, labels, aggregate_by);
    buf
}

/// Writes [`series_key`] into `buf`, replacing its contents, so a hot path
/// can reuse one buffer.
pub fn write_series_key(
    buf: &mut String,
    agg_id: AggId,
    resource_labels: &[KeyValue],
    labels: &[KeyValue],
    aggregate_by: &[String],
) {
    use std::fmt::Write;
    buf.clear();
    let _ = write!(buf, "{agg_id}");
    buf.push('|');
    write_attributes_key(buf, resource_labels, &[]);
    buf.push('|');
    write_attributes_key(buf, labels, aggregate_by);
}

/// Returns just the per-label-set portion of [`series_key`].
///
/// When `aggregate_by` is
/// empty, all labels are included sorted by key; otherwise only the
/// listed keys are included in their `aggregate_by` order. Missing
/// keys are skipped (no `k=;` placeholder), matching today's
/// behavior.
pub fn attributes_key(labels: &[KeyValue], aggregate_by: &[String]) -> String {
    let mut buf = String::new();
    write_attributes_key(&mut buf, labels, aggregate_by);
    buf
}

fn write_attributes_key(buf: &mut String, labels: &[KeyValue], aggregate_by: &[String]) {
    if aggregate_by.is_empty() {
        // Already sorted with unique keys (e.g. one label): same output as
        // the sorted walk below, without collecting the keys.
        if labels.windows(2).all(|w| w[0].key < w[1].key) {
            for kv in labels {
                buf.push_str(&kv.key);
                buf.push('=');
                buf.push_str(&kv.value);
                buf.push(';');
            }
            return;
        }
        // Sort by key for stable output.
        let mut keys: Vec<&str> = labels.iter().map(|kv| kv.key.as_str()).collect();
        keys.sort();
        for k in keys {
            if let Some(v) = lookup_label(labels, k) {
                buf.push_str(k);
                buf.push('=');
                buf.push_str(v);
                buf.push(';');
            }
        }
        return;
    }
    // aggregate_by is already sorted by config validation; we
    // preserve the caller's order rather than re-sort, so legacy
    // callers that sort upstream get bit-identical keys.
    for k in aggregate_by {
        if let Some(v) = lookup_label(labels, k) {
            buf.push_str(k);
            buf.push('=');
            buf.push_str(v);
            buf.push(';');
        }
    }
}

/// Filters `labels` to only those in `aggregate_by`, preserving
/// `aggregate_by` order.
///
/// When `aggregate_by` is
/// empty, returns a copy of all labels; otherwise returns only the
/// listed-and-present ones.
pub fn series_attrs(labels: &[KeyValue], aggregate_by: &[String]) -> Vec<KeyValue> {
    if aggregate_by.is_empty() {
        return labels.to_vec();
    }
    let mut out = Vec::with_capacity(aggregate_by.len());
    for k in aggregate_by {
        if let Some(v) = lookup_label(labels, k) {
            out.push(KeyValue::new(k.clone(), v.to_string()));
        }
    }
    out
}

/// Label carrying the attribute key in per-attribute series.
pub const ATTRIBUTE_KEY_LABEL: &str = "attribute.key";

/// Splits one data point into one observation per attribute key in `keys`
/// (`"*"` = every attribute it has): labels `attribute.key=<key>`, value =
/// the attribute's value as bytes. A cardinality or frequency sketch over
/// these series answers "how many distinct values" and "which values
/// dominate" per attribute key.
///
/// Each split is written into `scratch` and passed to `f`, so a stream of
/// points reuses one observation's buffers instead of allocating per point.
pub fn per_attribute<E>(
    obs: &Observation,
    keys: &[String],
    scratch: &mut Observation,
    mut f: impl FnMut(&Observation) -> Result<(), E>,
) -> Result<(), E> {
    let all = keys.iter().any(|k| k == "*");
    for kv in &obs.labels {
        if !all && !keys.contains(&kv.key) {
            continue;
        }
        scratch.timestamp_ms = obs.timestamp_ms;
        scratch.metric.clone_from(&obs.metric);
        scratch.resource_labels.clone_from(&obs.resource_labels);
        scratch.labels.resize_with(1, KeyValue::default);
        scratch.labels[0].key.clear();
        scratch.labels[0].key.push_str(ATTRIBUTE_KEY_LABEL);
        scratch.labels[0].value.clone_from(&kv.key);
        let value = &mut scratch.value;
        value.kind = crate::observation::ObservationValueKind::Bytes;
        value.envelope = None;
        value.bytes.clear();
        value.bytes.extend_from_slice(kv.value.as_bytes());
        f(scratch)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::ObservationValue;

    #[test]
    fn default_op_returns_equal() {
        assert_eq!(LabelMatcher::default_op(), MatchOp::Equal);
        assert_eq!(MatchOp::default(), MatchOp::Equal);
    }

    #[test]
    fn empty_matchers_admit_everything() {
        let obs = Observation::new(0, "m", vec![], vec![], ObservationValue::float(0.0));
        assert!(LabelMatcher::matches_all(&[], &obs));
    }

    #[test]
    fn equal_matcher_rejects_missing_label() {
        let obs = Observation::new(0, "m", vec![], vec![], ObservationValue::float(0.0));
        let matchers = vec![LabelMatcher::equal("region", "us-east")];
        assert!(!LabelMatcher::matches_all(&matchers, &obs));
    }

    #[test]
    fn equal_matcher_admits_matching_value() {
        let obs = Observation::new(
            0,
            "m",
            vec![],
            vec![KeyValue::new("region", "us-east")],
            ObservationValue::float(0.0),
        );
        let matchers = vec![LabelMatcher::equal("region", "us-east")];
        assert!(LabelMatcher::matches_all(&matchers, &obs));
    }

    #[test]
    fn sorted_fast_path_writes_the_same_key_as_the_sorted_walk() {
        let sorted = vec![KeyValue::new("a", "1"), KeyValue::new("b", "2")];
        let unsorted = vec![KeyValue::new("b", "2"), KeyValue::new("a", "1")];
        assert_eq!(attributes_key(&sorted, &[]), "a=1;b=2;");
        assert_eq!(attributes_key(&unsorted, &[]), "a=1;b=2;");
        let mut buf = "stale".to_string();
        write_series_key(&mut buf, 7, &sorted, &unsorted, &[]);
        assert_eq!(buf, series_key(7, &sorted, &unsorted, &[]));
    }

    #[test]
    fn per_attribute_reuses_the_scratch_for_each_selected_key() {
        let obs = Observation::new(
            5,
            "m",
            vec![KeyValue::new("service", "s")],
            vec![KeyValue::new("route", "/a"), KeyValue::new("user", "u1")],
            ObservationValue::float(1.0),
        );
        let mut scratch = Observation::default();
        let mut seen = Vec::new();
        per_attribute::<()>(&obs, &["*".into()], &mut scratch, |o| {
            seen.push(o.clone());
            Ok(())
        })
        .unwrap();
        let attr = |key: &str, value: &str| {
            Observation::new(
                5,
                "m",
                vec![KeyValue::new("service", "s")],
                vec![KeyValue::new(ATTRIBUTE_KEY_LABEL, key)],
                ObservationValue::bytes(value.as_bytes().to_vec()),
            )
        };
        assert_eq!(seen, vec![attr("route", "/a"), attr("user", "u1")]);
    }
}
