//! `SeriesDictionary` / `SeriesDictionaryDecoder` — the Schema /
//! Dictionary / Record tiering from
//! `docs/data_model.md#schema--dictionary--record-as-entities`,
//! implemented as ASAP's own inter-node sketch-stream wire shape.
//!
//! This is deliberately a *different* codec from [`super::encode_batch`]
//! / [`super::decode_batch`] / [`super::records`], not a replacement:
//! those exist to disguise a sketch envelope as an OTAP-Metrics-shaped
//! payload (one self-contained row per envelope, `_asap_*` attributes
//! lifted onto the per-row attribute child batch) so it can transit an
//! OTAP pipeline hop that only knows how to move Logs/Metrics/Traces
//! payloads. `docs/data_model.md` is about a narrower, different hop —
//! its very first line scopes it to "sketch state cross[ing] a node or
//! network boundary between `asap_sketches` processor instances" — an
//! ASAP-aware sender talking to an ASAP-aware receiver, where there's
//! no need to *look like* a generic OTLP metric. That's the hop this
//! module implements: `SCHEMA` is sent once per distinct `agg_id`,
//! `DICTIONARY` (+ `LABELS`) once per distinct series, and `RECORD`
//! carries only what's genuinely unique per window — `series_id`,
//! window bounds, and `envelope`/`value`. No `metric` name or label is
//! ever repeated on a `RECORD` row.
//!
//! # Statefulness
//!
//! Per the doc's "Where the Schema/Dictionary statefulness guarantee
//! actually comes from": this only saves anything if the same
//! [`SeriesDictionary`] keeps encoding every batch for a given output
//! stream (so "already sent" state is meaningful), and the same
//! [`SeriesDictionaryDecoder`] keeps decoding every batch from that
//! stream in order (so retained `SCHEMA`/`DICTIONARY` state is there
//! to join against). A `RECORD` referencing a `series_id`/`agg_id`
//! the decoder never saw a `DICTIONARY`/`SCHEMA` row for is a hard
//! decode error, not a silent partial result — see
//! [`super::decode::OtapDecodeError::UnknownSeriesId`] /
//! [`OtapDecodeError::UnknownAggId`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};

use crate::config::{sketch_size_string, AggId, PrecomputeConfig};
use crate::envelope::{Encoding, SketchEnvelope, SketchType};
use crate::observation::KeyValue;

use super::decode::{parse_encoding, parse_sketch_type, OtapDecodeError};
use super::encode::OtapEncodeError;
use super::schema::{
    DICT_COLUMN_METRIC, DICT_COLUMN_SERIES_ID, LABELS_COLUMN_KEY, LABELS_COLUMN_VALUE,
    RECORD_COLUMN_ENVELOPE, RECORD_COLUMN_VALUE, RECORD_COLUMN_WINDOW_END_MS,
    RECORD_COLUMN_WINDOW_START_MS, SCHEMA_COLUMN_AGG_ID, SCHEMA_COLUMN_ENCODING,
    SCHEMA_COLUMN_HASH_FUNCTION, SCHEMA_COLUMN_HASH_SEED, SCHEMA_COLUMN_SCHEMA_VERSION,
    SCHEMA_COLUMN_SKETCH_SIZE, SCHEMA_COLUMN_SKETCH_TYPE,
};

/// The four-batch family [`SeriesDictionary::encode`] produces and
/// [`SeriesDictionaryDecoder::decode`] consumes, mirroring
/// `docs/data_model.md`'s ER diagram one-for-one.
#[derive(Debug, Clone)]
pub struct SketchStreamBatch {
    /// One row per `agg_id` first seen by the encoding
    /// [`SeriesDictionary`]. Empty (schema-only) once every live
    /// `agg_id` has already been sent.
    pub schema: RecordBatch,
    /// One row per series first seen by the encoding
    /// [`SeriesDictionary`]. Empty once every live series has already
    /// been sent.
    pub dictionary: RecordBatch,
    /// One row per label key, for series first seen this call (a
    /// child of `dictionary` — same `series_id`s, zero or more rows
    /// apiece).
    pub labels: RecordBatch,
    /// One row per envelope passed to [`SeriesDictionary::encode`],
    /// always — this is the only batch whose row count scales with
    /// observations.
    pub record: RecordBatch,
}

impl SketchStreamBatch {
    /// True when every batch in the family has zero rows.
    pub fn is_empty(&self) -> bool {
        self.schema.num_rows() == 0
            && self.dictionary.num_rows() == 0
            && self.labels.num_rows() == 0
            && self.record.num_rows() == 0
    }
}

/// Appends `s` to `buf` as a length-prefixed segment (`"<len>:<s>"`)
/// — used by [`SeriesDictionary::identity_key`] so joining several
/// caller-controlled strings can never let one segment's content be
/// mistaken for a delimiter or for the start of the next segment.
fn push_len_prefixed(buf: &mut String, s: &str) {
    buf.push_str(&s.len().to_string());
    buf.push(':');
    buf.push_str(s);
}

/// Sender-side dictionary state: assigns stable `series_id`s and
/// tracks which `agg_id`s / series have already had a `SCHEMA` /
/// `DICTIONARY` row emitted, so repeat windows for the same series
/// cost only a `RECORD` row.
///
/// One instance per **output stream** (i.e. per downstream receiver
/// this node is emitting to) — its whole value comes from persisting
/// across calls to [`Self::encode`], the same way
/// [`crate::snapshot_cache::SnapshotCache`] persists across window
/// rotations for delta encoding.
pub struct SeriesDictionary {
    next_series_id: u32,
    series_ids: HashMap<String, u32>,
    known_series: HashSet<u32>,
    known_schemas: HashSet<AggId>,
}

impl Default for SeriesDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl SeriesDictionary {
    /// Constructs an empty dictionary — nothing sent yet.
    pub fn new() -> Self {
        Self {
            next_series_id: 0,
            series_ids: HashMap::new(),
            known_series: HashSet::new(),
            known_schemas: HashSet::new(),
        }
    }

    /// Canonical series identity: `(agg_id, metric_name, labels)`,
    /// labels sorted by key. Deliberately independent of
    /// [`crate::matchers::series_key`] (which is byte-parity-locked
    /// for the snapshot-cache's unrelated purpose) — this
    /// canonicalization is private to this dictionary and never
    /// crosses a process boundary itself, only the `series_id` it
    /// produces does.
    ///
    /// Each segment is length-prefixed (`"<len>:<bytes>"`) rather than
    /// joined with a bare `|`/`=`/`;` delimiter — those separator
    /// characters are otherwise legal inside a label key/value (e.g.
    /// an HTTP path label containing `;`), and an unescaped join lets
    /// two genuinely different label sets collide onto the same
    /// string (`{"a": "1;b=2"}` vs. `{"a": "1", "b": "2"}` both used
    /// to produce `"...|a=1;b=2;"`). A length prefix makes each
    /// segment's boundary unambiguous regardless of its contents.
    fn identity_key(env: &SketchEnvelope) -> String {
        let mut labels: Vec<&KeyValue> = env.labels.iter().collect();
        labels.sort_by(|a, b| a.key.cmp(&b.key));
        let mut buf = String::new();
        push_len_prefixed(&mut buf, &env.agg_id.to_string());
        push_len_prefixed(&mut buf, &env.metric_name);
        for kv in labels {
            push_len_prefixed(&mut buf, &kv.key);
            push_len_prefixed(&mut buf, &kv.value);
        }
        buf
    }

    /// Returns `env`'s `series_id`, assigning a fresh one the first
    /// time this identity is seen.
    fn series_id_for(&mut self, env: &SketchEnvelope) -> u32 {
        let key = Self::identity_key(env);
        if let Some(id) = self.series_ids.get(&key) {
            return *id;
        }
        let id = self.next_series_id;
        self.next_series_id += 1;
        self.series_ids.insert(key, id);
        id
    }

    /// Encodes one `Precompute::tick`/`drain` call's worth of
    /// envelopes against this dictionary's accumulated state.
    ///
    /// `cfg` sources `SCHEMA`-tier facts that live on the config
    /// rather than on `SketchEnvelope` (`sketch_params`, rendered via
    /// [`sketch_size_string`]); pass `None` when unavailable — the
    /// `sketch_size` column is simply left null for that row, matching
    /// the field's optional status. Only used for envelopes whose
    /// `agg_id` matches `cfg.agg_id`; irrelevant when every envelope
    /// in `envelopes` shares one `agg_id` (the common case — see
    /// `crate::precompute::Precompute`'s "one instance owns one
    /// `agg_id`" contract).
    ///
    /// `schema` / `dictionary` / `labels` rows appear only for
    /// `agg_id`s / series not already marked known; `record` always
    /// carries one row per envelope. An empty `envelopes` slice
    /// produces four empty batches.
    pub fn encode(
        &mut self,
        envelopes: &[SketchEnvelope],
        cfg: Option<&PrecomputeConfig>,
    ) -> Result<SketchStreamBatch, OtapEncodeError> {
        let mut schema_agg_id: Vec<u64> = Vec::new();
        let mut schema_sketch_type: Vec<&'static str> = Vec::new();
        let mut schema_sketch_size: Vec<Option<String>> = Vec::new();
        let mut schema_hash_seed: Vec<Option<u64>> = Vec::new();
        let mut schema_hash_function: Vec<Option<String>> = Vec::new();
        let mut schema_encoding: Vec<&'static str> = Vec::new();
        let mut schema_version_col: Vec<u32> = Vec::new();

        let mut dict_series_id: Vec<u32> = Vec::new();
        let mut dict_agg_id: Vec<u64> = Vec::new();
        let mut dict_metric: Vec<String> = Vec::new();

        let mut labels_series_id: Vec<u32> = Vec::new();
        let mut labels_key: Vec<String> = Vec::new();
        let mut labels_value: Vec<Option<String>> = Vec::new();

        let mut rec_series_id: Vec<u32> = Vec::new();
        let mut rec_window_start: Vec<u64> = Vec::new();
        let mut rec_window_end: Vec<u64> = Vec::new();
        let mut rec_envelope: Vec<Option<Vec<u8>>> = Vec::new();
        let mut rec_value: Vec<Option<f64>> = Vec::new();

        for env in envelopes {
            if self.known_schemas.insert(env.agg_id) {
                schema_agg_id.push(env.agg_id);
                schema_sketch_type.push(env.sketch_type.name());
                schema_sketch_size.push(
                    cfg.filter(|c| c.agg_id == env.agg_id)
                        .and_then(|c| sketch_size_string(env.sketch_type, &c.sketch_params)),
                );
                let (seed, function) = resolve_hash_seed(env.sketch_type, env.hash_spec.as_ref());
                schema_hash_seed.push(seed);
                schema_hash_function.push(function);
                schema_encoding.push(env.encoding.name());
                schema_version_col.push(env.schema_version);
            }

            let series_id = self.series_id_for(env);
            if self.known_series.insert(series_id) {
                dict_series_id.push(series_id);
                dict_agg_id.push(env.agg_id);
                dict_metric.push(env.metric_name.clone());
                for kv in &env.labels {
                    labels_series_id.push(series_id);
                    labels_key.push(kv.key.clone());
                    labels_value.push(Some(kv.value.clone()));
                }
            }

            rec_series_id.push(series_id);
            rec_window_start.push(env.window_start_ms);
            rec_window_end.push(env.window_end_ms);
            // `RECORD` carries envelope bytes xor an estimate value,
            // never both — `Precompute::serialize_series` already
            // enforces non-empty payload for sketch-mode envelopes
            // (empty-payload sketch rows are dropped before reaching
            // here), so `payload.is_empty()` is an unambiguous
            // discriminator between the two modes.
            if env.payload.is_empty() {
                rec_envelope.push(None);
                rec_value.push(Some(env.value));
            } else {
                rec_envelope.push(Some(env.payload.clone()));
                rec_value.push(None);
            }
        }

        let schema = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(SCHEMA_COLUMN_AGG_ID, DataType::UInt64, false),
                Field::new(SCHEMA_COLUMN_SKETCH_TYPE, DataType::Utf8, false),
                Field::new(SCHEMA_COLUMN_SKETCH_SIZE, DataType::Utf8, true),
                Field::new(SCHEMA_COLUMN_HASH_SEED, DataType::UInt64, true),
                Field::new(SCHEMA_COLUMN_HASH_FUNCTION, DataType::Utf8, true),
                Field::new(SCHEMA_COLUMN_ENCODING, DataType::Utf8, false),
                Field::new(SCHEMA_COLUMN_SCHEMA_VERSION, DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(schema_agg_id)),
                Arc::new(StringArray::from(schema_sketch_type)),
                Arc::new(StringArray::from(schema_sketch_size)),
                Arc::new(UInt64Array::from(schema_hash_seed)),
                Arc::new(StringArray::from(schema_hash_function)),
                Arc::new(StringArray::from(schema_encoding)),
                Arc::new(UInt32Array::from(schema_version_col)),
            ],
        )
        .map_err(|e| OtapEncodeError::ArrowError(e.to_string()))?;

        let dictionary = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(DICT_COLUMN_SERIES_ID, DataType::UInt32, false),
                Field::new(SCHEMA_COLUMN_AGG_ID, DataType::UInt64, false),
                Field::new(DICT_COLUMN_METRIC, DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt32Array::from(dict_series_id)),
                Arc::new(UInt64Array::from(dict_agg_id)),
                Arc::new(StringArray::from(dict_metric)),
            ],
        )
        .map_err(|e| OtapEncodeError::ArrowError(e.to_string()))?;

        let labels = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(DICT_COLUMN_SERIES_ID, DataType::UInt32, false),
                Field::new(LABELS_COLUMN_KEY, DataType::Utf8, false),
                Field::new(LABELS_COLUMN_VALUE, DataType::Utf8, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(labels_series_id)),
                Arc::new(StringArray::from(labels_key)),
                Arc::new(StringArray::from(labels_value)),
            ],
        )
        .map_err(|e| OtapEncodeError::ArrowError(e.to_string()))?;

        let record = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(DICT_COLUMN_SERIES_ID, DataType::UInt32, false),
                Field::new(RECORD_COLUMN_WINDOW_START_MS, DataType::UInt64, false),
                Field::new(RECORD_COLUMN_WINDOW_END_MS, DataType::UInt64, false),
                Field::new(RECORD_COLUMN_ENVELOPE, DataType::Binary, true),
                Field::new(RECORD_COLUMN_VALUE, DataType::Float64, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(rec_series_id)),
                Arc::new(UInt64Array::from(rec_window_start)),
                Arc::new(UInt64Array::from(rec_window_end)),
                Arc::new(BinaryArray::from_opt_vec(
                    rec_envelope.iter().map(|o| o.as_deref()).collect(),
                )),
                Arc::new(Float64Array::from(rec_value)),
            ],
        )
        .map_err(|e| OtapEncodeError::ArrowError(e.to_string()))?;

        Ok(SketchStreamBatch {
            schema,
            dictionary,
            labels,
            record,
        })
    }
}

/// Resolves a [`asap_sketchlib::proto::sketchlib::HashSpec`] down to
/// the *one* seed this envelope's sketch actually used, plus which
/// algorithm it hashed with.
///
/// `asap_sketchlib`'s own self-describing wire format
/// (`docs/asapv1_wire_format.md`) inlines the full 20-entry
/// `seed_list` plus several per-family index fields (canonical /
/// matrix / hydra / …) so a receiver can reconstruct *any* of the
/// hasher's seeds from the bytes alone — necessary there because one
/// producer process's `HashProfile` backs several concurrently-running
/// sketch families at once. `SCHEMA_COLUMN_HASH_SEED` doesn't need
/// that generality: one `SCHEMA` row already describes exactly one
/// `agg_id`'s one `sketch_type`, so there's exactly one seed position
/// that matters — but *which* position depends on `sketch_type`.
/// `asap_sketchlib`'s matrix-family sketches (`CountSketch` /
/// `CountMinSketch`) always hash on the packed hot path via
/// `HashSpec::matrix_seed()`, i.e. `seed_list[0]`, regardless of
/// `canonical_seed_index` — `canonical_seed_index` only governs
/// `CanonicalHash`/`hh_keys` lookups, a different code path. Every
/// other sketch type here uses the canonical position,
/// `seed_list[canonical_seed_index]`. Reporting the wrong one would
/// silently mismatch a receiver's determinism/compatibility check
/// against the seed the bytes were actually hashed with.
///
/// Returns `(None, None)` when `spec` is absent (nothing upstream
/// populates [`SketchEnvelope::hash_spec`] yet) or when the resolved
/// index is out of bounds for `seed_list` (a malformed spec — better
/// to omit the seed than fabricate one).
pub(crate) fn resolve_hash_seed(
    sketch_type: SketchType,
    spec: Option<&asap_sketchlib::proto::sketchlib::HashSpec>,
) -> (Option<u64>, Option<String>) {
    let Some(spec) = spec else {
        return (None, None);
    };
    let is_matrix_family = matches!(
        sketch_type,
        SketchType::CountSketch | SketchType::CountMinSketch
    );
    let seed = if is_matrix_family {
        spec.seed_list.first().copied()
    } else {
        spec.seed_list
            .get(spec.canonical_seed_index as usize)
            .copied()
    };
    let function = asap_sketchlib::proto::sketchlib::HashAlgorithm::try_from(spec.algorithm)
        .ok()
        .map(|a| a.as_str_name().to_string());
    (seed, function)
}

#[derive(Clone)]
struct SchemaFacts {
    sketch_type: SketchType,
    encoding: Encoding,
    schema_version: u32,
    hash_seed: Option<u64>,
    hash_function: Option<String>,
}

/// Public snapshot of one `agg_id`'s retained `SCHEMA` facts, returned
/// by [`SeriesDictionaryDecoder::schema_for`]. Exists because
/// [`SketchEnvelope::hash_spec`] deliberately isn't reconstructed on
/// decode (see the comment in `build_records`) — a caller that needs
/// the resolved hash seed reads it from here instead.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaSnapshot {
    /// Which sketch algorithm this `agg_id` runs.
    pub sketch_type: SketchType,
    /// Wire layout of this `agg_id`'s `RECORD.envelope` bytes.
    pub encoding: Encoding,
    /// Wire-schema version.
    pub schema_version: u32,
    /// The one resolved canonical hash seed, if this `agg_id`'s
    /// sketch hashes at all — see `resolve_hash_seed`.
    pub hash_seed: Option<u64>,
    /// Which hash function `hash_seed` applies to (the proto
    /// `HashAlgorithm`'s canonical name), if any.
    pub hash_function: Option<String>,
}

#[derive(Clone)]
struct SeriesFacts {
    agg_id: AggId,
    metric: String,
    labels: Vec<KeyValue>,
}

/// Receiver-side mirror of [`SeriesDictionary`]'s state — retains
/// every `SCHEMA` / `DICTIONARY` / `LABELS` row it has ever seen from
/// one continuous stream, so a bare `RECORD` row (just `series_id` +
/// window + envelope/value) can be joined back into a full
/// [`SketchEnvelope`].
///
/// One instance per **input stream** (i.e. per upstream sender this
/// node is receiving from), fed every [`SketchStreamBatch`] that
/// sender's [`SeriesDictionary`] produced, in order. See the module
/// doc's "Statefulness" section for the continuity contract this
/// assumes.
#[derive(Default)]
pub struct SeriesDictionaryDecoder {
    schemas: HashMap<AggId, SchemaFacts>,
    series: HashMap<u32, SeriesFacts>,
}

impl SeriesDictionaryDecoder {
    /// Constructs a decoder with no retained state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the retained `SCHEMA` facts for `agg_id`, or `None` if
    /// this decoder has never ingested a `SCHEMA` row for it.
    pub fn schema_for(&self, agg_id: AggId) -> Option<SchemaSnapshot> {
        self.schemas.get(&agg_id).map(|f| SchemaSnapshot {
            sketch_type: f.sketch_type,
            encoding: f.encoding,
            schema_version: f.schema_version,
            hash_seed: f.hash_seed,
            hash_function: f.hash_function.clone(),
        })
    }

    /// Ingests one [`SketchStreamBatch`], updating retained
    /// `SCHEMA`/`DICTIONARY`/`LABELS` state from any new rows, and
    /// reconstructs a full [`SketchEnvelope`] for every `RECORD` row
    /// by joining back to that state (freshly-arrived or previously
    /// retained).
    ///
    /// Returns [`OtapDecodeError::UnknownSeriesId`] /
    /// [`OtapDecodeError::UnknownAggId`] if a `RECORD` (or
    /// `DICTIONARY`) row references an identity this decoder has
    /// never seen a defining row for — a continuity-contract
    /// violation rather than a value to silently paper over.
    pub fn decode(
        &mut self,
        batch: &SketchStreamBatch,
    ) -> Result<Vec<SketchEnvelope>, OtapDecodeError> {
        self.ingest_schema(&batch.schema)?;
        self.ingest_dictionary(&batch.dictionary)?;
        self.ingest_labels(&batch.labels)?;
        self.build_records(&batch.record)
    }

    fn ingest_schema(&mut self, batch: &RecordBatch) -> Result<(), OtapDecodeError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let agg_id = col_u64(batch, "schema", SCHEMA_COLUMN_AGG_ID)?;
        let sketch_type = col_str(batch, "schema", SCHEMA_COLUMN_SKETCH_TYPE)?;
        let encoding = col_str(batch, "schema", SCHEMA_COLUMN_ENCODING)?;
        let schema_version = col_u32(batch, "schema", SCHEMA_COLUMN_SCHEMA_VERSION)?;
        let hash_seed = opt_u64(batch, SCHEMA_COLUMN_HASH_SEED)?;
        let hash_function = opt_str(batch, SCHEMA_COLUMN_HASH_FUNCTION)?;
        for row in 0..batch.num_rows() {
            let id = agg_id.value(row);
            let st = parse_sketch_type(row, sketch_type.value(row))?;
            let enc = parse_encoding(row, encoding.value(row))?;
            let seed = hash_seed
                .as_ref()
                .filter(|arr| !arr.is_null(row))
                .map(|arr| arr.value(row));
            let function = hash_function
                .as_ref()
                .filter(|arr| !arr.is_null(row))
                .map(|arr| arr.value(row).to_string());
            self.schemas.insert(
                id,
                SchemaFacts {
                    sketch_type: st,
                    encoding: enc,
                    schema_version: schema_version.value(row),
                    hash_seed: seed,
                    hash_function: function,
                },
            );
        }
        Ok(())
    }

    fn ingest_dictionary(&mut self, batch: &RecordBatch) -> Result<(), OtapDecodeError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let series_id = col_u32(batch, "dictionary", DICT_COLUMN_SERIES_ID)?;
        let agg_id = col_u64(batch, "dictionary", SCHEMA_COLUMN_AGG_ID)?;
        let metric = col_str(batch, "dictionary", DICT_COLUMN_METRIC)?;
        for row in 0..batch.num_rows() {
            let sid = series_id.value(row);
            // A DICTIONARY row for a series_id is only ever supposed
            // to arrive once, paired with LABELS rows in that same
            // batch (`SeriesDictionary::encode` never re-emits either
            // for a series it already considers known). If one
            // arrives for a series_id this decoder already has facts
            // for — a duplicate/replayed batch, or a sender that lost
            // its own dictionary state and resent it — preserve the
            // labels already learned instead of resetting them to
            // empty and hoping a fresh LABELS batch refills them
            // (which `SeriesDictionary` won't send, since it still
            // considers this series known).
            self.series
                .entry(sid)
                .and_modify(|facts| {
                    facts.agg_id = agg_id.value(row);
                    facts.metric = metric.value(row).to_string();
                })
                .or_insert_with(|| SeriesFacts {
                    agg_id: agg_id.value(row),
                    metric: metric.value(row).to_string(),
                    labels: Vec::new(),
                });
        }
        Ok(())
    }

    fn ingest_labels(&mut self, batch: &RecordBatch) -> Result<(), OtapDecodeError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let series_id = col_u32(batch, "labels", DICT_COLUMN_SERIES_ID)?;
        let key = col_str(batch, "labels", LABELS_COLUMN_KEY)?;
        let value = opt_str(batch, LABELS_COLUMN_VALUE)?;
        for row in 0..batch.num_rows() {
            let sid = series_id.value(row);
            let v = value
                .as_ref()
                .filter(|arr| !arr.is_null(row))
                .map(|arr| arr.value(row).to_string())
                .unwrap_or_default();
            // A LABELS row for a series_id this batch's own
            // DICTIONARY didn't define (and no earlier batch did
            // either) is dropped here; build_records still fails
            // loudly for that series_id since it never resolves.
            if let Some(entry) = self.series.get_mut(&sid) {
                entry
                    .labels
                    .push(KeyValue::new(key.value(row).to_string(), v));
            }
        }
        Ok(())
    }

    fn build_records(&self, batch: &RecordBatch) -> Result<Vec<SketchEnvelope>, OtapDecodeError> {
        let series_id = col_u32(batch, "record", DICT_COLUMN_SERIES_ID)?;
        let window_start = col_u64(batch, "record", RECORD_COLUMN_WINDOW_START_MS)?;
        let window_end = col_u64(batch, "record", RECORD_COLUMN_WINDOW_END_MS)?;
        let envelope = opt_binary(batch, RECORD_COLUMN_ENVELOPE)?;
        let value = opt_f64(batch, RECORD_COLUMN_VALUE)?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let sid = series_id.value(row);
            let series = self
                .series
                .get(&sid)
                .ok_or(OtapDecodeError::UnknownSeriesId { series_id: sid })?;
            let schema = self
                .schemas
                .get(&series.agg_id)
                .ok_or(OtapDecodeError::UnknownAggId {
                    agg_id: series.agg_id,
                })?;

            let payload = envelope
                .as_ref()
                .filter(|arr| !arr.is_null(row))
                .map(|arr| arr.value(row).to_vec())
                .unwrap_or_default();
            let est_value = value
                .as_ref()
                .filter(|arr| !arr.is_null(row))
                .map(|arr| arr.value(row))
                .unwrap_or(0.0);

            out.push(SketchEnvelope {
                schema_version: schema.schema_version,
                sketch_type: schema.sketch_type,
                agg_id: series.agg_id,
                resource_labels: Vec::new(),
                labels: series.labels.clone(),
                window_start_ms: window_start.value(row),
                window_end_ms: window_end.value(row),
                encoding: schema.encoding,
                payload,
                // Deliberately not reconstructed: `resolve_hash_seed`
                // only carries the one resolved canonical seed across
                // the wire, not `asap_sketchlib`'s full `HashSpec`
                // (algorithm + 20-entry seed_list + seed_derivation) —
                // synthesizing a fake one-entry `HashSpec` here would
                // be more misleading than omitting it. A receiver that
                // needs the resolved seed calls
                // `Self::schema_for(series.agg_id)` instead of
                // expecting it to round-trip through this field.
                hash_spec: None,
                metric_name: series.metric.clone(),
                count: 0,
                aggregation_temporality: 0,
                value: est_value,
            });
        }
        Ok(out)
    }
}

// -- Small typed-column accessors -------------------------------------------
//
// Deliberately local rather than shared with `records.rs` / `decode.rs`'s
// own downcast helpers — the batch shapes here are simple and fixed, and
// duplicating a handful of one-line downcasts is cheaper to read than a
// shared generic accessor would be.

fn col_u64<'a>(
    batch: &'a RecordBatch,
    label: &'static str,
    name: &'static str,
) -> Result<&'a UInt64Array, OtapDecodeError> {
    let col = batch
        .column_by_name(name)
        .ok_or(OtapDecodeError::MissingColumn {
            batch: label,
            column: name,
        })?;
    col.as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "UInt64",
            actual: col.data_type().clone(),
        })
}

fn col_u32<'a>(
    batch: &'a RecordBatch,
    label: &'static str,
    name: &'static str,
) -> Result<&'a UInt32Array, OtapDecodeError> {
    let col = batch
        .column_by_name(name)
        .ok_or(OtapDecodeError::MissingColumn {
            batch: label,
            column: name,
        })?;
    col.as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "UInt32",
            actual: col.data_type().clone(),
        })
}

fn col_str<'a>(
    batch: &'a RecordBatch,
    label: &'static str,
    name: &'static str,
) -> Result<&'a StringArray, OtapDecodeError> {
    let col = batch
        .column_by_name(name)
        .ok_or(OtapDecodeError::MissingColumn {
            batch: label,
            column: name,
        })?;
    col.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "Utf8",
            actual: col.data_type().clone(),
        })
}

fn opt_str<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<Option<&'a StringArray>, OtapDecodeError> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => Ok(Some(
            col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                OtapDecodeError::WrongColumnType {
                    column: name.to_string(),
                    expected: "Utf8",
                    actual: col.data_type().clone(),
                }
            })?,
        )),
    }
}

fn opt_u64<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<Option<&'a UInt64Array>, OtapDecodeError> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => Ok(Some(
            col.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                OtapDecodeError::WrongColumnType {
                    column: name.to_string(),
                    expected: "UInt64",
                    actual: col.data_type().clone(),
                }
            })?,
        )),
    }
}

fn opt_binary<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<Option<&'a BinaryArray>, OtapDecodeError> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => Ok(Some(
            col.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                OtapDecodeError::WrongColumnType {
                    column: name.to_string(),
                    expected: "Binary",
                    actual: col.data_type().clone(),
                }
            })?,
        )),
    }
}

fn opt_f64<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<Option<&'a Float64Array>, OtapDecodeError> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(col) => Ok(Some(
            col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                OtapDecodeError::WrongColumnType {
                    column: name.to_string(),
                    expected: "Float64",
                    actual: col.data_type().clone(),
                }
            })?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::KeyValue;

    fn envelope(
        agg_id: u64,
        metric: &str,
        labels: Vec<KeyValue>,
        window: [u64; 2],
    ) -> SketchEnvelope {
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::DDSketch,
            agg_id,
            resource_labels: Vec::new(),
            labels,
            window_start_ms: window[0],
            window_end_ms: window[1],
            encoding: Encoding::ProtoFull,
            payload: vec![1, 2, 3, 4],
            hash_spec: None,
            metric_name: metric.to_string(),
            count: 10,
            aggregation_temporality: 1,
            value: 0.0,
        }
    }

    #[test]
    fn first_window_emits_schema_dictionary_labels_record() {
        let mut dict = SeriesDictionary::new();
        let env = envelope(
            7,
            "http_request_duration",
            vec![
                KeyValue::new("path", "/api"),
                KeyValue::new("region", "us-east"),
            ],
            [1_000, 11_000],
        );
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");

        assert_eq!(batch.schema.num_rows(), 1, "new agg_id gets a SCHEMA row");
        assert_eq!(
            batch.dictionary.num_rows(),
            1,
            "new series gets a DICTIONARY row"
        );
        assert_eq!(
            batch.labels.num_rows(),
            2,
            "two label keys on the new series"
        );
        assert_eq!(batch.record.num_rows(), 1, "one RECORD row per envelope");
        assert!(!batch.is_empty());
    }

    #[test]
    fn repeat_window_for_same_series_emits_record_only() {
        let mut dict = SeriesDictionary::new();
        let env1 = envelope(
            7,
            "http_request_duration",
            vec![KeyValue::new("path", "/api")],
            [1_000, 11_000],
        );
        let _ = dict
            .encode(std::slice::from_ref(&env1), None)
            .expect("first window");

        // Same series (same agg_id/metric/labels), next window.
        let env2 = envelope(
            7,
            "http_request_duration",
            vec![KeyValue::new("path", "/api")],
            [11_000, 21_000],
        );
        let batch2 = dict
            .encode(std::slice::from_ref(&env2), None)
            .expect("second window");

        assert_eq!(
            batch2.schema.num_rows(),
            0,
            "agg_id already known — no SCHEMA row"
        );
        assert_eq!(
            batch2.dictionary.num_rows(),
            0,
            "series already known — no DICTIONARY row"
        );
        assert_eq!(
            batch2.labels.num_rows(),
            0,
            "series already known — no LABELS rows"
        );
        assert_eq!(
            batch2.record.num_rows(),
            1,
            "RECORD is still emitted every window"
        );

        // The series_id assigned in window 1 is reused in window 2.
        let record_series_id = batch2
            .record
            .column_by_name(DICT_COLUMN_SERIES_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0);
        assert_eq!(record_series_id, 0);
    }

    #[test]
    fn distinct_label_combinations_get_distinct_series_ids() {
        let mut dict = SeriesDictionary::new();
        let envs = vec![
            envelope(7, "m", vec![KeyValue::new("path", "/api")], [0, 10]),
            envelope(7, "m", vec![KeyValue::new("path", "/login")], [0, 10]),
        ];
        let batch = dict.encode(&envs, None).expect("encode");
        assert_eq!(batch.dictionary.num_rows(), 2, "two distinct series");
        let ids = batch
            .dictionary
            .column_by_name(DICT_COLUMN_SERIES_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_ne!(ids.value(0), ids.value(1));
    }

    #[test]
    fn empty_envelopes_produce_four_empty_batches() {
        let mut dict = SeriesDictionary::new();
        let batch = dict.encode(&[], None).expect("encode empty");
        assert!(batch.is_empty());
    }

    #[test]
    fn hash_seed_resolves_to_the_one_canonical_position() {
        use asap_sketchlib::proto::sketchlib::{HashAlgorithm, HashSpec, SeedDerivation};

        let spec = HashSpec {
            algorithm: HashAlgorithm::Xxh364 as i32,
            canonical_seed_index: 5,
            seed_list: (0..20).map(|i| 1000 + i as u64).collect(),
            seed_derivation: SeedDerivation::AdditiveOffset as i32,
        };
        let mut dict = SeriesDictionary::new();
        let env = SketchEnvelope {
            hash_spec: Some(spec),
            sketch_type: SketchType::HLLSketch,
            ..envelope(7, "m", vec![], [0, 10])
        };
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");

        let seeds = batch
            .schema
            .column_by_name(SCHEMA_COLUMN_HASH_SEED)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        // canonical_seed_index = 5 -> seed_list[5] = 1005, NOT the
        // whole 20-entry table.
        assert_eq!(seeds.value(0), 1005);

        let functions = batch
            .schema
            .column_by_name(SCHEMA_COLUMN_HASH_FUNCTION)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(functions.value(0), "HASH_ALGORITHM_XXH3_64");
    }

    #[test]
    fn decoder_exposes_resolved_hash_seed_via_schema_for() {
        use asap_sketchlib::proto::sketchlib::{HashAlgorithm, HashSpec, SeedDerivation};

        let spec = HashSpec {
            algorithm: HashAlgorithm::Xxh364 as i32,
            canonical_seed_index: 5,
            seed_list: (0..20).map(|i| 1000 + i as u64).collect(),
            seed_derivation: SeedDerivation::AdditiveOffset as i32,
        };
        let mut dict = SeriesDictionary::new();
        let mut decoder = SeriesDictionaryDecoder::new();
        let env = SketchEnvelope {
            hash_spec: Some(spec),
            sketch_type: SketchType::HLLSketch,
            ..envelope(7, "m", vec![], [0, 10])
        };
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");
        let decoded = decoder.decode(&batch).expect("decode");

        // The reconstructed envelope itself doesn't carry a
        // (necessarily lossy) HashSpec back...
        assert!(decoded[0].hash_spec.is_none());
        // ...but the decoder retains the resolved seed for direct
        // lookup by agg_id.
        let schema = decoder.schema_for(7).expect("schema retained");
        assert_eq!(schema.hash_seed, Some(1005));
        assert_eq!(
            schema.hash_function.as_deref(),
            Some("HASH_ALGORITHM_XXH3_64")
        );
        assert_eq!(schema.sketch_type, SketchType::HLLSketch);
    }

    #[test]
    fn hash_seed_is_null_without_a_hash_spec() {
        let mut dict = SeriesDictionary::new();
        let env = envelope(7, "m", vec![], [0, 10]); // hash_spec: None (default)
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");
        let seeds = batch
            .schema
            .column_by_name(SCHEMA_COLUMN_HASH_SEED)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(seeds.is_null(0));
    }

    #[test]
    fn sketch_size_is_populated_from_config_when_agg_id_matches() {
        use crate::config::{PrecomputeConfig, SketchParams};
        let mut params = SketchParams::new();
        params.insert("relative_accuracy".into(), 0.01);
        let cfg = PrecomputeConfig {
            agg_id: 7,
            sketch_params: params,
            ..Default::default()
        };
        let mut dict = SeriesDictionary::new();
        let env = envelope(7, "m", vec![], [0, 10]);
        let batch = dict
            .encode(std::slice::from_ref(&env), Some(&cfg))
            .expect("encode");
        let sizes = batch
            .schema
            .column_by_name(SCHEMA_COLUMN_SKETCH_SIZE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sizes.value(0), "0.01");
    }

    #[test]
    fn round_trip_two_windows_preserves_envelopes() {
        let mut dict = SeriesDictionary::new();
        let mut decoder = SeriesDictionaryDecoder::new();

        let env1 = envelope(
            7,
            "http_request_duration",
            vec![
                KeyValue::new("path", "/api"),
                KeyValue::new("region", "us-east"),
            ],
            [1_000, 11_000],
        );
        let batch1 = dict
            .encode(std::slice::from_ref(&env1), None)
            .expect("encode 1");
        let decoded1 = decoder.decode(&batch1).expect("decode 1");
        assert_eq!(decoded1.len(), 1);
        assert_eq!(decoded1[0].payload, env1.payload);
        assert_eq!(decoded1[0].metric_name, env1.metric_name);
        assert_eq!(decoded1[0].labels, env1.labels);
        assert_eq!(decoded1[0].sketch_type, env1.sketch_type);
        assert_eq!(decoded1[0].window_start_ms, env1.window_start_ms);
        assert_eq!(decoded1[0].window_end_ms, env1.window_end_ms);

        // Second window: sender's DICTIONARY/SCHEMA batches are empty
        // (already known), so the encoded bytes genuinely shrink — but
        // the decoder must still reconstruct the full envelope by
        // joining the bare RECORD row against retained state.
        let env2 = envelope(
            7,
            "http_request_duration",
            vec![
                KeyValue::new("path", "/api"),
                KeyValue::new("region", "us-east"),
            ],
            [11_000, 21_000],
        );
        let batch2 = dict
            .encode(std::slice::from_ref(&env2), None)
            .expect("encode 2");
        assert!(batch2.schema.num_rows() == 0 && batch2.dictionary.num_rows() == 0);
        let decoded2 = decoder.decode(&batch2).expect("decode 2");
        assert_eq!(decoded2.len(), 1);
        assert_eq!(decoded2[0].labels, env2.labels);
        assert_eq!(decoded2[0].metric_name, env2.metric_name);
        assert_eq!(decoded2[0].window_start_ms, 11_000);
        assert_eq!(decoded2[0].window_end_ms, 21_000);
    }

    #[test]
    fn decode_unknown_series_id_is_a_hard_error() {
        // A RECORD batch referencing a series_id with no prior
        // DICTIONARY row (fresh decoder, no schema/dictionary/labels
        // ingested first) must fail loudly, not synthesize a
        // half-empty envelope.
        let mut dict = SeriesDictionary::new();
        let env = envelope(7, "m", vec![], [0, 10]);
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");

        // Fresh decoder that never saw batch's SCHEMA/DICTIONARY rows
        // — simulate by decoding only the RECORD-bearing part via a
        // hand-built batch with empty schema/dictionary/labels.
        let empty_schema = batch.schema.slice(0, 0);
        let empty_dict = batch.dictionary.slice(0, 0);
        let empty_labels = batch.labels.slice(0, 0);
        let record_only = SketchStreamBatch {
            schema: empty_schema,
            dictionary: empty_dict,
            labels: empty_labels,
            record: batch.record,
        };
        let mut decoder = SeriesDictionaryDecoder::new();
        let err = decoder.decode(&record_only).expect_err("should fail");
        assert!(matches!(
            err,
            OtapDecodeError::UnknownSeriesId { series_id: 0 }
        ));
    }
}
