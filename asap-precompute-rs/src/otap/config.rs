//! ASAP processor configuration and `sketch_type` dispatch.
//!
//! A single plugin parameterized by `sketch_type` with a
//! dispatch table that maps the user-facing string to:
//!
//! - The `Precompute` config knobs (`SketchType` enum + sketch_params).
//! - A [`SketchFactory`] producing fresh `Sketch` instances.
//! - A [`BoxedObserver`] that knows how to feed `ObservationValue`
//!   into the concrete sketch.
//!
//! The `sketch_type` strings (lowercase) are the canonical config
//! spellings.
//!
//! [`SketchFactory`]: crate::precompute::SketchFactory
//! [`BoxedObserver`]: crate::precompute::BoxedObserver

use std::time::Duration;

use crate::config::{PrecomputeConfig, SketchParams, WindowSpec};
use crate::envelope::SketchType;
use crate::matchers::{LabelMatcher, METRIC_NAME_LABEL};
use crate::precompute::{BoxedObserver, SketchFactory, SketchObserver};
use crate::sketches::{
    CMSHeapObserver, CMSHeapWrapper, CMSObserver, CMSWrapper, CountSketchObserver,
    CountSketchWrapper, DDSketchObserver, DDSketchWrapper, HLLObserver, HLLWrapper, KLLObserver,
    KLLWrapper,
};
use asap_sketchlib::SketchAlgorithm;

// -- Sketch-type spellings accepted in the plugin's TOML
// -- `sketch_type` field. The strings are case-insensitively matched
// -- so the plugin tolerates `DDSketch` or `ddsketch` or `Ddsketch`.

/// `sketch_type = "ddsketch"`.
pub const SKETCH_TYPE_DDSKETCH: &str = "ddsketch";
/// `sketch_type = "kll"`.
pub const SKETCH_TYPE_KLL: &str = "kll";
/// `sketch_type = "hll"`.
pub const SKETCH_TYPE_HLL: &str = "hll";
/// `sketch_type = "countsketch"`.
pub const SKETCH_TYPE_COUNTSKETCH: &str = "countsketch";
/// `sketch_type = "countminsketch"`.
pub const SKETCH_TYPE_COUNTMINSKETCH: &str = "countminsketch";
/// `sketch_type = "countminsketch_heap"`.
pub const SKETCH_TYPE_COUNTMINSKETCH_HEAP: &str = "countminsketch_heap";

/// Dispatch error when [`PluginConfig`] cannot be turned into a
/// runtime config + sketch factory + observer triple.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Sliding KLL pane/lookback configuration is invalid.
    #[error("asap_sketches: invalid sliding window: {reason}")]
    InvalidSlidingWindow {
        /// Specific validation failure.
        reason: String,
    },
    /// User-facing `sketch_type` string didn't match a supported sketch.
    #[error("asap_sketches: unsupported sketch_type {value:?} (want one of: {valid})")]
    UnknownSketchType {
        /// Raw value from config.
        value: String,
        /// Valid spellings (joined for the error message).
        valid: String,
    },

    /// A `matchers` entry is malformed or cannot apply.
    #[error("asap_sketches: invalid matcher: {reason}")]
    InvalidMatcher {
        /// Specific validation failure.
        reason: String,
    },

    /// `window_size` was zero or unparsable. Surface as a config-time
    /// error rather than a runtime drop.
    #[error("asap_sketches: window_size must be > 0 (got {value:?})")]
    InvalidWindowSize {
        /// Raw value from config (in milliseconds, post-parse).
        value: u128,
    },
}

/// Plugin-level config block. Exposes the `sketch_type` knob plus the
/// per-sketch tuning
/// parameters; the plugin unpacks this into a [`PrecomputeConfig`]
/// and constructs the right `Precompute` impl.
///
/// Phase D wires this into the OTAP submodule's TOML loader; for
/// Phase C the struct is constructed in code from test harnesses.
#[derive(Clone, Debug)]
pub struct PluginConfig {
    /// One of `"ddsketch"` / `"kll"` / `"hll"` / `"countsketch"` /
    /// `"countminsketch"` / `"countminsketch_heap"` (case-insensitive).
    /// Mandatory.
    pub sketch_type: String,
    /// Window rotation period. Mandatory.
    pub window_size: Duration,
    /// Sliding KLL emission interval/pane size; zero selects tumbling windows.
    pub window_slide: Duration,
    /// Output metric name stamped onto every emitted envelope.
    pub output_metric_name: String,
    /// Controller-plan join key.
    pub agg_id: u64,
    /// Sketch-specific tuning knobs.
    pub sketch_params: SketchParams,
    /// Optional CountSketch / CMS default key — the bytes used when
    /// an observation's `bytes` field is empty. Defaults to
    /// `output_metric_name`.
    pub default_key: Option<String>,
    /// Whether to omit resource attrs from series-key construction.
    pub omit_resource_attrs: bool,
    /// Whether every observation collapses into a single global
    /// series.
    pub global_aggregation: bool,
    /// Whether to append `sample_count` / `window_duration_seconds`
    /// to every emitted envelope's labels.
    pub emit_window_stats: bool,
    /// When `false`, emit estimated scalar results instead of sketch bytes
    /// (see [`crate::config::PrecomputeConfig::transmit_sketch`]): one Gauge
    /// per [`Self::quantiles`] entry for DDSketch / KLL, a cardinality Gauge
    /// for HLL, one Gauge per heavy hitter for CMS with heap. Defaults to
    /// `true` (transmit the ASAPv1 sketch).
    pub transmit_sketch: bool,
    /// Quantiles emitted per series when `transmit_sketch = false` and the
    /// sketch is a quantile sketch (DDSketch / KLL). Ignored otherwise.
    pub quantiles: Vec<f64>,
    /// Labels that identify a series; empty keys series by every label.
    pub aggregate_by: Vec<String>,
    /// Attribute keys sketched as their own series; `"*"` = every attribute.
    pub per_attribute: Vec<String>,
    /// Admits only points matching every matcher; `name: "__name__"`
    /// matches the metric name.
    pub matchers: Vec<LabelMatcher>,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            sketch_type: SKETCH_TYPE_KLL.to_string(),
            window_size: Duration::from_secs(10),
            window_slide: Duration::ZERO,
            output_metric_name: "asap_sketch".to_string(),
            agg_id: 0,
            sketch_params: SketchParams::new(),
            default_key: None,
            omit_resource_attrs: false,
            global_aggregation: false,
            emit_window_stats: false,
            transmit_sketch: true,
            quantiles: Vec::new(),
            aggregate_by: Vec::new(),
            per_attribute: Vec::new(),
            matchers: Vec::new(),
        }
    }
}

/// Resolved factory + observer pair for a single sketch type, plus
/// the `SketchType` enum the runtime needs.
///
/// `Debug` is implemented by hand because [`SketchFactory`] /
/// [`BoxedObserver`] are erased trait objects without `Debug`
/// bounds; the impl prints only the carrier `SketchType` so
/// `expect_err`-style test diagnostics still work.
pub struct SketchDispatch {
    /// Wire-format-aligned `SketchType` enum value the runtime stamps
    /// on every emitted envelope.
    pub sketch_type: SketchType,
    /// Constructs an empty `Sketch` instance per series.
    pub factory: SketchFactory,
    /// Routes per-observation values into the constructed sketch.
    pub observer: BoxedObserver,
}

impl std::fmt::Debug for SketchDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SketchDispatch")
            .field("sketch_type", &self.sketch_type)
            .finish_non_exhaustive()
    }
}

/// Build a [`PrecomputeConfig`] + [`SketchDispatch`] pair from a
/// [`PluginConfig`].
pub fn resolve(config: &PluginConfig) -> Result<(PrecomputeConfig, SketchDispatch), ConfigError> {
    if config.window_size.is_zero() {
        return Err(ConfigError::InvalidWindowSize {
            value: config.window_size.as_millis(),
        });
    }
    for matcher in &config.matchers {
        matcher
            .validate()
            .map_err(|reason| ConfigError::InvalidMatcher { reason })?;
        // per_attribute replaces a point's labels with `attribute.key`
        // before the runtime evaluates matchers.
        if !config.per_attribute.is_empty() && matcher.name != METRIC_NAME_LABEL {
            return Err(ConfigError::InvalidMatcher {
                reason: format!(
                    "with per_attribute only {METRIC_NAME_LABEL:?} can be matched, got {:?}",
                    matcher.name
                ),
            });
        }
    }
    let dispatch = build_dispatch(config)?;
    let sliding = !config.window_slide.is_zero();
    let pcfg = PrecomputeConfig {
        agg_id: config.agg_id,
        sketch_type: dispatch.sketch_type,
        mode: if sliding {
            crate::config::AggregationMode::Sliding
        } else {
            crate::config::AggregationMode::Tumbling
        },
        window: WindowSpec {
            size: config.window_size,
            slide: config.window_slide,
        },
        matchers: config.matchers.clone(),
        aggregate_by: config.aggregate_by.clone(),
        transmit_sketch: config.transmit_sketch,
        quantiles: config.quantiles.clone(),
        sketch_params: config.sketch_params.clone(),
        max_series: 0,
        on_overflow: crate::config::OnOverflow::Drop,
        metric_name: config.output_metric_name.clone(),
        temporality: 1, // delta
        omit_resource_attrs: config.omit_resource_attrs,
        global_aggregation: config.global_aggregation,
        emit_window_stats: config.emit_window_stats,
    };
    if sliding {
        crate::kll_windows::KllRollingPrecompute::new(pcfg.clone()).map_err(|e| {
            ConfigError::InvalidSlidingWindow {
                reason: e.to_string(),
            }
        })?;
    }
    Ok((pcfg, dispatch))
}

/// How a sketch reads a sample: its value, or the value of one label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SketchInput {
    /// The sample value (DDSketch, KLL).
    Value,
    /// One label's value, as a key (HLL, CountSketch, Count-Min).
    Label,
}

/// The bounds a sketch parameter must meet; `None` leaves that side open.
/// JSON: the bounds that are set, e.g. `{"gt": 0, "lt": 1}`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ParameterRange {
    /// Exclusive lower bound.
    pub gt: Option<f64>,
    /// Inclusive lower bound.
    pub ge: Option<f64>,
    /// Exclusive upper bound.
    pub lt: Option<f64>,
    /// Inclusive upper bound.
    pub le: Option<f64>,
}

impl ParameterRange {
    /// Whether `v` meets every bound; NaN meets none.
    pub fn contains(&self, v: f64) -> bool {
        self.gt.is_none_or(|b| v > b)
            && self.ge.is_none_or(|b| v >= b)
            && self.lt.is_none_or(|b| v < b)
            && self.le.is_none_or(|b| v <= b)
    }
}

impl serde::Serialize for ParameterRange {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let bounds = [
            ("gt", self.gt),
            ("ge", self.ge),
            ("lt", self.lt),
            ("le", self.le),
        ];
        let mut map = serializer.serialize_map(None)?;
        for (name, bound) in bounds {
            match bound {
                // Whole bounds print as integers (`1`, not `1.0`).
                Some(b) if b.fract() == 0.0 && b.abs() < 2f64.powi(53) => {
                    map.serialize_entry(name, &(b as i64))?
                }
                Some(b) => map.serialize_entry(name, &b)?,
                None => {}
            }
        }
        map.end()
    }
}

/// One size parameter of a sketch type, by its ASAPPlanner `SketchParams`
/// field name.
#[derive(Clone, Copy, Debug)]
pub struct SketchParameter {
    /// The `SketchParams` field name, e.g. `alpha` or `heap_size`.
    pub name: &'static str,
    /// The `sketch_params` key that sets it; `None` when the collector
    /// builds the sketch at one fixed value (`default`).
    pub config_key: Option<&'static str>,
    /// The value used when the key is absent or out of `range`.
    pub default: f64,
    /// The values the collector builds the sketch with.
    pub range: ParameterRange,
}

impl SketchParameter {
    /// The configured value when it is in range, otherwise `default`.
    fn value(&self, params: &crate::config::SketchParams) -> f64 {
        self.config_key
            .and_then(|key| params.get(key).copied())
            .filter(|&v| self.range.contains(v))
            .unwrap_or(self.default)
    }
}

/// One `sketch_type` the processor accepts: the processor's dispatch table
/// and the source of the sketches the collector declares
/// (`otap::capabilities`), so the two cannot drift.
#[derive(Clone, Copy)]
pub struct SketchTypeSpec {
    /// The `sketch_type` spelling (lowercase; matched case-insensitively).
    pub config_name: &'static str,
    /// The ASAPPlanner / `asap_sketchlib` name of the sketch.
    pub algorithm: SketchAlgorithm,
    /// What the sketch reads from a sample.
    pub input: SketchInput,
    /// Its size parameters, with the ranges `resolve` accepts.
    pub parameters: &'static [SketchParameter],
    build: fn(&SketchTypeSpec, &PluginConfig) -> SketchDispatch,
}

impl std::fmt::Debug for SketchTypeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SketchTypeSpec")
            .field("config_name", &self.config_name)
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl SketchTypeSpec {
    /// The value of the parameter named `name` (a `SketchParams` field
    /// name) for `config`.
    fn parameter(&self, name: &str, config: &PluginConfig) -> f64 {
        self.parameters
            .iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("{} has no parameter {name}", self.config_name))
            .value(&config.sketch_params)
    }
}

const AT_LEAST_ONE: ParameterRange = ParameterRange {
    gt: None,
    ge: Some(1.0),
    lt: None,
    le: None,
};

/// The KLL `k` range `asap_sketchlib` builds: it clamps `k` to
/// `[DEFAULT_M, MAX_K]` (`sketches::kll`, both crate-private there), so a
/// `k` outside it would build a different sketch than the one asked for.
const KLL_MIN_K: f64 = 8.0;
const KLL_MAX_K: f64 = 26_602.0;

/// The precision of the collector's HLL (`asap_sketchlib::HyperLogLog`).
const HLL_PRECISION: f64 = 14.0;

/// Every `sketch_type` the processor accepts.
pub const SKETCH_TYPES: &[SketchTypeSpec] = &[
    SketchTypeSpec {
        config_name: SKETCH_TYPE_DDSKETCH,
        algorithm: SketchAlgorithm::DDSketch,
        input: SketchInput::Value,
        parameters: &[SketchParameter {
            name: "alpha",
            config_key: Some("relative_accuracy"),
            default: 0.01,
            range: ParameterRange {
                gt: Some(0.0),
                ge: None,
                lt: Some(1.0),
                le: None,
            },
        }],
        build: |spec, config| {
            let alpha = spec.parameter("alpha", config);
            SketchDispatch {
                sketch_type: SketchType::DDSketch,
                factory: Box::new(move || Box::new(DDSketchWrapper::new(alpha))),
                observer: boxed_observer(DDSketchObserver),
            }
        },
    },
    SketchTypeSpec {
        config_name: SKETCH_TYPE_KLL,
        algorithm: SketchAlgorithm::KLL,
        input: SketchInput::Value,
        parameters: &[SketchParameter {
            name: "k",
            config_key: Some("k"),
            default: 200.0,
            range: ParameterRange {
                gt: None,
                ge: Some(KLL_MIN_K),
                lt: None,
                le: Some(KLL_MAX_K),
            },
        }],
        build: |spec, config| {
            let k = spec.parameter("k", config) as i32;
            let seed_param = crate::config::sketch_param_get(&config.sketch_params, "seed", 0.0);
            let seed = if seed_param != 0.0 {
                Some(seed_param.to_bits())
            } else {
                None
            };
            SketchDispatch {
                sketch_type: SketchType::KLLSketch,
                factory: Box::new(move || Box::new(KLLWrapper::new(k, seed))),
                observer: boxed_observer(KLLObserver),
            }
        },
    },
    SketchTypeSpec {
        config_name: SKETCH_TYPE_HLL,
        algorithm: SketchAlgorithm::HLL,
        input: SketchInput::Label,
        parameters: &[SketchParameter {
            name: "precision",
            config_key: None,
            default: HLL_PRECISION,
            range: ParameterRange {
                gt: None,
                ge: Some(HLL_PRECISION),
                lt: None,
                le: Some(HLL_PRECISION),
            },
        }],
        build: |_, _| SketchDispatch {
            sketch_type: SketchType::HLLSketch,
            factory: Box::new(|| Box::new(HLLWrapper::new())),
            observer: boxed_observer(HLLObserver),
        },
    },
    SketchTypeSpec {
        config_name: SKETCH_TYPE_COUNTSKETCH,
        algorithm: SketchAlgorithm::CountSketch,
        input: SketchInput::Label,
        parameters: &[
            SketchParameter {
                name: "width",
                config_key: Some("width"),
                default: 2048.0,
                range: AT_LEAST_ONE,
            },
            SketchParameter {
                name: "depth",
                config_key: Some("depth"),
                default: 4.0,
                range: AT_LEAST_ONE,
            },
        ],
        build: |spec, config| {
            let width = spec.parameter("width", config) as usize;
            let depth = spec.parameter("depth", config) as usize;
            let default_key = config
                .default_key
                .clone()
                .unwrap_or_else(|| config.output_metric_name.clone());
            SketchDispatch {
                sketch_type: SketchType::CountSketch,
                factory: Box::new(move || Box::new(CountSketchWrapper::new(depth, width))),
                observer: boxed_observer(CountSketchObserver { default_key }),
            }
        },
    },
    SketchTypeSpec {
        config_name: SKETCH_TYPE_COUNTMINSKETCH,
        algorithm: SketchAlgorithm::CountMinSketch,
        input: SketchInput::Label,
        parameters: &[
            SketchParameter {
                name: "width",
                config_key: Some("width"),
                default: 2048.0,
                range: AT_LEAST_ONE,
            },
            SketchParameter {
                name: "depth",
                config_key: Some("depth"),
                default: 4.0,
                range: AT_LEAST_ONE,
            },
        ],
        build: |spec, config| {
            let width = spec.parameter("width", config) as usize;
            let depth = spec.parameter("depth", config) as usize;
            SketchDispatch {
                sketch_type: SketchType::CountMinSketch,
                factory: Box::new(move || Box::new(CMSWrapper::new(depth, width))),
                observer: boxed_observer(CMSObserver),
            }
        },
    },
    SketchTypeSpec {
        config_name: SKETCH_TYPE_COUNTMINSKETCH_HEAP,
        algorithm: SketchAlgorithm::CountMinSketchWithHeap,
        input: SketchInput::Label,
        parameters: &[
            SketchParameter {
                name: "width",
                config_key: Some("columns"),
                default: 2048.0,
                range: AT_LEAST_ONE,
            },
            SketchParameter {
                name: "depth",
                config_key: Some("rows"),
                default: 4.0,
                range: AT_LEAST_ONE,
            },
            SketchParameter {
                name: "heap_size",
                config_key: Some("k"),
                default: 10.0,
                range: AT_LEAST_ONE,
            },
        ],
        build: |spec, config| {
            let width = spec.parameter("width", config) as usize;
            let depth = spec.parameter("depth", config) as usize;
            let heap_size = spec.parameter("heap_size", config) as usize;
            SketchDispatch {
                sketch_type: SketchType::CountMinSketchWithHeap,
                factory: Box::new(move || Box::new(CMSHeapWrapper::new(depth, width, heap_size))),
                observer: boxed_observer(CMSHeapObserver),
            }
        },
    },
];

/// Map `sketch_type` (case-insensitive) onto its [`SKETCH_TYPES`] row and
/// build the factory + observer + `SketchType` triple.
fn build_dispatch(config: &PluginConfig) -> Result<SketchDispatch, ConfigError> {
    let spec = SKETCH_TYPES
        .iter()
        .find(|spec| spec.config_name.eq_ignore_ascii_case(&config.sketch_type))
        .ok_or_else(|| ConfigError::UnknownSketchType {
            value: config.sketch_type.clone(),
            valid: SKETCH_TYPES
                .iter()
                .map(|spec| spec.config_name)
                .collect::<Vec<_>>()
                .join(", "),
        })?;
    Ok((spec.build)(spec, config))
}

fn boxed_observer<O: SketchObserver + 'static>(o: O) -> BoxedObserver {
    Box::new(o)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(sketch_type: &str) -> PluginConfig {
        PluginConfig {
            sketch_type: sketch_type.to_string(),
            window_size: Duration::from_secs(10),
            output_metric_name: "out".to_string(),
            agg_id: 1,
            transmit_sketch: false,
            ..Default::default()
        }
    }

    /// Every sketch type transmits its ASAPv1 sketch; the factory's sketch
    /// snapshots to a payload the processor accepts.
    #[test]
    fn every_sketch_type_transmits_sketches() {
        use crate::observation::{Observation, ObservationValue};
        for sketch_type in [
            SKETCH_TYPE_DDSKETCH,
            SKETCH_TYPE_KLL,
            SKETCH_TYPE_HLL,
            SKETCH_TYPE_COUNTSKETCH,
            SKETCH_TYPE_COUNTMINSKETCH,
            SKETCH_TYPE_COUNTMINSKETCH_HEAP,
        ] {
            let mut cfg = make_config(sketch_type);
            cfg.transmit_sketch = true;
            let (pcfg, dispatch) = resolve(&cfg).expect("resolve");
            assert!(pcfg.transmit_sketch);
            let mut sketch = (dispatch.factory)();
            let obs = Observation::new(1, "m", vec![], vec![], ObservationValue::float(1.5));
            dispatch.observer.observe(sketch.as_mut(), &obs).unwrap();
            let bytes = sketch.snapshot().unwrap();
            assert_eq!(&bytes[..6], b"ASAPv1", "{sketch_type}");
            let mut fresh = (dispatch.factory)();
            fresh.merge_snapshot(&bytes).unwrap();
        }
    }

    #[test]
    fn dispatch_handles_all_canonical_sketch_types() {
        for (in_str, expected) in [
            ("ddsketch", SketchType::DDSketch),
            ("kll", SketchType::KLLSketch),
            ("hll", SketchType::HLLSketch),
            ("countsketch", SketchType::CountSketch),
            ("countminsketch", SketchType::CountMinSketch),
            ("countminsketch_heap", SketchType::CountMinSketchWithHeap),
        ] {
            let (pcfg, dispatch) = resolve(&make_config(in_str)).expect("resolve");
            assert_eq!(dispatch.sketch_type, expected);
            assert_eq!(pcfg.sketch_type, expected);
            assert_eq!(pcfg.metric_name, "out");
            assert_eq!(pcfg.window.size, Duration::from_secs(10));
        }
    }

    #[test]
    fn dispatch_is_case_insensitive() {
        let (_, dispatch) = resolve(&make_config("DDSketch")).expect("resolve");
        assert_eq!(dispatch.sketch_type, SketchType::DDSketch);
    }

    #[test]
    fn unknown_sketch_type_rejected() {
        let err = resolve(&make_config("notarealsketch")).expect_err("should reject");
        match err {
            ConfigError::UnknownSketchType { value, .. } => assert_eq!(value, "notarealsketch"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn zero_window_rejected() {
        let mut cfg = make_config("ddsketch");
        cfg.window_size = Duration::from_secs(0);
        assert!(matches!(
            resolve(&cfg),
            Err(ConfigError::InvalidWindowSize { .. })
        ));
    }

    #[test]
    fn ddsketch_uses_relative_accuracy_param() {
        let mut cfg = make_config("ddsketch");
        cfg.sketch_params.insert("relative_accuracy".into(), 0.001);
        let (_, dispatch) = resolve(&cfg).expect("resolve");
        // We can't introspect the closure's captured alpha directly,
        // but constructing via the factory exercises the path; the
        // wrapper-side test in `sketches::ddsketch` covers correctness.
        let _ = (dispatch.factory)();
    }

    /// Every row of the table is a `sketch_type` `resolve` accepts, and
    /// each spelling and each algorithm is in it once.
    #[test]
    fn sketch_table_rows_resolve_and_are_unique() {
        for (i, spec) in SKETCH_TYPES.iter().enumerate() {
            resolve(&make_config(spec.config_name)).expect("resolve");
            for other in &SKETCH_TYPES[i + 1..] {
                assert_ne!(spec.config_name, other.config_name);
                assert_ne!(spec.algorithm, other.algorithm);
            }
            for p in spec.parameters {
                assert!(
                    p.range.contains(p.default),
                    "{}.{}",
                    spec.config_name,
                    p.name
                );
            }
        }
        let err = resolve(&make_config("nope")).expect_err("unknown");
        assert!(err
            .to_string()
            .contains("ddsketch, kll, hll, countsketch, countminsketch, countminsketch_heap"));
    }

    /// The HLL row's fixed precision is the one the wrapper builds.
    #[test]
    fn hll_row_precision_matches_the_wrapper() {
        let registers = crate::sketches::HLLWrapper::new()
            .inner()
            .registers_as_slice()
            .len();
        assert_eq!(registers, 1 << HLL_PRECISION as u32);
    }

    #[test]
    fn parameter_out_of_range_falls_back_to_default() {
        let kll = SKETCH_TYPES
            .iter()
            .find(|spec| spec.config_name == SKETCH_TYPE_KLL)
            .unwrap();
        let mut cfg = make_config(SKETCH_TYPE_KLL);
        cfg.sketch_params.insert("k".into(), 4.0);
        assert_eq!(kll.parameter("k", &cfg), 200.0);
        cfg.sketch_params.insert("k".into(), 8.0);
        assert_eq!(kll.parameter("k", &cfg), 8.0);
        let range = ParameterRange {
            gt: Some(0.0),
            lt: Some(1.0),
            ..Default::default()
        };
        assert!(!range.contains(f64::NAN) && !range.contains(0.0) && range.contains(0.5));
    }

    #[test]
    fn cms_constructs_with_default_dims() {
        let cfg = make_config("countminsketch");
        let (_, dispatch) = resolve(&cfg).expect("resolve");
        let _ = (dispatch.factory)();
    }
}
