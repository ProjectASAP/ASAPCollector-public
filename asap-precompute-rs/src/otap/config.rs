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
use crate::precompute::{BoxedObserver, SketchFactory, SketchObserver};
use crate::sketches::{
    CMSHeapObserver, CMSHeapWrapper, CMSObserver, CMSWrapper, CountSketchObserver,
    CountSketchWrapper, DDSketchObserver, DDSketchWrapper, HLLObserver, HLLWrapper, KLLObserver,
    KLLWrapper,
};

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
        valid: &'static str,
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
            ..Default::default()
        },
        matchers: Vec::new(),
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

/// Map `sketch_type` (case-insensitive) onto the right factory +
/// observer + `SketchType` enum value.
fn build_dispatch(config: &PluginConfig) -> Result<SketchDispatch, ConfigError> {
    let normalized = config.sketch_type.to_ascii_lowercase();
    let params = &config.sketch_params;
    match normalized.as_str() {
        SKETCH_TYPE_DDSKETCH => {
            let alpha = positive_in_unit_interval(
                crate::config::sketch_param_get(params, "relative_accuracy", 0.01),
                0.01,
            );
            Ok(SketchDispatch {
                sketch_type: SketchType::DDSketch,
                factory: Box::new(move || Box::new(DDSketchWrapper::new(alpha))),
                observer: boxed_observer(DDSketchObserver),
            })
        }
        SKETCH_TYPE_KLL => {
            let k = clamp_positive_int(crate::config::sketch_param_get(params, "k", 200.0), 200);
            let seed_param = crate::config::sketch_param_get(params, "seed", 0.0);
            let seed = if seed_param != 0.0 {
                Some(seed_param.to_bits())
            } else {
                None
            };
            Ok(SketchDispatch {
                sketch_type: SketchType::KLLSketch,
                factory: Box::new(move || Box::new(KLLWrapper::new(k as i32, seed))),
                observer: boxed_observer(KLLObserver),
            })
        }
        SKETCH_TYPE_HLL => Ok(SketchDispatch {
            sketch_type: SketchType::HLLSketch,
            factory: Box::new(|| Box::new(HLLWrapper::new())),
            observer: boxed_observer(HLLObserver),
        }),
        SKETCH_TYPE_COUNTSKETCH => {
            let depth =
                clamp_positive_int(crate::config::sketch_param_get(params, "depth", 4.0), 4);
            let width = clamp_positive_int(
                crate::config::sketch_param_get(params, "width", 2048.0),
                2048,
            );
            let default_key = config
                .default_key
                .clone()
                .unwrap_or_else(|| config.output_metric_name.clone());
            Ok(SketchDispatch {
                sketch_type: SketchType::CountSketch,
                factory: Box::new(move || {
                    Box::new(CountSketchWrapper::new(depth as usize, width as usize))
                }),
                observer: boxed_observer(CountSketchObserver { default_key }),
            })
        }
        SKETCH_TYPE_COUNTMINSKETCH => {
            let depth =
                clamp_positive_int(crate::config::sketch_param_get(params, "depth", 4.0), 4);
            let width = clamp_positive_int(
                crate::config::sketch_param_get(params, "width", 2048.0),
                2048,
            );
            Ok(SketchDispatch {
                sketch_type: SketchType::CountMinSketch,
                factory: Box::new(move || {
                    Box::new(CMSWrapper::new(depth as usize, width as usize))
                }),
                observer: boxed_observer(CMSObserver),
            })
        }
        SKETCH_TYPE_COUNTMINSKETCH_HEAP => {
            let rows = clamp_positive_int(crate::config::sketch_param_get(params, "rows", 4.0), 4);
            let columns = clamp_positive_int(
                crate::config::sketch_param_get(params, "columns", 2048.0),
                2048,
            );
            let k = clamp_positive_int(crate::config::sketch_param_get(params, "k", 10.0), 10);
            Ok(SketchDispatch {
                sketch_type: SketchType::CountMinSketchWithHeap,
                factory: Box::new(move || {
                    Box::new(CMSHeapWrapper::new(
                        rows as usize,
                        columns as usize,
                        k as usize,
                    ))
                }),
                observer: boxed_observer(CMSHeapObserver),
            })
        }
        _ => Err(ConfigError::UnknownSketchType {
            value: config.sketch_type.clone(),
            valid: "ddsketch, kll, hll, countsketch, countminsketch, countminsketch_heap",
        }),
    }
}

fn positive_in_unit_interval(v: f64, default: f64) -> f64 {
    if v > 0.0 && v < 1.0 {
        v
    } else {
        default
    }
}

fn clamp_positive_int(v: f64, default: u64) -> u64 {
    if v.is_finite() && v > 0.0 {
        v as u64
    } else {
        default
    }
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

    #[test]
    fn cms_constructs_with_default_dims() {
        let cfg = make_config("countminsketch");
        let (_, dispatch) = resolve(&cfg).expect("resolve");
        let _ = (dispatch.factory)();
    }
}
