# `asap_sketches` — OTAP-Rust unified ASAP precompute plugin

One plugin parameterized by `sketch_type` that adapts the
host-neutral ASAP precompute runtime to OTAP Dataflow's Arrow-native
streaming model.

This directory holds the user-facing configuration reference: sample
TOML and this README. The actual code — the `local::Processor<OtapPdata>`
adapter, the direct `SketchEnvelope <-> OtapPdata` codec, and the
`linkme` registration — lives in
[`asap-precompute-rs/src/otap/{processor,codec}.rs`](../../src/otap/),
behind the `otap-engine` Cargo feature. There is no separate overlay
crate or build-time staging step: `asap-precompute-rs` depends
directly on the real OTAP Dataflow crates (via git, pinned to a
specific `open-telemetry/otel-arrow` commit — see `Cargo.toml`) when
that feature is enabled, and `cargo build --features otap-engine`
builds the whole thing.

Design notes:

- This is a single unified plugin rather than five per-sketch
  plugins.
- Registered under the URN `urn:asap:processor:asap_sketches`
  (`processor.rs`'s `ASAP_SKETCHES_PROCESSOR_FACTORY`).
- The configuration shape is documented by
  [`sample.toml`](./sample.toml), and validated by
  `AsapSketchesUserConfig` (`processor.rs`).
- There is exactly one transport: whatever OTAP pipeline this node is
  wired into (`effect_handler.send_message_with_source_node`). No
  direct-TCP alternative — see `processor.rs`'s module doc, "There is
  exactly one transport: the pipeline", for why.

## Configuration

See [`sample.toml`](./sample.toml) for the canonical TOML block.
Field reference:

| Field | Required | Description |
|---|---|---|
| `sketch_type` | yes | One of `ddsketch` / `kll` / `hll` / `countsketch` / `countminsketch`. Case-insensitive. |
| `window_size` | yes | Duration string (e.g. `"10s"`). Tumbling window rotation period. |
| `output_metric_name` | yes | Metric name stamped onto every emitted envelope. |
| `agg_id` | optional | Controller-plan join key. Defaults to `0`. |
| `sketch_params.relative_accuracy` | ddsketch | DDSketch alpha (`0 < α < 1`). Default `0.01`. |
| `sketch_params.k` | kll | KLL buffer size. Default `200`. |
| `sketch_params.seed` | kll | Optional deterministic compaction seed. Default unseeded. |
| `sketch_params.precision` | hll | HLL register-count exponent (4-18). Default `12`. |
| `sketch_params.width` | countsketch, cms | Sketch column count. Default `2048`. |
| `sketch_params.depth` | countsketch, cms | Sketch row count. Default `4`. |
| `controller_url` | optional | Legacy bootstrap field, predates the engine's own OpAMP controller extension (which now owns `instance_uid`/`endpoint`/`agent_description` centrally) — likely a deprecation candidate rather than something to keep pushing new values into. |
| `agent_id` | optional | Same caveat as `controller_url`. |

## Lifecycle

`AsapSketchesProcessor` (`processor.rs`) drives a bare `Precompute`
instance directly from OTAP's own per-message/per-timer `process()`
calls — no separate Tokio-task lifecycle needed:

1. **`Message::PData`** — `codec::decode_pdata_to_observations()`
   turns the real `OtapPdata` directly into `Vec<Observation>`
   (content-routed: a data point carrying `_asap_envelope` merges as a
   pre-aggregated sketch via `Precompute::observe_envelope`; everything
   else is a plain scalar sample).
2. **`NodeControlMsg::TimerTick`** (regular flush) / **`Shutdown`**
   (final drain) — `Precompute::tick`/`drain` produce
   `SketchEnvelope`s, and `codec::encode_envelopes_to_pdata()` builds
   the real `OtapPdata` directly from them (no intermediate
   `RecordBatch` or two-batch family) before sending via
   `effect_handler.send_message_with_source_node`.
3. **`NodeControlMsg::Config`** — parses the same
   `AsapSketchesUserConfig` JSON shape used at startup and applies it
   via `Precompute::update_config`.
