# `asap_sketches` — OTAP-Rust unified ASAP precompute plugin

The OTAP-Rust mirror of Telegraf's `processors.allsketches` and
`asap-otelcol`'s legacy per-sketch processors: one plugin
parameterized by `sketch_type` that adapts the host-neutral
`asap-precompute-rs` runtime to OTAP Dataflow's Arrow-native
streaming model.

This directory holds the **plugin shell** scaffolding: sample TOML,
this README, and a future `Cargo.toml` / `src/lib.rs` once Phase D
wires the OTAP submodule's plugin registry. The actual lifecycle —
input stream consumption, `Wakeup`-driven flush, control-channel
poll, graceful drain — lives upstream in
[`asap-precompute-rs/src/otap/`](../../../asap-precompute-rs/src/otap/),
gated by the `otap` feature.

See [`docs/design-asap-otap-rust-integration.md`](../../../docs/design-asap-otap-rust-integration.md)
for the design rationale; in particular:

- §3 explains why this is a single unified plugin rather than five
  per-sketch plugins.
- §5 specifies the lifecycle contract: `linkme` slice registration,
  async `Stream<RecordBatch>` consumption, `NodeControlMsg::Wakeup`
  scheduled flush, graceful-shutdown drain.
- §6 calls out this directory layout as the target shape.
- §8 documents the configuration shape that
  [`sample.toml`](./sample.toml) demonstrates.

## Phase status

- **Phase B** (PR #256) — codec (`decode_batch` / `encode_batch`)
  shipped.
- **Phase C** (this PR) — full plugin lifecycle + 5-sketch
  dispatch live in
  [`asap-precompute-rs/src/otap/{config,lifecycle,records}.rs`](../../../asap-precompute-rs/src/otap/).
  The plugin shell here documents the user-facing configuration;
  the binary-level `linkme` registration is **deliberately
  deferred** to Phase D per the §11 phase plan.
- **Phase D** (next) — `build_asap_otap.sh` build script and
  `otap-patch/all/mod.rs` `linkme` distributed-slice registration.
  Phase D adds `Cargo.toml` + `src/lib.rs` here once the OTAP
  submodule binding is wired in.
- **Phase E** (optional) — cross-host envelope parity, gated on
  cross-language byte-parity (issue #243).

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
| `controller_url` | optional | Bootstrap controller address for the `HttpPollChannel`. |
| `agent_id` | optional | Agent identifier reported to the controller. |

## Lifecycle

The plugin owns three async tasks (per
[design doc §5](../../../docs/design-asap-otap-rust-integration.md#5-plugin-lifecycle--otap-receiver--processor)):

1. **Input task** — consumes the host-supplied
   `Stream<OtapMetricRecords>`. For each batch:
   `flatten()` → `decode_batch()` → `Precompute::observe()`.
2. **Flush ticker** — Tokio `interval(window_size)` modeled on
   `NodeControlMsg::Wakeup`. On each tick: `Precompute::tick()` →
   `encode_batch()` → `lift()` → emit a synthesized
   `OtapMetricRecords` family. `lift()` is the Strategy-B
   attribute-lift step that places `_asap_*` keys onto the per-row
   attribute child batch (so the resulting batch passes OTAP's
   strict schema validator at
   `crates/pdata/src/schema/payloads.rs::check_match`).
3. **Control-channel task** — polls the `ControlChannel` impl
   (`HttpPollChannel` in production) and applies plan changes via
   `Precompute::update_config`.

Graceful shutdown drains the active window once before exit so
shutting down before the natural window boundary doesn't drop
in-flight observations.
