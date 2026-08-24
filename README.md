# ASAPCollector — OTAP Dataflow processor 

This repository is a **standalone, buildable snapshot** of ASAP's
[OTAP Dataflow](https://github.com/open-telemetry/otel-arrow/tree/main/rust/otap-dataflow)
processor implementation.

It ports ASAP's edge **precompute + sketching** runtime (windowed
DDSketch / KLL / HLL / CountSketch / CountMin-Sketch aggregation) onto
OTAP Dataflow's Arrow-native streaming model as a single unified
`asap_sketches` processor.

## Layout

| Path | What it is | Builds standalone? |
|---|---|---|
| [`asap-precompute-rs/`](./asap-precompute-rs) | The Rust crate: host-neutral precompute runtime **plus** the OTAP integration layer under the `otap` feature (`src/otap/`). | **Yes** — `cargo build --features otap` |
| [`otap-patch/`](./otap-patch) | The overlay applied onto the upstream OTAP Dataflow workspace at build time: the actual `local::Processor<OtapPdata>` node + `linkme` registration. | No — depends on the OTAP workspace crates (`otap-df-engine`, `otap-df-otap`, …); included here as reference for how the crate plugs into OTAP. |

## The OTAP processor, in two layers

**Layer A — the processor node** (`otap-patch/all/mod.rs`)
The piece OTAP's runtime actually sees:

- `AsapSketchesProcessor` implements
  `otel_arrow_dfe_engine::local::processor::Processor<OtapPdata>` — an
  `async fn process(msg: Message<OtapPdata>, effect_handler)` handling
  `Message::PData` and `NodeControlMsg::{TimerTick, Config, Shutdown, …}`.
  (Crate prefix `otel-arrow-dfe-*`, not the older `otap-df-*` — upstream
  renamed these very recently, otel-arrow issue #1848.)
- Registered via `#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]` under
  the URN `urn:asap:processor:asap_sketches`, with a `ProcessorFactory`
  (`create` + `validate_config` + `WiringContract::UNRESTRICTED`).
- `AsapSketchesUserConfig` is the TOML/serde shape validated at
  `--validate-and-exit` time (see
  [`otap-patch/plugins/asap_sketches/sample.toml`](./otap-patch/plugins/asap_sketches/sample.toml)).

> Note: the `OtapPdata ↔ OtapMetricRecords` binding is now implemented
> (`otap-patch/all/otap_bridge.rs`) — real OTLP metrics in,
> sketch/estimate output back out as real OTLP metrics, for the
> **producer role** (`AsapSketchesPlugin::start_from_envelopes`'s
> **receiver role** — ingesting another `asap_sketches` node's
> `SketchStreamBatch` output — isn't covered; that format doesn't fit
> OTAP's metrics shape). The adapter drives a bare `Precompute`
> instance directly rather than `AsapSketchesPlugin`'s own Tokio-task
> lifecycle, whose emit shape (`SketchStreamBatch`, PR #5/#6's
> dictionary economics) diverged from what this binding needs.
> **Build/lint/test-verified** against a real `open-telemetry/otel-arrow`
> checkout (commit `3e85c346`, 2026-08-24) — this repo itself still has
> no standalone build of `otap-patch/` (see Layer A/B split below), so
> that verification happened by staging both files into a temporary,
> separate checkout of the real workspace as a `crates/*` member; see
> `otap_bridge.rs`'s module doc "Verification status" for exactly what
> that covered.

**Layer B — the runtime lifecycle + Arrow codec** (`asap-precompute-rs/src/otap/`)

| File | Responsibility |
|---|---|
| [`lifecycle.rs`](./asap-precompute-rs/src/otap/lifecycle.rs) | `AsapSketchesPlugin` — Tokio runtime with three tasks: **input** (`Stream<OtapMetricRecords>` → `flatten` → `decode_batch` → `observe`), **flush ticker** (modeled on `NodeControlMsg::Wakeup`; `tick` → `encode_batch` → `lift` → emit), **control-channel** poll (`update_config`), plus a graceful drain on shutdown. |
| [`config.rs`](./asap-precompute-rs/src/otap/config.rs) | `PluginConfig` + `resolve()` — the 5-sketch `sketch_type` dispatch table (factory + observer + `SketchType`). |
| [`decode.rs`](./asap-precompute-rs/src/otap/decode.rs) / [`encode.rs`](./asap-precompute-rs/src/otap/encode.rs) | Arrow codec: `RecordBatch ↔ Vec<Observation>` / `[SketchEnvelope]`, keyed on well-known columns + Strategy-B `_asap_*` carrier keys ([`schema.rs`](./asap-precompute-rs/src/otap/schema.rs)). |
| [`records.rs`](./asap-precompute-rs/src/otap/records.rs) | `OtapMetricRecords` + `flatten`/`lift` — projects OTAP's sibling-batch family (metrics + per-row attribute child batch joined by `parent_id`) ↔ flat `RecordBatch`, including the attribute-lift that keeps emitted batches passing OTAP's strict schema validator. |

Both layers sit on the host-neutral runtime in the same crate
(`precompute`, `window`, `snapshot_cache`, `sketches/*`) — ASAP's
host-neutral edge precompute runtime.

## Build & test

```sh
cd asap-precompute-rs
cargo build --features otap
cargo test  --features otap        # 156 tests: runtime + OTAP codec + lifecycle
cargo clippy --all-targets --features otap -- -D warnings
cargo fmt --check
```

The default (no-feature) build excludes Arrow/Tokio and compiles the
row-oriented runtime only; the `otap` feature turns on the OTAP codec +
plugin lifecycle.

## Dependency note

[`asap_sketchlib`](https://github.com/ProjectASAP/asap_sketchlib) is a
public dependency.

## Status

Phases **B** (Arrow codec) and **C** (full 5-sketch plugin lifecycle)
are complete and tested here. Phase **D** (the `linkme` registration +
OTAP submodule build wiring, plus the `OtapPdata` binding) is present
as the `otap-patch/` overlay — the producer-role binding is now
implemented (`otap_bridge.rs`) and build/lint/test-verified against a
real OTAP Dataflow workspace checkout, though not by this repo's own
build (see the Layer A note above). The receiver-role `OtapPdata`
binding and cross-host byte-parity (Phase **E**) are the remaining
work.

---

This is a temporary public snapshot for review.
