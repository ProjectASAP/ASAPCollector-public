# ASAPCollector — OTAP Dataflow processor (public snapshot)

This repository is a **standalone, buildable snapshot** of ASAP's
[OTAP Dataflow](https://github.com/open-telemetry/otel-arrow/tree/main/rust/otap-dataflow)
processor implementation, extracted from the private `ASAPCollector`
monorepo so the OpenTelemetry / OTAP maintainers can read and build it.

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
  `otap_df_engine::local::processor::Processor<OtapPdata>` — an
  `async fn process(msg: Message<OtapPdata>, effect_handler)` handling
  `Message::PData` and `NodeControlMsg::{Wakeup, Config, Shutdown, …}`.
- Registered via `#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]` under
  the URN `urn:asap:processor:asap_sketches`, with a `ProcessorFactory`
  (`create` + `validate_config` + `WiringContract::UNRESTRICTED`).
- `AsapSketchesUserConfig` is the TOML/serde shape validated at
  `--validate-and-exit` time (see
  [`otap-patch/plugins/asap_sketches/sample.toml`](./otap-patch/plugins/asap_sketches/sample.toml)).

> Note: this adapter is currently a deliberate **pass-through** — the
> `OtapPdata ↔ OtapMetricRecords` `From`/`Into` binding is the one seam
> left to wire (marked Phase D/E in the code). Everything it drives
> (Layer B) is complete and tested.

**Layer B — the runtime lifecycle + Arrow codec** (`asap-precompute-rs/src/otap/`)

| File | Responsibility |
|---|---|
| [`lifecycle.rs`](./asap-precompute-rs/src/otap/lifecycle.rs) | `AsapSketchesPlugin` — Tokio runtime with three tasks: **input** (`Stream<OtapMetricRecords>` → `flatten` → `decode_batch` → `observe`), **flush ticker** (modeled on `NodeControlMsg::Wakeup`; `tick` → `encode_batch` → `lift` → emit), **control-channel** poll (`update_config`), plus a graceful drain on shutdown. |
| [`config.rs`](./asap-precompute-rs/src/otap/config.rs) | `PluginConfig` + `resolve()` — the 5-sketch `sketch_type` dispatch table (factory + observer + `SketchType`). |
| [`decode.rs`](./asap-precompute-rs/src/otap/decode.rs) / [`encode.rs`](./asap-precompute-rs/src/otap/encode.rs) | Arrow codec: `RecordBatch ↔ Vec<Observation>` / `[SketchEnvelope]`, keyed on well-known columns + Strategy-B `_asap_*` carrier keys ([`schema.rs`](./asap-precompute-rs/src/otap/schema.rs)). |
| [`records.rs`](./asap-precompute-rs/src/otap/records.rs) | `OtapMetricRecords` + `flatten`/`lift` — projects OTAP's sibling-batch family (metrics + per-row attribute child batch joined by `parent_id`) ↔ flat `RecordBatch`, including the attribute-lift that keeps emitted batches passing OTAP's strict schema validator. |

Both layers sit on the host-neutral runtime in the same crate
(`precompute`, `window`, `snapshot_cache`, `sketches/*`), which is the
Rust mirror of ASAP's `asap-precompute-go` edge runtime.

## Build & test

```sh
cd asap-precompute-rs
cargo build --features otap
cargo test  --features otap        # 135 tests: runtime + OTAP codec + lifecycle
cargo clippy --all-targets --features otap -- -D warnings
cargo fmt --check
```

The default (no-feature) build excludes Arrow/Tokio and compiles the
row-oriented runtime only; the `otap` feature turns on the OTAP codec +
plugin lifecycle.

## Dependency note

The one external ASAP dependency, the sketch library
[`asap_sketchlib`](https://github.com/ProjectASAP/asap_sketchlib), is
**already public** (open on GitHub and published on crates.io).
`asap-precompute-rs/Cargo.toml` pins it to a specific public commit so
this crate builds against exactly the sketch API it was written for.

## Status

Phases **B** (Arrow codec) and **C** (full 5-sketch plugin lifecycle)
are complete and tested here. Phase **D** (the `linkme` registration +
OTAP submodule build wiring) is present as the `otap-patch/` overlay
with the processor adapter as a pass-through; the `OtapPdata` binding
and cross-host byte-parity (Phase **E**) are the remaining work.

---

This is a temporary public snapshot for review; the source of truth is
the private `ASAPCollector` monorepo.
