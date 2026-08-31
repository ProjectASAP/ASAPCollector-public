# ASAPCollector — OTAP Dataflow processor

This repository is a **standalone, buildable** implementation of
ASAP's [OTAP Dataflow](https://github.com/open-telemetry/otel-arrow/tree/main/rust/otap-dataflow)
processor.

It ports ASAP's edge **precompute + sketching** runtime (windowed
DDSketch / KLL / HLL / CountSketch / CountMin-Sketch aggregation) onto
OTAP Dataflow's Arrow-native streaming model as a single unified
`asap_sketches` processor.

## Layout

One crate: [`asap-precompute-rs/`](./asap-precompute-rs). Three feature
levels, each `cargo build`-able on its own with no manual staging or
overlay step:

| Feature | What it adds | Extra dependency |
|---|---|---|
| *(default, no features)* | Host-neutral precompute runtime — the five sketch types, windowing, overflow policy. No Arrow, no OTAP, no Tokio. | none |
| `otap` | The Arrow codec (`encode_batch`/`decode_batch`, `OtapMetricRecords`), the legacy `SeriesDictionary`/`otap::wire` transport, and `AsapSketchesPlugin`'s Tokio-task lifecycle. | `arrow-*`, `tokio` |
| `otap-engine` | The real OTAP node: `AsapSketchesProcessor` (a genuine `local::Processor<OtapPdata>`, `linkme`-registered) and its direct `SketchEnvelope <-> OtapPdata` binding (`otap::codec`). | the real `otel-arrow-dfe-*` crates, via a plain git dependency pinned to a specific `open-telemetry/otel-arrow` commit — see `Cargo.toml` |

`otap-engine`'s git dependency is what used to require staging this
crate's files into a separately-checked-out copy of the OTAP Dataflow
workspace (the old `otap-patch/` overlay). That's gone: Cargo resolves
the named `otel-arrow-dfe-*` packages directly from a clone of that
repo and unifies dependency versions across the whole build graph the
same way it already does for the `asap_sketchlib` git dependency —
`cargo build --features otap-engine` just works.

## The real OTAP node (`otap::processor` / `otap::codec`)

- [`processor.rs`](./asap-precompute-rs/src/otap/processor.rs) —
  `AsapSketchesProcessor` implements
  `otel_arrow_dfe_engine::local::processor::Processor<OtapPdata>` — an
  `async fn process(msg: Message<OtapPdata>, effect_handler)` handling
  `Message::PData` and `NodeControlMsg::{TimerTick, Config, Shutdown, …}`.
  Registered via `#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]` under
  the URN `urn:asap:processor:asap_sketches`. `AsapSketchesUserConfig`
  is the TOML/serde config shape validated at `--validate-and-exit`
  time — see [`plugins/asap_sketches/sample.toml`](./asap-precompute-rs/plugins/asap_sketches/sample.toml).
- [`codec.rs`](./asap-precompute-rs/src/otap/codec.rs) — the direct
  `SketchEnvelope <-> OtapPdata` binding: implements OTAP's own
  `MetricsView` trait family straight over `&[SketchEnvelope]`
  (encode) and decodes a real `OtapPdata` straight into
  `Vec<Observation>` (decode). No intermediate `RecordBatch` or
  `OtapMetricRecords` two-batch hop — those exist as a general,
  real-OTAP-free carrier format for other potential Strategy-B
  adapters (Telegraf, Vector), but this repo only ever ships to OTAP,
  so the OTAP-facing path skips straight to the real type.

**There is exactly one transport: the pipeline.** `AsapSketchesProcessor`
sends only via `effect_handler.send_message_with_source_node` — an
earlier revision also carried a direct-TCP "wire lane" as a second
transport; that's been removed. It turned out to be solving a problem
OTAP's real Arrow encoding already solves: the custom wire lane
existed to give sketch traffic dictionary/schema-reuse economics (the
legacy `SeriesDictionary` SCHEMA/DICTIONARY/RECORD tiering), but
`codec.rs`'s real `OtapPdata` gets that for free — OTAP's own Arrow
encoder dictionary-encodes the metric name and every string-valued
attribute key/value by construction. Confirmed against the real
workspace and guarded by a permanent regression test
(`codec.rs`'s `real_otap_encoding_dictionary_encodes_metric_name_and_string_attributes`);
see `processor.rs`'s module doc, "There is exactly one transport: the
pipeline", for the full rationale and the real schema this produces.

**Build/lint/test-verified**, by this repo's own `cargo build
--features otap-engine` / `cargo test --features otap-engine` /
`cargo clippy --features otap-engine --all-targets -D warnings` /
`cargo fmt --check`, against `open-telemetry/otel-arrow` commit
`3e85c346` (2026-08-24, pinned in `Cargo.toml`).

## The rest of the runtime lifecycle + Arrow codec

| File | Responsibility |
|---|---|
| [`lifecycle.rs`](./asap-precompute-rs/src/otap/lifecycle.rs) | `AsapSketchesPlugin` — Tokio runtime with three tasks: **input** (`Stream<OtapMetricRecords>` → `flatten` → `decode_batch` → `observe`), **flush ticker** (modeled on `NodeControlMsg::Wakeup`; `tick` → `encode_batch` → `lift` → emit), **control-channel** poll (`update_config`), plus a graceful drain on shutdown. Also how `otap::processor` constructs its bare `Precompute`. |
| [`config.rs`](./asap-precompute-rs/src/otap/config.rs) | `PluginConfig` + `resolve()` — the 5-sketch `sketch_type` dispatch table (factory + observer + `SketchType`). |
| [`decode.rs`](./asap-precompute-rs/src/otap/decode.rs) / [`encode.rs`](./asap-precompute-rs/src/otap/encode.rs) | Arrow codec: `RecordBatch ↔ Vec<Observation>` / `[SketchEnvelope]`, keyed on well-known columns + Strategy-B `_asap_*` carrier keys ([`schema.rs`](./asap-precompute-rs/src/otap/schema.rs)). Backs the legacy `dictionary`/`wire` transport below, not `otap::codec`. |
| [`records.rs`](./asap-precompute-rs/src/otap/records.rs) | `OtapMetricRecords` + `flatten`/`lift` — projects an `OtapArrowRecords`-shaped sibling-batch family (metrics + per-row attribute child batch joined by `parent_id`) ↔ flat `RecordBatch`. Real-OTAP-free; same scope note as above. |
| [`dictionary.rs`](./asap-precompute-rs/src/otap/dictionary.rs) / [`wire.rs`](./asap-precompute-rs/src/otap/wire.rs) | `SeriesDictionary`/`SketchStreamBatch` — the SCHEMA/DICTIONARY/RECORD wire economics, and its Arrow-IPC/TCP transport. Tested, but not part of the `otap-engine` path — kept for the standalone `sketch_producer_node`/`sketch_receiver_node` example binaries. |

All of this sits on the host-neutral runtime in the same crate
(`precompute`, `window`, `snapshot_cache`, `sketches/*`).

## Build & test

For a presentation-ready walkthrough of the real YAML pipeline integration,
see the [live OTAP pipeline demo guide](./docs/demo-guide.md).

```sh
cd asap-precompute-rs
cargo build --features otap-engine
cargo test  --features otap-engine       # runtime + codec + lifecycle + real pipeline tests
cargo clippy --all-targets --features otap-engine -- -D warnings
cargo fmt --check
```

The default (no-feature) build excludes Arrow/Tokio and compiles the
row-oriented runtime only; `otap` turns on the Arrow codec + legacy
transport + plugin lifecycle; `otap-engine` additionally turns on the
real OTAP node (and pulls in the real OTAP Dataflow crates — a heavier,
slower build: datafusion, tonic, and the rest of that dependency tree).

## Dependency note

[`asap_sketchlib`](https://github.com/ProjectASAP/asap_sketchlib) is a
public dependency. `otap-engine` additionally depends on
`open-telemetry/otel-arrow`'s `otel-arrow-dfe-*` crates, git-pinned to
a specific commit in `Cargo.toml`.

## Status

Phases **B** (Arrow codec) and **C** (full 5-sketch plugin lifecycle)
are complete and tested. Phase **D** (the `linkme` registration + the
real `OtapPdata` binding) is complete and build/lint/test-verified by
this repo's own build. `tests/otap_pipeline_e2e.rs` parses a genuine
OTAP pipeline YAML, builds it through `OTAP_PIPELINE_FACTORY`, runs the
resulting `RuntimePipeline`, injects real scalar `OtapPdata`, and verifies a
three-processor creation → merge → estimation flow at the configured exporter.
Run the same live pipeline as a terminal demo with:

```sh
cd asap-precompute-rs
cargo run --bin asap-otap-demo --features otap-engine
```

The real OTAP adapter merges an upstream processor's self-describing sketch
directly from `OtapPdata`; the separate legacy `SketchStreamBatch` format has
its own standalone example binaries. Cross-host byte-parity (Phase **E**) is
the remaining work.

---

This is a temporary public snapshot for review.
