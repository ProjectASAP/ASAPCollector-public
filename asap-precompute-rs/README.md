# asap-precompute-rs

`asap-precompute-rs` is ASAP's host-neutral edge precompute runtime.
It owns the windowing logic for the Rust edge runtime, and — under the
`otap-engine` feature — the OTAP Dataflow processor and its native
`OtapPdata` codec.

A host translates its native event into an `Observation`, hands it to a
`Precompute`, and translates each emitted `SketchEnvelope` back to its
own format. A sketch envelope's payload is the sketch's full state as the
self-describing ASAPv1 envelope from `asap_sketchlib`; there are no
deltas.

## Module map

### Host-neutral runtime

| Rust module | Responsibility |
| --- | --- |
| `observation` | Input event model (`Observation`, `ObservationValue`). |
| `envelope` | Output model (`SketchEnvelope`, `SketchType`). |
| `precompute` | Core runtime: `observe` / `tick` / `drain` / `update_config`. |
| `window` | Tumbling-window state and rotation. |
| `kll_windows` | Retained KLL panes and sliding-window estimates. |
| `matchers` | Label matching / series-key construction. |
| `config` | `PrecomputeConfig`, `SketchParams`, `WindowSpec`. |
| `sketches/*` | Sketch wrappers over `asap_sketchlib` (DDSketch, KLL, HLL, CountSketch, Count-Min, Count-Min with heap). |

### OTAP Dataflow integration (`src/otap/`)

| Rust module | Responsibility |
| --- | --- |
| `otap::config` | `PluginConfig` + `resolve()` — `sketch_type` dispatch. |
| `otap::schema` | `sketch.*` carrier attribute keys. |
| `otap::codec` | `SketchEnvelope` ↔ `OtapPdata` (`otap-engine`). |
| `otap::processor` | The `urn:asap:processor:asap_sketches` processor (`otap-engine`). |

## Dependency

[`asap_sketchlib`](https://github.com/ProjectASAP/asap_sketchlib) is a
public dependency.

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets --features otap-engine,stream-benchmark -- -D warnings
cargo test --features otap-engine,stream-benchmark
```

The default build compiles the row-oriented runtime only (no
Arrow/Tokio). The test suite covers the runtime, the OTAP codec, and the
real dataflow processor (`tests/otap_pipeline_e2e.rs`).
