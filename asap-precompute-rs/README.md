# asap-precompute-rs

`asap-precompute-rs` is the Rust mirror of ASAP's `asap-precompute-go`,
the host-neutral edge precompute runtime. It owns the windowing,
snapshot caching, and delta-encoding logic for the Rust edge runtime,
and — under the `otap` feature — the OTAP Dataflow integration layer
(Arrow codec + Tokio plugin lifecycle).

Per-platform Adapter implementations (the Layer-4 shims) translate their
host's native event into an `Observation`, hand it to a `Precompute`,
and translate the runtime's emitted `SketchEnvelope` back to the host's
native event.

## Module map

### Host-neutral runtime

| Rust module | Responsibility |
| --- | --- |
| `observation` | Input event model (`Observation`, `ObservationValue`). |
| `envelope` | Output model (`SketchEnvelope`, `SketchType`, `Encoding`). |
| `precompute` | Core runtime: `observe` / `tick` / `drain` / `update_config`. |
| `window` | Tumbling-window state and rotation. |
| `snapshot_cache` | Delta encoding — full vs delta snapshot decisions. |
| `matchers` | Label matching / series-key construction. |
| `config` | `PrecomputeConfig`, `SketchParams`, `WindowSpec`. |
| `sketches/*` | Sketch wrappers over `asap_sketchlib` (DDSketch, KLL, HLL, CountSketch, CountMin-Sketch). |
| `adapter` | Host-adapter trait surface. |
| `control_channel` | Controller plan delivery (`ControlChannel`). |

### OTAP Dataflow integration (`src/otap/`, `otap` feature)

| Rust module | Responsibility |
| --- | --- |
| `otap::lifecycle` | `AsapSketchesPlugin` — Tokio runtime (input / flush-ticker / control-channel tasks + graceful drain). |
| `otap::config` | `PluginConfig` + `resolve()` — 5-sketch `sketch_type` dispatch. |
| `otap::decode` / `otap::encode` | Arrow `RecordBatch` ↔ `Observation` / `SketchEnvelope` codec. |
| `otap::records` | `OtapMetricRecords` + `flatten` / `lift` (sibling-batch ↔ flat-batch projection, Strategy-B attribute-lift). |
| `otap::schema` | Well-known column names + `_asap_*` Strategy-B carrier keys. |
| `otap::plugin` | `StubPlugin` — minimal codec-only shell (regression backstop). |

## Dependency

The sketch library [`asap_sketchlib`](https://github.com/ProjectASAP/asap_sketchlib)
is public (open on GitHub, published on crates.io). `Cargo.toml` pins it
to a specific public commit; switch to `branch = "main"` or a crates.io
version spec to track other releases.

## Validation

```sh
cargo build
cargo build --features otap
cargo test  --features otap
cargo clippy --all-targets --features otap -- -D warnings
cargo fmt --check
```

The default build compiles the row-oriented runtime only (no
Arrow/Tokio); the `otap` feature enables the OTAP codec + plugin
lifecycle. The test suite covers the runtime plus the OTAP codec
(`tests/otap_codec.rs`) and plugin lifecycle (`tests/otap_lifecycle.rs`).
