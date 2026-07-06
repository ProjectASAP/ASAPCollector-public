//! `asap_sketches` — OTAP-Rust plugin shell entry point.
//!
//! This module is the binary-level binding between the OTAP
//! submodule's `linkme` distributed-slice registry and the
//! `asap-precompute-rs` lifecycle code. **Phase C does not populate
//! the registry call** — that work is Phase D.
//!
//! Phase C scope:
//! - Plugin shell directory layout established (this file +
//!   `sample.toml` + `README.md` siblings).
//! - User-facing configuration documented.
//!
//! Phase D scope (NOT done here):
//! - `Cargo.toml` for this crate (path-dep on `asap-precompute-rs`).
//! - `pub static`-shaped `linkme::distributed_slice` entry that
//!   registers the plugin with the OTAP runtime.
//! - Patches to `otap-patch/all/mod.rs` to bring the slice into
//!   the host binary's link scope.
//! - Build script for the overlay.
//!
//! See [`asap-precompute-rs/src/otap/lifecycle.rs`](../../../asap-precompute-rs/src/otap/lifecycle.rs)
//! for the actual plugin runtime; the Phase D code here is just an
//! adapter from OTAP's `local::Processor<OtapPdata>` trait surface
//! onto `AsapSketchesPlugin::start()`.

// Phase C placeholder — body populated in Phase D once the OTAP
// submodule is wired in.
