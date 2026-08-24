// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! The wire-lane's receive side, as a real OTAP `local::Receiver<OtapPdata>`
//! node — [`AsapSketchesReceiver`] listens on a TCP address, decodes each
//! connection with [`otap_wire::OtapWireReader`], and pushes the resulting
//! `OtapPdata` into the pipeline exactly like any other receiver would.
//!
//! # Why a `Receiver`, not something bolted onto `AsapSketchesProcessor`
//!
//! `local::Processor<PData>` (what [`crate::AsapSketchesProcessor`]
//! implements) is purely reactive — its `process()` method is only called
//! when the engine already has a `Message` to hand it, so it has no way to
//! independently accept a TCP connection. `local::Receiver<PData>` is OTAP's
//! dedicated shape for a node that owns its own listening loop: `start()`
//! takes exclusive ownership (`Box<Self>`) and runs until shutdown, using
//! `effect_handler.send_message` to hand decoded data to the rest of the
//! pipeline. This is the same trait the in-tree `host_metrics_receiver` and
//! `otap_receiver` (gRPC) nodes implement — this module follows that
//! pattern, not a new one.
//!
//! # Downstream wiring
//!
//! This receiver only decodes bytes off the wire into `OtapPdata` — it does
//! not know about `Precompute` at all. A pipeline wires it to an
//! `AsapSketchesProcessor` node downstream (`receiver:asap_sketches_wire ->
//! processor:asap_sketches`), which already handles arbitrary inbound
//! `OtapPdata` identically whether it arrived via a generic pipeline hop or
//! this direct-wire receiver (see `otap_bridge.rs`'s module doc, "Scope").
//!
//! # Scope / known limitation
//!
//! One connection is drained fully (looping [`otap_wire::OtapWireReader::recv`]
//! until a clean EOF) before the next `accept()` — i.e. this receiver serves
//! one producer at a time, not a connection pool. That matches the wire
//! lane's intended topology (one `asap_sketches` producer directly paired
//! with one receiver instance) rather than being a general-purpose ingress
//! server; supporting concurrent producers would need per-connection tasks
//! (`tokio::task::spawn_local`, which needs a `LocalSet` this adapter
//! doesn't currently establish) and is real follow-up work if that topology
//! is ever needed.
//!
//! # Verification status
//!
//! Build/lint/test-verified the same way as `otap_bridge.rs` / `otap_wire.rs`
//! — staged into a real `open-telemetry/otel-arrow` checkout
//! (`3e85c3460361446ebfce99e9f35fffd2dd5ab740`, 2026-08-24) as a `crates/*`
//! workspace member; see this module's own test.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use linkme::distributed_slice;
use serde::Deserialize;
use tokio::net::TcpListener;

use otel_arrow_dfe_config::error::Error as OtapConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ReceiverFactory;
use otel_arrow_dfe_engine::config::ReceiverConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::{Error, ReceiverErrorKind};
use otel_arrow_dfe_engine::local::receiver as local;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;

use crate::otap_wire::OtapWireReader;

/// Public URN for the wire-lane's receive-side node. Distinct from the
/// `asap_sketches` processor URN — this is specifically the direct-TCP
/// counterpart to [`crate::otap_wire::OtapWireWriter`], not a general
/// OTAP-metrics receiver (OTAP already has one of those,
/// `urn:otel:receiver:otap`).
pub const ASAP_SKETCHES_WIRE_RECEIVER_URN: &str = "urn:asap:receiver:asap_sketches_wire";

/// User-facing config for the wire-lane receiver: just the address to
/// listen on. Everything else (sketch type, window, etc.) belongs to the
/// downstream `asap_sketches` processor this receiver feeds — this node
/// only turns bytes on a socket into `OtapPdata`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsapSketchesReceiverConfig {
    /// TCP address to accept wire-lane connections on, e.g.
    /// `"0.0.0.0:4317"`.
    pub listen_addr: SocketAddr,
}

/// `linkme` registration entry — analogous to
/// [`crate::ASAP_SKETCHES_PROCESSOR_FACTORY`], but for
/// [`OTAP_RECEIVER_FACTORIES`].
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static ASAP_SKETCHES_WIRE_RECEIVER_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: ASAP_SKETCHES_WIRE_RECEIVER_URN,
    create: create_asap_sketches_wire_receiver,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: validate_asap_sketches_wire_receiver_config,
};

fn validate_asap_sketches_wire_receiver_config(
    config: &serde_json::Value,
) -> Result<(), OtapConfigError> {
    let _: AsapSketchesReceiverConfig =
        serde_json::from_value(config.clone()).map_err(|e| OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches_wire: {e}"),
        })?;
    Ok(())
}

/// Factory function — invoked once per pipeline instance at startup.
pub fn create_asap_sketches_wire_receiver(
    _pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    receiver_config: &ReceiverConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, OtapConfigError> {
    let user: AsapSketchesReceiverConfig = serde_json::from_value(node_config.config.clone())
        .map_err(|e| OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches_wire: failed to parse config: {e}"),
        })?;
    Ok(ReceiverWrapper::local(
        AsapSketchesReceiver {
            listen_addr: user.listen_addr,
        },
        node,
        node_config,
        receiver_config,
    ))
}

/// OTAP `local::Receiver<OtapPdata>` — the wire lane's receive side. See
/// this module's doc for why this is a `Receiver` and not folded into
/// [`crate::AsapSketchesProcessor`].
pub struct AsapSketchesReceiver {
    listen_addr: SocketAddr,
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for AsapSketchesReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let AsapSketchesReceiver { listen_addr } = *self;

        let listener = TcpListener::bind(listen_addr)
            .await
            .map_err(|e| Error::ReceiverError {
                receiver: effect_handler.receiver_id(),
                kind: ReceiverErrorKind::Connect,
                error: format!("asap_sketches_wire: failed to bind {listen_addr}: {e}"),
                source_detail: String::new(),
            })?;

        loop {
            tokio::select! {
                biased;

                msg = ctrl_chan.recv() => {
                    match msg {
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            effect_handler.notify_receiver_drained().await?;
                            return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Err(e) => return Err(Error::ChannelRecvError(e)),
                        _ => {}
                    }
                }

                accepted = listener.accept() => {
                    let Ok((mut socket, _peer_addr)) = accepted else {
                        // Transient accept error (e.g. peer reset before
                        // accept completed) — log and keep serving.
                        effect_handler
                            .info("asap_sketches_wire: accept() failed, continuing")
                            .await;
                        continue;
                    };
                    // One producer at a time, drained to a clean EOF — see
                    // this module's doc, "Scope / known limitation".
                    let mut reader = OtapWireReader::new();
                    loop {
                        match reader.recv(&mut socket).await {
                            Ok(Some(pdata)) => {
                                if effect_handler.send_message(pdata).await.is_err() {
                                    // Downstream is gone; nothing more to do
                                    // with this connection.
                                    break;
                                }
                            }
                            Ok(None) => break, // clean EOF at a frame boundary
                            Err(e) => {
                                effect_handler
                                    .info(&format!("asap_sketches_wire: connection error: {e}"))
                                    .await;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_factory_carries_expected_urn() {
        assert_eq!(
            ASAP_SKETCHES_WIRE_RECEIVER_FACTORY.name,
            ASAP_SKETCHES_WIRE_RECEIVER_URN
        );
        assert_eq!(
            ASAP_SKETCHES_WIRE_RECEIVER_URN,
            "urn:asap:receiver:asap_sketches_wire"
        );
    }

    #[test]
    fn listen_addr_config_round_trips() {
        let cfg: AsapSketchesReceiverConfig =
            serde_json::from_value(serde_json::json!({ "listen_addr": "127.0.0.1:4317" }))
                .expect("valid listen_addr parses");
        assert_eq!(cfg.listen_addr, "127.0.0.1:4317".parse().unwrap());
    }

    #[test]
    fn unknown_field_is_rejected() {
        let res: Result<AsapSketchesReceiverConfig, _> =
            serde_json::from_value(serde_json::json!({
                "listen_addr": "127.0.0.1:4317",
                "typo_field": true,
            }));
        assert!(res.is_err(), "unknown fields should be rejected");
    }

    /// End-to-end: the real `AsapSketchesReceiver::start()` (not a mock)
    /// listening on a real loopback TCP socket, fed by a real
    /// `OtapWireWriter::send`, decoded and pushed into the pipeline via
    /// the real `effect_handler.send_message` — proving this node
    /// actually runs, not just that its pieces compile. This is the
    /// receive-side counterpart to `otap_wire.rs`'s transport-level
    /// tests; this one exercises the OTAP `Receiver` node itself.
    #[test]
    fn end_to_end_receiver_pushes_a_real_otap_pdata_into_the_pipeline() {
        use crate::otap_bridge::{otap_metric_records_to_pdata, pdata_to_otap_metric_records};
        use arrow_array::{
            BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
        };
        use arrow_schema::{DataType, Field, Schema};
        use asap_precompute_rs::otap::records::{
            ATTR_BATCH_BYTES, ATTR_BATCH_INT, ATTR_BATCH_KEY, ATTR_BATCH_PARENT_ID, ATTR_BATCH_STR,
        };
        use asap_precompute_rs::otap::{
            COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE, OtapMetricRecords,
        };
        use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
        use otel_arrow_dfe_engine::testing::{receiver::TestRuntime, test_node};
        use std::sync::Arc as StdArc;
        use std::time::{Duration, Instant};
        use tokio::net::TcpStream;

        let metrics = RecordBatch::try_new(
            StdArc::new(Schema::new(vec![
                Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Float64, false),
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            ])),
            vec![
                StdArc::new(UInt64Array::from(vec![1_000_000_000_u64])),
                StdArc::new(StringArray::from(vec!["http_request_duration_ms"])),
                StdArc::new(Float64Array::from(vec![42.5_f64])),
                StdArc::new(UInt32Array::from(vec![0_u32])),
            ],
        )
        .expect("metrics batch");
        let attributes = RecordBatch::try_new(
            StdArc::new(Schema::new(vec![
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
                Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
                Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
                Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
                Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
            ])),
            vec![
                StdArc::new(UInt32Array::from(vec![0_u32])),
                StdArc::new(StringArray::from(vec!["path"])),
                StdArc::new(StringArray::from(vec![Some("/api")])),
                StdArc::new(UInt64Array::from(vec![None::<u64>])),
                StdArc::new(BinaryArray::from_opt_vec(vec![None])),
            ],
        )
        .expect("attributes batch");
        let pdata = otap_metric_records_to_pdata(&OtapMetricRecords {
            metrics,
            attributes,
        })
        .expect("build real OtapPdata");

        let addr: SocketAddr = format!(
            "127.0.0.1:{}",
            otel_arrow_dfe_test_net::pick_unused_loopback_tcp_port()
        )
        .parse()
        .expect("valid loopback addr");

        let test_runtime = TestRuntime::<OtapPdata>::new();
        let node_config = StdArc::new(NodeUserConfig::new_receiver_config(
            ASAP_SKETCHES_WIRE_RECEIVER_URN,
        ));
        let receiver = ReceiverWrapper::local(
            AsapSketchesReceiver { listen_addr: addr },
            test_node(test_runtime.config().name.clone()),
            node_config,
            test_runtime.config(),
        );

        test_runtime
            .set_receiver(receiver)
            .run_test(move |ctx| async move {
                let mut client = TcpStream::connect(addr)
                    .await
                    .expect("connect to the running AsapSketchesReceiver");
                crate::otap_wire::OtapWireWriter::new()
                    .send(&mut client, pdata)
                    .await
                    .expect("send real OtapPdata over the wire lane");
                drop(client); // clean EOF: receiver's inner connection loop returns to accept()
                ctx.send_shutdown(Instant::now() + Duration::from_secs(5), "test done")
                    .await
                    .expect("send shutdown");
            })
            .run_validation(|mut vctx| async move {
                let received = vctx
                    .recv()
                    .await
                    .expect("receiver pushed the decoded OtapPdata downstream");
                let outcome = pdata_to_otap_metric_records(received).expect("decode");
                let decoded = outcome.records.expect("one row survived the round trip");
                let metric_col = decoded
                    .metrics
                    .column_by_name(COLUMN_METRIC)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(metric_col.value(0), "http_request_duration_ms");
                let value_col = decoded
                    .metrics
                    .column_by_name(COLUMN_VALUE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(value_col.value(0), 42.5);
            });
    }
}
