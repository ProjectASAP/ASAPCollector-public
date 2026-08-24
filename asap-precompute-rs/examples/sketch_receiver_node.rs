//! Network-transport half of the `docs/data_model.md` demo: a
//! **receive processor** running as a real `AsapSketchesPlugin` in
//! its receiver role (`start_from_envelopes` — see
//! `src/otap/lifecycle.rs`'s module doc, "The other role"), reading
//! [`SketchStreamBatch`]es off a real TCP socket (Arrow IPC-decoded
//! via `otap::wire`) sent by `sketch_producer_node`
//! (`examples/sketch_producer_node.rs`).
//!
//! Merges every reconstructed envelope via `Precompute::observe_envelope`,
//! then — because this plugin's own config sets `transmit_sketch =
//! false`, `quantiles = [0.99]` — its own ticker/drain naturally
//! produces p99 *estimate* envelopes instead of re-emitting sketch
//! bytes. Those come back out through this plugin's own emit channel
//! as another `SketchStreamBatch`, which this binary decodes and
//! prints as Prometheus text (the "Prometheus backend" stage — see
//! `sketch_pipeline_demo.rs`'s module doc for why printing stands in
//! for a real `/metrics` HTTP handler).
//!
//! Run this first (see `sketch_producer_node.rs`'s module doc):
//! ```text
//! cargo run --example sketch_receiver_node --features otap
//! ```

use std::time::Duration;

use asap_precompute_rs::envelope::SketchEnvelope;
use asap_precompute_rs::otap::config::PluginConfig;
use asap_precompute_rs::otap::wire::WireReader;
use asap_precompute_rs::otap::{
    AsapSketchesPlugin, SeriesDictionaryDecoder, SketchStreamBatch, StartOptions,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Must match `sketch_producer_node`'s connect address.
const LISTEN_ADDR: &str = "127.0.0.1:47821";
const AGG_ID: u64 = 1;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(LISTEN_ADDR).await.expect("bind");
    println!("[receiver] listening on {LISTEN_ADDR}, waiting for sketch_producer_node...");
    let (mut socket, peer) = listener.accept().await.expect("accept");
    println!("[receiver] accepted connection from {peer}");

    // Bridge "read framed batches off the socket" into the
    // Stream<Item = SketchStreamBatch> start_from_envelopes wants.
    let (batch_tx, batch_rx) = mpsc::unbounded_channel::<SketchStreamBatch>();
    let socket_task = tokio::spawn(async move {
        // One WireReader for the whole connection, matching the
        // producer's one WireWriter — its retained per-role IPC state
        // is what lets a later window's delta (schema/dictionary/
        // labels genuinely absent) still resolve correctly.
        let mut wire_reader = WireReader::new();
        loop {
            match wire_reader.recv(&mut socket).await {
                Ok(Some(batch)) => {
                    println!(
                        "[receiver] received over the wire: schema={} dictionary={} labels={} record={} row(s)",
                        batch.schema.num_rows(),
                        batch.dictionary.num_rows(),
                        batch.labels.num_rows(),
                        batch.record.num_rows(),
                    );
                    if batch_tx.send(batch).is_err() {
                        return; // plugin already gone.
                    }
                }
                Ok(None) => {
                    println!("[receiver] producer closed the connection");
                    return;
                }
                Err(e) => {
                    eprintln!("[receiver] wire error: {e}");
                    return;
                }
            }
        }
    });

    let plugin_cfg = PluginConfig {
        sketch_type: "ddsketch".into(),
        window_size: Duration::from_secs(60), // driven by drain() on shutdown, same as the producer.
        output_metric_name: "http_request_duration_ms_p99".into(),
        agg_id: AGG_ID,
        transmit_sketch: false, // query mode: emits quantile estimates, not sketch bytes.
        quantiles: vec![0.99],
        ..Default::default()
    };
    let plugin = AsapSketchesPlugin::from_plugin_config(&plugin_cfg).expect("receiver config");
    let (handle, mut emit_rx) = plugin.start_from_envelopes(
        UnboundedReceiverStream::new(batch_rx),
        None,
        StartOptions::default(),
    );

    // Wait for the producer to finish sending (socket EOF) before
    // asking for the final drain -- otherwise shutdown could race a
    // still-in-flight batch.
    socket_task.await.expect("socket task");
    handle.shutdown().await.expect("receiver shutdown");

    let mut decoder = SeriesDictionaryDecoder::new();
    while let Ok(Some(batch)) =
        tokio::time::timeout(Duration::from_millis(500), emit_rx.recv()).await
    {
        for estimate in decoder.decode(&batch).expect("decode estimate batch") {
            print!("{}", format_prometheus_gauge(&estimate));
        }
    }
}

/// Formats one estimate-mode [`SketchEnvelope`] (`payload` empty,
/// `value` set — see `docs/data_model.md`'s `RECORD.value`) as a
/// Prometheus text-exposition gauge sample. Stands in for a real
/// `/metrics` HTTP handler.
fn format_prometheus_gauge(env: &SketchEnvelope) -> String {
    let mut labels: Vec<String> = env
        .labels
        .iter()
        .map(|kv| format!("{}=\"{}\"", kv.key, kv.value))
        .collect();
    labels.sort();
    let label_str = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", labels.join(","))
    };
    format!(
        "# HELP {name} sketch-derived quantile estimate\n\
         # TYPE {name} gauge\n\
         {name}{label_str} {value} {ts}\n",
        name = env.metric_name,
        value = env.value,
        ts = env.window_end_ms,
    )
}
