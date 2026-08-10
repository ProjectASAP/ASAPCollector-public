//! Network-transport half of the `docs/data_model.md` demo: a
//! **sketch creation processor** running as a real `AsapSketchesPlugin`
//! (not called directly like `sketch_pipeline_demo.rs` does), pushing
//! each emitted [`SketchStreamBatch`] over a real TCP socket — Arrow
//! IPC-serialized via `otap::wire` — to `sketch_receiver_node`
//! (`examples/sketch_receiver_node.rs`), which must already be
//! listening.
//!
//! Run (in one terminal, first):
//! ```text
//! cargo run --example sketch_receiver_node --features otap
//! ```
//! Then (in a second terminal):
//! ```text
//! cargo run --example sketch_producer_node --features otap
//! ```
//!
//! Unlike `sketch_pipeline_demo.rs`'s in-process `mpsc` channel, what
//! crosses the wire here is genuinely serialized bytes over a socket
//! — the actual "crosses a node or network boundary" hop
//! `docs/data_model.md` opens with. See `otap::wire`'s module doc for
//! the exact frame layout.
//!
//! Unlike `sketch_pipeline_demo.rs` (which force-closes windows with
//! explicit `drain()` calls), this binary feeds observations through
//! a real OTAP-shaped input stream (`records::flatten` +
//! `decode_batch`, `AsapSketchesPlugin::start`'s input task) and lets
//! the plugin's real `Wakeup`-style Tokio ticker close windows on its
//! own wall-clock schedule — a `window_size` short enough (300ms) to
//! see several windows roll in one run.

use std::time::Duration;

use arrow_array::{BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use asap_precompute_rs::otap::config::PluginConfig;
use asap_precompute_rs::otap::records::{
    OtapMetricRecords, ATTR_BATCH_BYTES, ATTR_BATCH_INT, ATTR_BATCH_KEY, ATTR_BATCH_PARENT_ID,
    ATTR_BATCH_STR,
};
use asap_precompute_rs::otap::wire::send_stream_batch;
use asap_precompute_rs::otap::{
    AsapSketchesPlugin, StartOptions, COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Must match `sketch_receiver_node`'s listen address.
const RECEIVER_ADDR: &str = "127.0.0.1:47821";
const AGG_ID: u64 = 1;
const NUM_WINDOWS: usize = 4;
const WINDOW_SIZE: Duration = Duration::from_millis(300);
/// Longer than `WINDOW_SIZE` so each window's data has already landed
/// before the real ticker rotates it out — real wall-clock pacing, not
/// a guaranteed lockstep boundary, so this is a margin, not a promise.
const PACING: Duration = Duration::from_millis(400);

#[tokio::main]
async fn main() {
    let mut socket = connect_with_retry(RECEIVER_ADDR).await;
    println!("[producer] connected to {RECEIVER_ADDR}");

    let plugin_cfg = PluginConfig {
        sketch_type: "ddsketch".into(),
        window_size: WINDOW_SIZE,
        output_metric_name: "http_request_duration_ms".into(),
        agg_id: AGG_ID,
        sketch_params: [("relative_accuracy".to_string(), 0.01)]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let plugin = AsapSketchesPlugin::from_plugin_config(&plugin_cfg).expect("producer config");

    // Bridge a paced feed of synthetic OTAP-shaped input records into
    // the Stream<Item = OtapMetricRecords> AsapSketchesPlugin::start
    // wants -- this is the plugin's real ingest path (records::flatten
    // + decode_batch), not a direct Precompute::observe() call.
    let (input_tx, input_rx) = mpsc::unbounded_channel::<OtapMetricRecords>();
    let feed_task = tokio::spawn(async move {
        for window_idx in 0..NUM_WINDOWS {
            // One series (path=/api), latency drifting upward window
            // to window so the receiver's printed p99 visibly moves.
            for i in 0..200u64 {
                let base = 10.0 + (window_idx as f64) * 8.0;
                let latency = base + (i % 25) as f64;
                let records =
                    build_scalar_records("http_request_duration_ms", latency, now_ms(), "/api");
                if input_tx.send(records).is_err() {
                    return; // plugin shut down early.
                }
            }
            // Let the ticker rotate this window out before the next
            // window's data starts arriving.
            tokio::time::sleep(PACING).await;
        }
        // Dropping input_tx here ends the plugin's input stream.
    });

    let (handle, mut emit_rx) = plugin.start(
        UnboundedReceiverStream::new(input_rx),
        None,
        StartOptions::default(),
    );

    // Forward every emitted SketchStreamBatch over the socket as it
    // arrives, concurrently with the feed task still running.
    let forward_task = tokio::spawn(async move {
        let mut window_idx = 0;
        while let Some(batch) = emit_rx.recv().await {
            println!(
                "[producer] window {window_idx}: schema={} dictionary={} labels={} record={} row(s) -- sending over the wire",
                batch.schema.num_rows(),
                batch.dictionary.num_rows(),
                batch.labels.num_rows(),
                batch.record.num_rows(),
            );
            send_stream_batch(&mut socket, &batch)
                .await
                .expect("send over socket");
            window_idx += 1;
        }
        socket
    });

    feed_task.await.expect("feed task");
    // Give the last window's data one more full period to roll
    // naturally, then shut down -- the final drain flushes any
    // residue that hadn't hit a tick boundary yet.
    tokio::time::sleep(PACING).await;
    handle.shutdown().await.expect("producer shutdown");

    let socket = forward_task.await.expect("forward task");
    drop(socket); // close the connection -> receiver sees a clean EOF.
    println!("[producer] done, connection closed");
}

/// Builds a one-row [`OtapMetricRecords`] for a raw scalar
/// observation with a single `path` label -- the OTAP-Metrics-shaped
/// input `AsapSketchesPlugin::start`'s input task consumes from a
/// real upstream OTAP source (Telegraf / Vector / another OTAP
/// collector).
fn build_scalar_records(
    metric: &str,
    value: f64,
    timestamp_ms: u64,
    path: &str,
) -> OtapMetricRecords {
    let metrics_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
        Field::new(COLUMN_METRIC, DataType::Utf8, false),
        Field::new(COLUMN_VALUE, DataType::Float64, false),
        Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
    ]));
    let metrics = RecordBatch::try_new(
        metrics_schema,
        vec![
            std::sync::Arc::new(UInt64Array::from(vec![timestamp_ms * 1_000_000])),
            std::sync::Arc::new(StringArray::from(vec![metric])),
            std::sync::Arc::new(Float64Array::from(vec![value])),
            std::sync::Arc::new(UInt32Array::from(vec![0_u32])),
        ],
    )
    .expect("metrics batch");
    let attributes_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
        Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
        Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
        Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
        Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
    ]));
    let attributes = RecordBatch::try_new(
        attributes_schema,
        vec![
            std::sync::Arc::new(UInt32Array::from(vec![0_u32])),
            std::sync::Arc::new(StringArray::from(vec!["path"])),
            std::sync::Arc::new(BinaryArray::from_opt_vec(vec![None as Option<&[u8]>])),
            std::sync::Arc::new(StringArray::from(vec![Some(path)])),
            std::sync::Arc::new(UInt64Array::from(vec![None as Option<u64>])),
        ],
    )
    .expect("attributes batch");
    OtapMetricRecords {
        metrics,
        attributes,
    }
}

/// Retries the connection a few times -- `sketch_receiver_node` may
/// not have bound its listener yet if both binaries are started at
/// nearly the same moment.
async fn connect_with_retry(addr: &str) -> TcpStream {
    for attempt in 0..20 {
        match TcpStream::connect(addr).await {
            Ok(s) => return s,
            Err(e) => {
                if attempt == 0 {
                    println!("[producer] waiting for {addr} to accept connections ({e})...");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("could not connect to {addr} after retries -- is sketch_receiver_node running?");
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}
