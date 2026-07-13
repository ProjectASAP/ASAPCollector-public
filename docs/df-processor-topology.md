# `asap_sketches` dataflow-processor topologies

How the `asap_sketches` OTAP Dataflow processor is deployed, and what actually
crosses each edge. The whole design turns on one fact:

> Arrow zero-copy makes **raw metrics in** and **results out** free between
> local (thread-per-core) nodes — the buffers are `Arc`-backed, so a channel
> hop is a pointer + refcount bump, not a memcpy. But a **sketch is not a
> column**: the moment a value lands in a `DdSketch`/`Hll`/… it becomes opaque
> single-owner state that never rides a channel. Crossing a node boundary with
> a sketch therefore *always* costs one serialize (proto or **msgpack**).

Design rule: **keep every sketch inside the one node that owns its window
state; only ever let SUMMARIES (estimated datapoints) or SERIALIZED envelopes
leave. Split into a second processor only at a real tier/host boundary, where a
network hop already forces the serialize anyway.**

---

## The two topologies

There are exactly two shapes. The four "cases" below are these two shapes ×
an output knob.

### Topology A — edge-terminal (the sketch never leaves the node)

`transmit_sketch = false`: observe → window → **estimate at flush** → emit
typed metric datapoints. Nothing but small gauges leaves the node.

```mermaid
flowchart LR
  R["OTLP / OTAP receiver"] -->|"Arrow raw metrics<br/>(zero-copy)"| P["<b>asap_sketches</b><br/>observe · window · estimate<br/>transmit_sketch = false"]
  P -->|"Arrow typed datapoints<br/>quantile / cardinality gauges<br/>(zero-copy)"| X["exporter → backend"]
  style P fill:#2b6cb0,color:#fff
```

The sketch state lives inside `P`, owned, never on a channel. At `Wakeup`
(window close) the node reads each sketch and emits Gauge rows.

### Topology B — edge → central (ship the sketch, merge & estimate downstream)

`transmit_sketch = true`, `encoding = Msgpack | MsgpackDelta`: the edge ships
sketch bytes; a **second instance of the same processor** on the central tier
decodes, merges across edges, and estimates.

```mermaid
flowchart LR
  subgraph EDGE["Edge host"]
    R1["receiver"] -->|"raw"| P1["<b>asap_sketches</b><br/>observe · window<br/>output = sketch bytes"]
    P1 -->|"Arrow _asap_envelope Binary<br/>msgpack full / delta<br/>serialize once"| E1["OTAP exporter / gRPC"]
  end
  subgraph CENTRAL["Central host"]
    R2["OTAP receiver"] -->|"Arrow envelope col"| P2["<b>asap_sketches</b><br/>input = envelope<br/>merge · window · estimate"]
    P2 -->|"Arrow typed datapoints"| X2["exporter → backend"]
  end
  E1 ==>|"network — msgpack full / delta"| R2
  style P1 fill:#2b6cb0,color:#fff
  style P2 fill:#2b6cb0,color:#fff
```

The only real network copy is the `==>` hop. Delta-encoding
(`MsgpackDelta`, per-window against-empty) shrinks it.

---

## The four cases

All four are the **same processor**, parameterized by two knobs —
`transmit_sketch` (sketch vs. estimate out) and `encoding` (proto vs. msgpack,
full vs. delta) — deployed once (terminal) or twice (edge + central).

| Case | Topology | Config | Compute in node | What crosses the boundary |
|---|---|---|---|---|
| **1** — insert → merge → estimate, ship results | A | `transmit_sketch=false`, `quantiles=[…]` | insert **+ merge inbound** + estimate | typed OTAP datapoints (gauges) |
| **2** — insert only, ship msgpack full | B (edge) | `transmit_sketch=true`, `encoding=Msgpack` | insert + window | `_asap_envelope` Binary — **msgpack full** |
| **3** — insert-merge, ship msgpack delta | B (edge **+** central) | `encoding=MsgpackDelta`, `delta_transmission=true` | insert (edge); merge + estimate (central) | `_asap_envelope` Binary — **msgpack delta** (edge); gauges (central) |
| **4** — insert + estimate, ship via metric types | A | `transmit_sketch=false` | insert + estimate | OTAP Gauge / Sum datapoints |

**Case 1 ⊇ Case 4:** same terminal node; Case 1 also folds *inbound* sketch
envelopes (`observe_envelope`), Case 4 is raw-in only.

```mermaid
flowchart TD
  C{"cfg.transmit_sketch?"}
  C -->|"false → estimate mode"| A["<b>Topology A</b><br/>Cases 1 and 4<br/>emit gauges"]
  C -->|"true → sketch on wire"| D{"cfg.encoding?"}
  D -->|"Msgpack (full)"| B2["<b>Topology B</b> · Case 2<br/>ship full sketch"]
  D -->|"MsgpackDelta<br/>(+ delta_transmission)"| B3["<b>Topology B</b> · Case 3<br/>ship sparse delta<br/>+ central merger"]
  D -->|"ProtoFull / ProtoDelta"| B4["Topology B (proto variant)"]
```

---

## What each processor does (per-node)

```
process(msg):
  PData(batch)          -> ingest(batch)          # raw metrics OR inbound sketches
  Control(Wakeup)       -> flush()                # window close = the only emit
  Control(Config(plan)) -> update_config; ack
  Control(Shutdown)     -> flush()                # final drain

ingest(batch):
  flat = flatten(batch)                           # zero-copy Arrow projection
  input_mode == RawObs   -> for obs in decode_batch(flat):  pre.observe(obs)
  input_mode == Envelope -> for env in decode(flat):        pre.observe_envelope(env)

flush():   # serialize_series per closed series
  if !transmit_sketch:                            # ESTIMATE mode (Topology A)
     for p in sketch.estimate(quantiles):         # DDSketch/KLL: 1 gauge per quantile
        emit Gauge{ value=p.value, labels+=p.labels }   # HLL: 1 cardinality gauge
  else:                                           # SKETCH mode (Topology B)
     payload,enc = delta_transmission ? compute_delta(...) : snapshot()
     emit Envelope{ _asap_envelope=payload, encoding=enc }   # proto | msgpack (full/delta)
  effect_handler.send_message(batch)              # zero-copy hop downstream
```

---

## The zero-copy / serialization ledger

Every boundary and its true cost:

| Boundary | What moves | Cost |
|---|---|---|
| receiver → proc | raw metrics (OTAP Arrow) | **zero-copy** (`Arc` buffers, channel move) |
| decode inside proc | Arrow columns → `f64`/`bytes` → `sketch.update()` | **read-in-place**, no intermediate object |
| the windowed sketches | owned `asap_sketchlib` structs | **resident, never serialized while live** |
| inbound sketch (merge) | Binary col → prost/msgpack decode → struct → `merge()` | **one deserialize** per envelope |
| proc → next, **estimate mode** | small Arrow gauge batch (series × quantiles) | **one small alloc**, then zero-copy |
| proc → next, **sketch mode** | `to_msgpack()` / delta → bytes in Binary col | **one serialize** (sketch isn't columnar) |
| edge → central (gRPC) | the envelope batch | the **only real network copy**; delta shrinks it |
| control (`Wakeup`/`Config`/…) | tiny `NodeControlMsg` | negligible |

---

## Which computation is one processor vs. separate

- **Fused into one node** (all touch the single-owner window state — splitting
  would serialize the sketch across a channel): decode-raw, insert,
  merge-inbound-envelope, window rotation, estimate, encode-output.
- **A second processor only at the edge↔central tier boundary** (Case 3): the
  gRPC hop already forces a serialize, so nothing is lost. The central node is
  the *same* processor in `input=Envelope, output=Estimate` mode.
- **Stock OTAP nodes** around it — OTLP/OTAP receiver, OTAP/gRPC exporter,
  batch, retry, signal-router. Sketches ride the standard metrics batch in the
  `_asap_envelope` Binary carrier column, untouched by stock nodes.

---

## Implemented status (this repo)

- **Sketch-on-wire**: proto (`ProtoFull`/`ProtoDelta`) **and** msgpack
  (`Msgpack` full, `MsgpackDelta` sparse) for DDSketch / HLL / CMS / CountSketch.
  KLL is **full-only** (`Msgpack`; no delta form).
- **Estimate mode** (`transmit_sketch=false`): DDSketch / KLL emit one Gauge per
  configured quantile (`quantile` label); HLL emits a cardinality Gauge.
  CMS / CountSketch top-k output needs a heavy-hitter tracker (not yet built).
- **Cross-host merge**: exact for the count/quantile-bucket sketches; KLL
  cross-host *merge* is approximate (a pre-existing limitation of both wire
  formats — retained items carry level weights that item-replay drops).

Config knobs live on `PluginConfig` / `PrecomputeConfig`:
`encoding`, `delta_transmission`, `transmit_sketch`, `quantiles`.
