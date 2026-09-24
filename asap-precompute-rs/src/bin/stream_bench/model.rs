//! Count-aligned windows for a fair streaming comparison. No wall-clock flush races.
use asap_precompute_rs::{
    envelope::{Encoding, SketchEnvelope, SketchType},
    observation::{KeyValue, Observation},
    otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata},
    precompute::{QuantileSketch, Sketch},
    sketches::KLLWrapper,
};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use std::collections::BTreeMap;

pub const MAX_PENDING: u64 = 4;
pub const SEED: u64 = 0x9e37_79b9_7f4a_7c15;
pub fn value(index: u64) -> f64 {
    let mut x = index.wrapping_add(SEED);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    let unit = (x >> 11) as f64 / (1_u64 << 53) as f64;
    if index.is_multiple_of(20) {
        1.0 + unit * 0.4
    } else {
        0.005 + unit * 0.1
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Meta {
    pub window: u64,
    pub source: usize,
    pub offset: usize,
    pub started_ns: u64,
}
impl Meta {
    pub fn read(o: &Observation) -> Result<Self, String> {
        let get = |name: &str| -> Result<u64, String> {
            o.labels
                .iter()
                .find(|kv| kv.key == name)
                .ok_or_else(|| format!("missing {name}"))?
                .value
                .parse()
                .map_err(|e| format!("{e}"))
        };
        Ok(Self {
            window: get("bench.window")?,
            source: get("bench.source")? as usize,
            offset: get("bench.offset")? as usize,
            started_ns: get("bench.started_ns")?,
        })
    }
    fn envelope(self, value: f64) -> SketchEnvelope {
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 7,
            resource_labels: vec![KeyValue::new("service.name", "checkout")],
            labels: vec![
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", "/checkout"),
                KeyValue::new("http.response.status_code", "200"),
                KeyValue::new("bench.window", self.window.to_string()),
                KeyValue::new("bench.source", self.source.to_string()),
                KeyValue::new("bench.offset", self.offset.to_string()),
                KeyValue::new("bench.started_ns", self.started_ns.to_string()),
            ],
            window_start_ms: 1000,
            window_end_ms: 2000,
            encoding: Encoding::Unspecified,
            payload: vec![],
            hash_spec: None,
            metric_name: "http.server.request.duration".into(),
            count: 0,
            aggregation_temporality: 0,
            value,
        }
    }
}
pub fn values_pdata(values: &[f64], meta: Meta) -> Result<OtapPdata, String> {
    encode_envelopes_to_pdata(&values.iter().map(|v| meta.envelope(*v)).collect::<Vec<_>>())
        .map_err(|e| e.to_string())
}
fn new_sketch(seed: u64) -> KLLWrapper {
    KLLWrapper::new(400, Some(seed)).with_wire_encoding(Encoding::Msgpack)
}
fn sketch_pdata(sketch: &KLLWrapper, count: usize, meta: Meta) -> Result<OtapPdata, String> {
    let mut env = meta.envelope(0.0);
    env.sketch_type = SketchType::KLLSketch;
    env.encoding = Encoding::Msgpack;
    env.payload = sketch.snapshot().map_err(|e| e.to_string())?;
    env.count = count as u64;
    encode_envelopes_to_pdata(&[env]).map_err(|e| e.to_string())
}
fn decode_sketch(o: &Observation, count: usize) -> Result<KLLWrapper, String> {
    let env = o.value.envelope.as_ref().ok_or("missing sketch envelope")?;
    if env.count != count as u64 {
        return Err("sketch count mismatch".into());
    }
    let mut sketch = new_sketch(99);
    sketch
        .apply_delta_encoded(&env.payload, env.encoding)
        .map_err(|e| e.to_string())?;
    Ok(sketch)
}
fn quantiles_pdata(values: [f64; 2], meta: Meta) -> Result<OtapPdata, String> {
    let envelopes = ["0.5", "0.99"]
        .into_iter()
        .zip(values)
        .map(|(q, value)| {
            let mut env = meta.envelope(value);
            env.metric_name = "http.server.request.duration.estimate".into();
            env.labels.push(KeyValue::new("quantile", q));
            env
        })
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&envelopes).map_err(|e| e.to_string())
}
pub fn exact(values: &[f64]) -> [f64; 2] {
    [0.5, 0.99].map(|q| values[((values.len() - 1) as f64 * q).round() as usize])
}
pub fn reference(points: usize) -> [f64; 2] {
    let mut values = (1..=points as u64 * 2).map(value).collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    exact(&values)
}

struct Window {
    received: [usize; 3],
    runs: [Vec<f64>; 3],
    sketch: KLLWrapper,
    started_ns: u64,
}
impl Window {
    fn new(started_ns: u64) -> Self {
        Self {
            received: [0; 3],
            runs: Default::default(),
            sketch: new_sketch(3),
            started_ns,
        }
    }
}
pub struct Processor {
    pub scenario: String,
    pub role: String,
    pub points: usize,
    pub batch: usize,
    windows: BTreeMap<u64, Window>,
    next: [u64; 3],
}
impl Processor {
    pub fn new(scenario: &str, role: &str, points: usize, batch: usize) -> Self {
        Self {
            scenario: scenario.into(),
            role: role.into(),
            points,
            batch,
            windows: BTreeMap::new(),
            next: [0; 3],
        }
    }
    pub fn process(&mut self, pdata: OtapPdata) -> Result<Vec<OtapPdata>, String> {
        // Raw is a real pass-through: no artificial decoding/sorting at intermediate hops.
        if self.scenario == "raw" {
            return Ok(vec![pdata]);
        }
        let obs = decode_pdata_to_observations(pdata)
            .map_err(|e| e.to_string())?
            .observations;
        let meta = Meta::read(obs.first().ok_or("empty batch")?)?;
        if meta.source > 2 || meta.window != self.next[meta.source] {
            return Err("duplicate or out-of-order window".into());
        }
        for o in &obs {
            let other = Meta::read(o)?;
            if (other.window, other.source, other.offset, other.started_ns)
                != (meta.window, meta.source, meta.offset, meta.started_ns)
            {
                return Err("mixed batch metadata".into());
            }
        }
        let branch = self.role == "branch";
        let estimate = self.role == "estimate";
        if (estimate && meta.source != 2) || (!estimate && meta.source > 1) {
            return Err("wrong source for role".into());
        }
        let count = if estimate {
            self.points * 2
        } else {
            self.points
        };
        let w = self
            .windows
            .entry(meta.window)
            .or_insert_with(|| Window::new(meta.started_ns));
        w.started_ns = w.started_ns.min(meta.started_ns);
        if meta.offset != w.received[meta.source] {
            return Err("duplicate or missing batch".into());
        }
        if self.scenario == "kll" && !branch {
            if obs.len() != 1 {
                return Err("expected one sketch".into());
            }
            w.sketch
                .merge(&decode_sketch(&obs[0], count)?)
                .map_err(|e| e.to_string())?;
            w.received[meta.source] += count;
        } else {
            if w.received[meta.source] + obs.len() > count {
                return Err("window overflow".into());
            }
            for o in &obs {
                if self.scenario == "kll" {
                    w.sketch.update(o.value.float);
                } else {
                    w.runs[meta.source].push(o.value.float);
                }
            }
            w.received[meta.source] += obs.len();
        }
        if w.received[meta.source] == count {
            self.next[meta.source] += 1;
        }
        let complete = if branch || estimate {
            w.received[meta.source] == count
        } else {
            w.received[0] == count && w.received[1] == count
        };
        if !complete {
            if self.windows.len() > MAX_PENDING as usize {
                return Err("pending window bound exceeded".into());
            }
            return Ok(vec![]);
        }
        let mut w = self.windows.remove(&meta.window).unwrap();
        let out_meta = Meta {
            source: if branch { meta.source } else { 2 },
            offset: 0,
            started_ns: w.started_ns,
            ..meta
        };
        if self.scenario == "kll" {
            return Ok(vec![if estimate {
                quantiles_pdata([w.sketch.quantile(0.5), w.sketch.quantile(0.99)], out_meta)?
            } else {
                sketch_pdata(
                    &w.sketch,
                    if branch { self.points } else { self.points * 2 },
                    out_meta,
                )?
            }]);
        }
        let values = if branch {
            let values = &mut w.runs[meta.source];
            values.sort_by(f64::total_cmp);
            std::mem::take(values)
        } else if estimate {
            std::mem::take(&mut w.runs[2])
        } else {
            let (a, b) = (&w.runs[0], &w.runs[1]);
            let (mut i, mut j) = (0, 0);
            let mut values = Vec::with_capacity(self.points * 2);
            while i < a.len() && j < b.len() {
                if a[i].total_cmp(&b[j]).is_le() {
                    values.push(a[i]);
                    i += 1;
                } else {
                    values.push(b[j]);
                    j += 1;
                }
            }
            values.extend_from_slice(&a[i..]);
            values.extend_from_slice(&b[j..]);
            values
        };
        if estimate {
            return Ok(vec![quantiles_pdata(exact(&values), out_meta)?]);
        }
        values
            .chunks(self.batch)
            .enumerate()
            .map(|(i, chunk)| {
                values_pdata(
                    chunk,
                    Meta {
                        offset: i * self.batch,
                        ..out_meta
                    },
                )
            })
            .collect()
    }
}

pub struct Backend {
    scenario: String,
    points: usize,
    expected: [f64; 2],
    windows: BTreeMap<u64, ([usize; 2], u64)>,
    next: [u64; 2],
    next_quantile: u64,
}
impl Backend {
    pub fn new(scenario: &str, points: usize) -> Self {
        Self {
            scenario: scenario.into(),
            points,
            expected: reference(points),
            windows: BTreeMap::new(),
            next: [0; 2],
            next_quantile: 0,
        }
    }
    /// Return completed input signals and optional (window id, start timestamp).
    pub fn receive(&mut self, pdata: OtapPdata) -> Result<(u64, Option<(u64, u64)>), String> {
        let obs = decode_pdata_to_observations(pdata)
            .map_err(|e| e.to_string())?
            .observations;
        let meta = Meta::read(obs.first().ok_or("empty backend batch")?)?;
        if self.scenario == "raw" {
            if meta.source > 1 || meta.window != self.next[meta.source] {
                return Err("raw window missing/duplicated".into());
            }
            let w = self
                .windows
                .entry(meta.window)
                .or_insert(([0; 2], meta.started_ns));
            if meta.offset != w.0[meta.source] || meta.offset + obs.len() > self.points {
                return Err("raw batch missing/duplicated".into());
            }
            for (i, o) in obs.iter().enumerate() {
                let actual = Meta::read(o)?;
                if (actual.window, actual.source, actual.offset)
                    != (meta.window, meta.source, meta.offset)
                    || o.value.float
                        != value((meta.source * self.points + meta.offset + i + 1) as u64)
                    || o.metric != "http.server.request.duration"
                {
                    return Err("raw payload mismatch".into());
                }
            }
            w.0[meta.source] += obs.len();
            w.1 = w.1.min(meta.started_ns);
            if w.0[meta.source] == self.points {
                self.next[meta.source] += 1;
            }
            let completed = if w.0 == [self.points; 2] {
                let start = w.1;
                self.windows.remove(&meta.window);
                Some((meta.window, start))
            } else {
                None
            };
            if self.windows.len() > MAX_PENDING as usize {
                return Err("backend pending window bound exceeded".into());
            }
            return Ok((obs.len() as u64, completed));
        }
        if meta.window != self.next_quantile || meta.source != 2 || obs.len() != 2 {
            return Err("quantile window missing/duplicated".into());
        }
        for (q, expected) in ["0.5", "0.99"].into_iter().zip(self.expected) {
            let matching = obs
                .iter()
                .filter(|o| {
                    o.labels
                        .iter()
                        .any(|kv| kv.key == "quantile" && kv.value == q)
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err("missing/duplicate quantile".into());
            }
            let o = matching[0];
            let other = Meta::read(o)?;
            let tolerance = if self.scenario == "exact" {
                0.0
            } else {
                (expected.abs() * 0.05).max(0.001)
            };
            if other.window != meta.window
                || other.source != 2
                || !o.value.float.is_finite()
                || (o.value.float - expected).abs() > tolerance
            {
                return Err(format!("{q}: {} != {expected}", o.value.float));
            }
        }
        self.next_quantile += 1;
        Ok((
            (self.points * 2) as u64,
            Some((meta.window, meta.started_ns)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn exercise(scenario: &str) {
        let (n, batch) = (1024, 127);
        let mut branches = [
            Processor::new(scenario, "branch", n, batch),
            Processor::new(scenario, "branch", n, batch),
        ];
        let mut merge = Processor::new(scenario, "merge", n, batch);
        let mut estimate = Processor::new(scenario, "estimate", n, batch);
        let mut backend = Backend::new(scenario, n);
        let (mut received, mut completed) = (0, 0);
        for window in 0..3 {
            for offset in (0..n).step_by(batch) {
                for (source, branch) in branches.iter_mut().enumerate() {
                    let values = (offset..(offset + batch).min(n))
                        .map(|i| value((source * n + i + 1) as u64))
                        .collect::<Vec<_>>();
                    let input = values_pdata(
                        &values,
                        Meta {
                            window,
                            source,
                            offset,
                            started_ns: 1,
                        },
                    )
                    .unwrap();
                    for a in branch.process(input).unwrap() {
                        for m in merge.process(a).unwrap() {
                            for e in estimate.process(m).unwrap() {
                                let (count, done) = backend.receive(e).unwrap();
                                received += count;
                                completed += u64::from(done.is_some());
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(received, 6 * n as u64);
        assert_eq!(completed, 3);
    }
    #[test]
    fn streaming_windows_survive_batch_boundaries() {
        for scenario in ["raw", "exact", "kll"] {
            exercise(scenario);
        }
    }
    #[test]
    fn rejects_duplicate_and_missing_batches() {
        let meta = Meta {
            window: 0,
            source: 0,
            offset: 0,
            started_ns: 1,
        };
        let input = || values_pdata(&[value(1)], meta).unwrap();
        let mut backend = Backend::new("raw", 10);
        backend.receive(input()).unwrap();
        assert!(backend.receive(input()).is_err());
        let mut processor = Processor::new("kll", "branch", 10, 2);
        assert!(processor
            .process(values_pdata(&[value(1)], Meta { offset: 1, ..meta }).unwrap())
            .is_err());
    }
}
