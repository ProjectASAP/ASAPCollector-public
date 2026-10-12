//! Keyed top-k over the same count-aligned windows as `model.rs`.
//!
//! `topk-exact` keeps a per-key count map per window, ships every entry and
//! sums the maps; `cmsheap` ships one ASAPv1 Count-Min-with-heap sketch per
//! source and merges sketches. Both answer the top `K` keys per window.
use super::model::{Meta, NormalizedBatch, MAX_PENDING};
use super::profile::{self, Category};
use asap_precompute_rs::{
    envelope::{SketchEnvelope, SketchType},
    observation::{KeyValue, Observation},
    otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata},
    precompute::FrequencySketch,
    sketches::CMSHeapWrapper,
};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::OnceLock;

pub const K: usize = 10;
const KEYS: usize = 1 << 20;
const ZIPF_S: f64 = 1.1;
const CMS_ROWS: usize = 4;
const CMS_COLS: usize = 2048;
const KEY_SEED: u64 = 0x2545_f491_4f6c_dd1d;
/// Minimum recall@K the sketch must reach in every window.
const MIN_RECALL: f64 = 0.9;

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Deterministic Zipf(s=1.1) key over 2^20 ranks; ranks are scrambled so the
/// heaviest keys are not small integers.
pub fn key(index: u64) -> u64 {
    static CDF: OnceLock<Vec<f64>> = OnceLock::new();
    let cdf = CDF.get_or_init(|| {
        let mut total = 0.0;
        let mut cdf = (1..=KEYS)
            .map(|rank| {
                total += (rank as f64).powf(-ZIPF_S);
                total
            })
            .collect::<Vec<_>>();
        cdf.iter_mut().for_each(|c| *c /= total);
        cdf
    });
    let unit = (mix(index.wrapping_add(KEY_SEED)) >> 11) as f64 / (1_u64 << 53) as f64;
    let rank = cdf.partition_point(|&c| c < unit).min(KEYS - 1) as u64;
    mix(rank ^ KEY_SEED)
}

/// Keys are already scrambled 64-bit values, so the exact baseline hashes
/// them with the identity rather than SipHash.
#[derive(Default)]
struct KeyHasher(u64);
impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 keys are hashed")
    }
    fn write_u64(&mut self, key: u64) {
        self.0 = key;
    }
}
type Counts = HashMap<u64, u64, BuildHasherDefault<KeyHasher>>;

/// Largest counts first; ties by key so exact answers are deterministic.
fn top(counts: &Counts) -> Vec<(u64, u64)> {
    let mut entries = counts.iter().map(|(k, c)| (*k, *c)).collect::<Vec<_>>();
    entries.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    entries.truncate(K);
    entries
}

fn label(o: &Observation, name: &str) -> Result<u64, String> {
    o.labels
        .iter()
        .find(|kv| kv.key == name)
        .ok_or_else(|| format!("missing {name}"))?
        .value
        .parse()
        .map_err(|e| format!("{name}: {e}"))
}

fn counts_pdata(entries: &[(u64, u64)], total: usize, meta: Meta) -> Result<OtapPdata, String> {
    let _cpu = profile::scope(Category::PdataCodec);
    let envelopes = entries
        .iter()
        .map(|(key, count)| {
            let mut env = meta.envelope(*count as f64);
            env.labels.push(KeyValue::new("topk.key", key.to_string()));
            env.labels
                .push(KeyValue::new("bench.total", total.to_string()));
            env
        })
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&envelopes).map_err(|e| e.to_string())
}

fn sketch_pdata(sketch: &CMSHeapWrapper, count: usize, meta: Meta) -> Result<OtapPdata, String> {
    use asap_precompute_rs::precompute::Sketch;
    let _cpu = profile::scope(Category::PdataCodec);
    let mut env: SketchEnvelope = meta.envelope(0.0);
    env.sketch_type = SketchType::CountMinSketchWithHeap;
    env.payload = {
        let _cpu = profile::scope(Category::SketchCodec);
        sketch.snapshot().map_err(|e| e.to_string())?
    };
    env.count = count as u64;
    encode_envelopes_to_pdata(&[env]).map_err(|e| e.to_string())
}

fn decode_sketch(o: &Observation, count: usize) -> Result<CMSHeapWrapper, String> {
    let _cpu = profile::scope(Category::SketchCodec);
    let env = o.value.envelope.as_ref().ok_or("missing sketch envelope")?;
    if env.count != count as u64 || env.sketch_type != SketchType::CountMinSketchWithHeap {
        return Err("sketch count/type mismatch".into());
    }
    CMSHeapWrapper::from_snapshot(&env.payload).map_err(|e| e.to_string())
}

fn sketch_top(sketch: &CMSHeapWrapper) -> Vec<(u64, u64)> {
    sketch
        .top_k(K)
        .into_iter()
        .map(|e| {
            let key = u64::from_be_bytes(e.key.as_slice().try_into().expect("8-byte key"));
            (key, e.count as u64)
        })
        .collect()
}

fn topk_pdata(results: &[(&str, Vec<(u64, u64)>)], meta: Meta) -> Result<OtapPdata, String> {
    let _cpu = profile::scope(Category::PdataCodec);
    let envelopes = results
        .iter()
        .flat_map(|(kind, entries)| {
            entries.iter().enumerate().map(move |(rank, (key, count))| {
                let mut env = meta.envelope(*count as f64);
                env.metric_name = "http.server.request.topk".into();
                env.labels.push(KeyValue::new("topk.key", key.to_string()));
                env.labels
                    .push(KeyValue::new("topk.rank", (rank + 1).to_string()));
                env.labels.push(KeyValue::new("window_kind", *kind));
                env
            })
        })
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&envelopes).map_err(|e| e.to_string())
}

struct Window {
    /// Signals (branch) or entries (merge/estimate) received per source.
    received: [usize; 3],
    /// Entries expected per source, once its first chunk arrives.
    total: [Option<usize>; 3],
    counts: Counts,
    sketch: Option<CMSHeapWrapper>,
    started_ns: u64,
}
impl Window {
    fn new(started_ns: u64) -> Self {
        Self {
            received: [0; 3],
            total: [None; 3],
            counts: Counts::default(),
            sketch: None,
            started_ns,
        }
    }
}

pub struct Processor {
    sketch: bool,
    role: String,
    points: usize,
    batch: usize,
    windows: BTreeMap<u64, Window>,
    next: [u64; 3],
    rolling: usize,
    count_history: VecDeque<Counts>,
    sketch_history: VecDeque<CMSHeapWrapper>,
}
impl Processor {
    pub fn new(scenario: &str, role: &str, points: usize, batch: usize, rolling: usize) -> Self {
        Self {
            sketch: scenario == "cmsheap",
            role: role.into(),
            points,
            batch,
            windows: BTreeMap::new(),
            next: [0; 3],
            rolling,
            count_history: VecDeque::new(),
            sketch_history: VecDeque::new(),
        }
    }

    fn new_sketch() -> CMSHeapWrapper {
        CMSHeapWrapper::new(CMS_ROWS, CMS_COLS, K)
    }

    /// Branch input: the normalized corpus indices of one source.
    pub fn process_normalized(&mut self, batch: NormalizedBatch) -> Result<Vec<OtapPdata>, String> {
        let _cpu = profile::scope(Category::ProcessorBookkeeping);
        let meta = batch.meta;
        if self.role != "branch" || meta.source > 1 || meta.window != self.next[meta.source] {
            return Err("duplicate or out-of-order window".into());
        }
        let w = self
            .windows
            .entry(meta.window)
            .or_insert_with(|| Window::new(meta.started_ns));
        if meta.offset != w.received[meta.source] || meta.offset + batch.count > self.points {
            return Err("duplicate, missing, or overflowing batch".into());
        }
        {
            let _cpu = profile::scope(Category::Computation);
            let keys = (0..batch.count as u64).map(|i| key(batch.first_index + i));
            if self.sketch {
                let sketch = w.sketch.get_or_insert_with(Self::new_sketch);
                keys.for_each(|k| sketch.update(&k.to_be_bytes(), 1));
            } else {
                keys.for_each(|k| *w.counts.entry(k).or_default() += 1);
            }
        }
        w.received[meta.source] += batch.count;
        if w.received[meta.source] < self.points {
            if self.windows.len() > MAX_PENDING as usize {
                return Err("pending window bound exceeded".into());
            }
            return Ok(vec![]);
        }
        self.next[meta.source] += 1;
        let w = self.windows.remove(&meta.window).unwrap();
        let out = Meta {
            offset: 0,
            started_ns: w.started_ns,
            ..meta
        };
        self.emit(w, out, self.points)
    }

    /// Ship a completed window: one sketch, or every count entry in chunks.
    fn emit(&self, w: Window, meta: Meta, signals: usize) -> Result<Vec<OtapPdata>, String> {
        if self.sketch {
            return Ok(vec![sketch_pdata(
                w.sketch.as_ref().unwrap(),
                signals,
                meta,
            )?]);
        }
        let entries = w.counts.into_iter().collect::<Vec<_>>();
        let total = entries.len();
        entries
            .chunks(self.batch)
            .enumerate()
            .map(|(i, chunk)| {
                counts_pdata(
                    chunk,
                    total,
                    Meta {
                        offset: i * self.batch,
                        ..meta
                    },
                )
            })
            .collect()
    }

    /// Merge and estimate input.
    pub fn process(&mut self, pdata: OtapPdata) -> Result<Vec<OtapPdata>, String> {
        let _cpu = profile::scope(Category::ProcessorBookkeeping);
        let obs = {
            let _cpu = profile::scope(Category::PdataCodec);
            decode_pdata_to_observations(pdata)
                .map_err(|e| e.to_string())?
                .observations
        };
        let meta = Meta::read(obs.first().ok_or("empty batch")?)?;
        let estimate = self.role == "estimate";
        if (estimate && meta.source != 2) || (!estimate && meta.source > 1) {
            return Err("wrong source for role".into());
        }
        if meta.window != self.next[meta.source] {
            return Err("duplicate or out-of-order window".into());
        }
        let signals = if estimate {
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
        let _cpu = profile::scope(Category::Computation);
        if self.sketch {
            if obs.len() != 1 {
                return Err("expected one sketch".into());
            }
            let decoded = decode_sketch(&obs[0], signals)?;
            match &mut w.sketch {
                Some(sketch) => sketch.merge_from(&decoded).map_err(|e| e.to_string())?,
                None => w.sketch = Some(decoded),
            }
            w.received[meta.source] = 1;
            w.total[meta.source] = Some(1);
        } else {
            let total = label(&obs[0], "bench.total")? as usize;
            if *w.total[meta.source].get_or_insert(total) != total
                || w.received[meta.source] + obs.len() > total
            {
                return Err("count entry overflow".into());
            }
            for o in &obs {
                *w.counts.entry(label(o, "topk.key")?).or_default() += o.value.float as u64;
            }
            w.received[meta.source] += obs.len();
        }
        let done = |s: usize| w.total[s] == Some(w.received[s]);
        if done(meta.source) {
            self.next[meta.source] += 1;
        }
        let complete = if estimate {
            done(2)
        } else {
            done(0) && done(1)
        };
        if !complete {
            if self.windows.len() > MAX_PENDING as usize {
                return Err("pending window bound exceeded".into());
            }
            return Ok(vec![]);
        }
        let w = self.windows.remove(&meta.window).unwrap();
        let out = Meta {
            source: 2,
            offset: 0,
            started_ns: w.started_ns,
            ..meta
        };
        if !estimate {
            return self.emit(w, out, self.points * 2);
        }
        let mut results = vec![("tumbling", Vec::new())];
        if self.sketch {
            let sketch = w.sketch.unwrap();
            results[0].1 = sketch_top(&sketch);
            if self.rolling > 1 {
                self.sketch_history.push_back(sketch);
                if self.sketch_history.len() > self.rolling {
                    self.sketch_history.pop_front();
                }
                let mut merged = Self::new_sketch();
                for s in &self.sketch_history {
                    merged.merge_from(s).map_err(|e| e.to_string())?;
                }
                results.push(("rolling", sketch_top(&merged)));
            }
        } else {
            results[0].1 = top(&w.counts);
            if self.rolling > 1 {
                self.count_history.push_back(w.counts);
                if self.count_history.len() > self.rolling {
                    self.count_history.pop_front();
                }
                let mut merged = Counts::default();
                for counts in &self.count_history {
                    for (k, c) in counts {
                        *merged.entry(*k).or_default() += c;
                    }
                }
                results.push(("rolling", top(&merged)));
            }
        }
        Ok(vec![topk_pdata(&results, out)?])
    }
}

/// One reported top-k entry and its exact count, for dashboards.
#[derive(Serialize, Clone, Debug, Default)]
pub struct TopkPoint {
    pub window_kind: String,
    pub rank: usize,
    pub key: String,
    pub estimate: f64,
    pub exact: f64,
}
/// Accuracy of one window's answer against the exact top-k.
#[derive(Serialize, Clone, Debug, Default)]
pub struct TopkAccuracy {
    pub window_kind: String,
    pub recall: f64,
    pub mean_relative_error: f64,
}

pub struct Backend {
    sketch: bool,
    rolling: usize,
    /// Exact per-key counts of one window; every window replays the corpus.
    counts: Counts,
    reference: Vec<(u64, u64)>,
    next: u64,
    pub latest: Vec<TopkPoint>,
    pub accuracy: Vec<TopkAccuracy>,
}
impl Backend {
    pub fn new(scenario: &str, points: usize, rolling: usize) -> Self {
        let mut counts = Counts::default();
        for index in 1..=points as u64 * 2 {
            *counts.entry(key(index)).or_default() += 1;
        }
        Self {
            sketch: scenario == "cmsheap",
            rolling,
            reference: top(&counts),
            counts,
            next: 0,
            latest: Vec::new(),
            accuracy: Vec::new(),
        }
    }

    /// Validate one window's answers; returns the window id and start time.
    pub fn receive(&mut self, obs: &[Observation]) -> Result<(u64, u64), String> {
        let meta = Meta::read(obs.first().ok_or("empty backend batch")?)?;
        let mut kinds = vec![("tumbling", 1)];
        if self.rolling > 1 {
            kinds.push(("rolling", (meta.window as usize + 1).min(self.rolling)));
        }
        if meta.window != self.next || meta.source != 2 || obs.len() != kinds.len() * K {
            return Err("top-k window missing/duplicated".into());
        }
        let (mut latest, mut accuracy) = (Vec::new(), Vec::new());
        for (kind, copies) in kinds {
            let mut answer = obs
                .iter()
                .filter(|o| {
                    o.labels
                        .iter()
                        .any(|kv| kv.key == "window_kind" && kv.value == kind)
                })
                .map(|o| Ok((label(o, "topk.rank")?, label(o, "topk.key")?, o.value.float)))
                .collect::<Result<Vec<_>, String>>()?;
            answer.sort_by_key(|(rank, _, _)| *rank);
            if answer.len() != K {
                return Err(format!("{kind}: expected {K} entries"));
            }
            let exact =
                |key: u64| (self.counts.get(&key).copied().unwrap_or(0) * copies as u64) as f64;
            let found = |key: u64| answer.iter().find(|(_, k, _)| *k == key).map(|e| e.2);
            let hits = self
                .reference
                .iter()
                .filter(|(k, _)| found(*k).is_some())
                .count();
            let recall = hits as f64 / K as f64;
            let error = self
                .reference
                .iter()
                .map(|(k, _)| (found(*k).unwrap_or(0.0) - exact(*k)).abs() / exact(*k))
                .sum::<f64>()
                / K as f64;
            let expected = self
                .reference
                .iter()
                .map(|(k, c)| (*k, (c * copies as u64) as f64))
                .collect::<Vec<_>>();
            let actual = answer.iter().map(|(_, k, c)| (*k, *c)).collect::<Vec<_>>();
            if (!self.sketch && actual != expected) || recall < MIN_RECALL {
                return Err(format!(
                    "{kind}: recall {recall}, answer {actual:?} != {expected:?}"
                ));
            }
            for (rank, k, c) in &answer {
                latest.push(TopkPoint {
                    window_kind: kind.into(),
                    rank: *rank as usize,
                    key: format!("{k:016x}"),
                    estimate: *c,
                    exact: exact(*k),
                });
            }
            accuracy.push(TopkAccuracy {
                window_kind: kind.into(),
                recall,
                mean_relative_error: error,
            });
        }
        self.latest = latest;
        self.accuracy = accuracy;
        self.next += 1;
        Ok((meta.window, meta.started_ns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_precompute_rs::otap::codec::decode_pdata_to_observations;

    fn exercise(scenario: &str, rolling: usize) -> Backend {
        let (n, batch) = (4096, 300);
        let mut branches = [0, 1].map(|_| Processor::new(scenario, "branch", n, batch, rolling));
        let mut merge = Processor::new(scenario, "merge", n, batch, rolling);
        let mut estimate = Processor::new(scenario, "estimate", n, batch, rolling);
        let mut backend = Backend::new(scenario, n, rolling);
        let mut completed = 0;
        for window in 0..3 {
            for offset in (0..n).step_by(batch) {
                for (source, branch) in branches.iter_mut().enumerate() {
                    let input = NormalizedBatch {
                        first_index: (source * n + offset + 1) as u64,
                        count: (offset + batch).min(n) - offset,
                        meta: Meta {
                            window,
                            source,
                            offset,
                            started_ns: 1,
                        },
                    };
                    for a in branch.process_normalized(input).unwrap() {
                        for m in merge.process(a).unwrap() {
                            for e in estimate.process(m).unwrap() {
                                let obs = decode_pdata_to_observations(e).unwrap().observations;
                                assert_eq!(backend.receive(&obs).unwrap().0, completed);
                                completed += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(completed, 3);
        backend
    }

    #[test]
    fn exact_top_k_matches_reference() {
        let backend = exercise("topk-exact", 2);
        assert!(backend
            .accuracy
            .iter()
            .all(|a| a.recall == 1.0 && a.mean_relative_error == 0.0));
        assert_eq!(backend.latest.len(), 2 * K);
    }

    #[test]
    fn sketch_top_k_meets_recall_bound() {
        let backend = exercise("cmsheap", 2);
        assert!(backend.accuracy.iter().all(|a| a.recall >= MIN_RECALL));
    }

    #[test]
    fn keys_are_heavy_tailed() {
        let mut counts = Counts::default();
        (1..=100_000).for_each(|i| *counts.entry(key(i)).or_default() += 1);
        let heaviest = top(&counts)[0].1;
        assert!(
            heaviest > 5_000 && counts.len() > 10_000,
            "{heaviest} {}",
            counts.len()
        );
    }
}
