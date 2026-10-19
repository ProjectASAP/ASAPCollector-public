//! Serialization and many-source merge cost per sketch type.
//!
//! For each deployed sketch type and N source sketches, times ASAPv1 encode
//! (`snapshot`) and decode (`from_snapshot`) of one sketch, merging all N
//! byte payloads the way `window_merge` does (`decode_snapshot` for the first,
//! `merge_snapshot` for the rest), and merging N already-decoded sketches
//! directly with sketchlib's native `merge`. Each timing is the median of
//! several runs. CSV on stdout, a readable table on stderr.
//!
//! Usage: `asap-merge-bench [--quick]`

use std::hint::black_box;
use std::time::{Duration, Instant};

use asap_precompute_rs::envelope::SketchType;
use asap_precompute_rs::precompute::{PrecomputeError, Sketch};
use asap_precompute_rs::sketches::{
    decode_snapshot, CMSHeapWrapper, CMSWrapper, CountSketchWrapper, DDSketchWrapper, HLLWrapper,
    KLLWrapper,
};
use asap_sketchlib::sketches::KLL;
use asap_sketchlib::{
    CMSHeap, Count, CountMin, DDSketch, ErtlMLE, HyperLogLog, RegularPath, Vector2D,
};

/// splitmix64; enough randomness for realistic sketch contents.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in (0, 1).
    fn unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    /// Lognormal latency-like value (median ~e^3 ≈ 20, sigma 1).
    fn lognormal(&mut self) -> f64 {
        let z = (-2.0 * self.unit().ln()).sqrt() * (std::f64::consts::TAU * self.unit()).cos();
        (3.0 + z).exp()
    }

    /// Zipf(s≈1)-ish rank over 2^20 keys: P(rank ≈ r) ∝ 1/r.
    fn zipf_key(&mut self) -> u64 {
        ((self.unit() * 20.0 * std::f64::consts::LN_2).exp() as u64 - 1).min((1 << 20) - 1)
    }
}

/// Builds source sketch `source` filled with `points` values.
fn fill(sketch_type: SketchType, source: u64, points: u64) -> Box<dyn Sketch> {
    let mut rng = Rng(source.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xA5A5);
    match sketch_type {
        SketchType::KLLSketch => {
            let mut w = KLLWrapper::new(200, None);
            w.update_batch((0..points).map(|_| rng.lognormal()));
            Box::new(w)
        }
        SketchType::DDSketch => {
            let mut w = DDSketchWrapper::new(0.01);
            (0..points).for_each(|_| w.update(rng.lognormal()));
            Box::new(w)
        }
        SketchType::HLLSketch => {
            let mut w = HLLWrapper::new();
            (0..points).for_each(|i| w.update(&(source * points + i).to_be_bytes()));
            Box::new(w)
        }
        SketchType::CountMinSketch => {
            let mut w = CMSWrapper::new(4, 2048);
            (0..points).for_each(|_| w.update(&rng.zipf_key().to_be_bytes(), 1));
            Box::new(w)
        }
        SketchType::CountMinSketchWithHeap => {
            let mut w = CMSHeapWrapper::new(4, 2048, 10);
            (0..points).for_each(|_| w.update(&rng.zipf_key().to_be_bytes(), 1));
            Box::new(w)
        }
        SketchType::CountSketch => {
            let mut w = CountSketchWrapper::new(4, 2048);
            (0..points).for_each(|_| w.update(&rng.zipf_key().to_be_bytes(), 1));
            Box::new(w)
        }
        SketchType::Unspecified | SketchType::Exact => unreachable!(),
    }
}

/// Median wall time of `runs` calls to `f`.
fn median(runs: usize, mut f: impl FnMut()) -> Duration {
    let mut times: Vec<Duration> = (0..runs)
        .map(|_| {
            let start = Instant::now();
            f();
            start.elapsed()
        })
        .collect();
    times.sort();
    times[times.len() / 2]
}

/// Median time to merge every payload by decoding natively, then `merge`.
/// Decoding happens outside the timed region.
fn direct_merge<T>(
    runs: usize,
    payloads: &[Vec<u8>],
    decode: impl Fn(&[u8]) -> T,
    merge: impl Fn(&mut T, &T),
) -> Duration {
    let decoded: Vec<T> = payloads.iter().map(|b| decode(b)).collect();
    median(runs, || {
        let mut acc = decode(&payloads[0]);
        for other in &decoded[1..] {
            merge(&mut acc, other);
        }
        black_box(&acc);
    })
}

fn direct_merge_for(sketch_type: SketchType, runs: usize, payloads: &[Vec<u8>]) -> Duration {
    type Cms = CountMin<Vector2D<i64>, RegularPath>;
    type Cs = Count<Vector2D<i64>, RegularPath>;
    match sketch_type {
        SketchType::KLLSketch => direct_merge(
            runs,
            payloads,
            |b| KLL::<f64>::deserialize_from_bytes(b).unwrap(),
            |a, o| a.merge(o),
        ),
        SketchType::DDSketch => direct_merge(
            runs,
            payloads,
            |b| DDSketch::deserialize_from_bytes(b).unwrap(),
            |a, o| a.merge(o).unwrap(),
        ),
        SketchType::HLLSketch => direct_merge(
            runs,
            payloads,
            |b| HyperLogLog::<ErtlMLE>::deserialize_from_bytes(b).unwrap(),
            |a, o| a.merge(o),
        ),
        SketchType::CountMinSketch => direct_merge(
            runs,
            payloads,
            |b| Cms::deserialize_from_bytes(b).unwrap(),
            |a, o| a.merge(o),
        ),
        SketchType::CountMinSketchWithHeap => direct_merge(
            runs,
            payloads,
            |b| CMSHeap::deserialize_from_bytes(b).unwrap(),
            |a: &mut CMSHeap, o: &CMSHeap| a.merge(o),
        ),
        SketchType::CountSketch => direct_merge(
            runs,
            payloads,
            |b| Cs::deserialize_from_bytes(b).unwrap(),
            |a, o| a.merge(o),
        ),
        SketchType::Unspecified | SketchType::Exact => unreachable!(),
    }
}

fn main() -> Result<(), PrecomputeError> {
    let quick = std::env::args().any(|a| a == "--quick");
    let (source_counts, points, runs): (&[u64], u64, usize) = if quick {
        (&[10, 100], 2_000, 3)
    } else {
        (&[10, 100, 1000], 20_000, 5)
    };
    let types = [
        ("kll", SketchType::KLLSketch),
        ("ddsketch", SketchType::DDSketch),
        ("hll", SketchType::HLLSketch),
        ("countminsketch", SketchType::CountMinSketch),
        ("countminsketch_heap", SketchType::CountMinSketchWithHeap),
        ("countsketch", SketchType::CountSketch),
    ];

    println!("sketch,sources,bytes_per_sketch,encode_us,decode_us,merge_all_ms,merge_per_sketch_us,direct_merge_all_ms");
    eprintln!("{points} points per source sketch, median of {runs} runs");
    eprintln!(
        "{:<20} {:>7} {:>10} {:>10} {:>10} {:>13} {:>13} {:>13}",
        "sketch",
        "sources",
        "bytes",
        "encode_us",
        "decode_us",
        "merge_all_ms",
        "per_sketch_us",
        "direct_ms"
    );
    for (name, sketch_type) in types {
        for &n in source_counts {
            let sources: Vec<Box<dyn Sketch>> =
                (0..n).map(|s| fill(sketch_type, s, points)).collect();
            let payloads = sources
                .iter()
                .map(|s| s.snapshot())
                .collect::<Result<Vec<_>, _>>()?;
            let bytes = payloads.iter().map(Vec::len).sum::<usize>() as f64 / n as f64;

            // Per-sketch encode/decode: time all N, divide by N.
            let encode = median(runs, || {
                for s in &sources {
                    black_box(s.snapshot().unwrap());
                }
            });
            let decode = median(runs, || {
                for p in &payloads {
                    black_box(decode_snapshot(sketch_type, p).unwrap());
                }
            });
            // The window_merge path: decode the first, merge_snapshot the rest.
            let merge_all = median(runs, || {
                let mut acc = decode_snapshot(sketch_type, &payloads[0]).unwrap();
                for p in &payloads[1..] {
                    acc.merge_snapshot(p).unwrap();
                }
                black_box(&acc);
            });
            let direct = direct_merge_for(sketch_type, runs, &payloads);

            let encode_us = encode.as_secs_f64() * 1e6 / n as f64;
            let decode_us = decode.as_secs_f64() * 1e6 / n as f64;
            let merge_ms = merge_all.as_secs_f64() * 1e3;
            let per_us = merge_ms * 1e3 / n as f64;
            let direct_ms = direct.as_secs_f64() * 1e3;
            println!("{name},{n},{bytes:.0},{encode_us:.2},{decode_us:.2},{merge_ms:.3},{per_us:.2},{direct_ms:.3}");
            eprintln!(
                "{name:<20} {n:>7} {bytes:>10.0} {encode_us:>10.2} {decode_us:>10.2} {merge_ms:>13.3} {per_us:>13.2} {direct_ms:>13.3}"
            );
        }
    }
    Ok(())
}
