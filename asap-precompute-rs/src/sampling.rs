//! Producer-side geometric skip-sampling (NitroSketch-style), mirroring
//! sketchlib-go's `common.GeometricSampler` and the Go precompute wrappers'
//! `WithSampleP`.
//!
//! Each update is admitted with probability `p`; the sketch wrapper stamps the
//! configured `p` onto the `SketchEnvelope.sample_p` field so the query side
//! rescales count-like estimates by `1/p`
//! (`asap_sketchlib::message_pack_format::portable::sampling`). Quantile
//! sketches (DDSketch/KLL) are scale-invariant under uniform sampling, so they
//! sample to shed work/bandwidth but need no query-time rescale.
//!
//! Sampling is per-producer and need not match the Go RNG sequence
//! byte-for-byte (different producers sample independently); it only needs to
//! admit an unbiased ~`p` fraction and stamp `p`. We use splitmix64 for cheap,
//! reproducible draws.

/// NitroSketch-style geometric skip-sampler. `p >= 1` (or NaN/≤0) means exact
/// (admit everything, no RNG cost).
#[derive(Debug, Clone)]
pub struct GeometricSampler {
    p: f64,
    exact: bool,
    skip: i64,
    state: u64,
}

impl GeometricSampler {
    /// Construct a sampler at probability `p` seeded from `seed`.
    pub fn new(p: f64, seed: u64) -> Self {
        let mut s = Self {
            p: 1.0,
            exact: true,
            skip: 0,
            state: 0,
        };
        s.reset(p, seed);
        s
    }

    /// Reconfigure with a new probability + reseed.
    pub fn reset(&mut self, p: f64, seed: u64) {
        self.state = seed ^ 0x9E37_79B9_7F4A_7C15;
        if !(p > 0.0) || p >= 1.0 || p.is_nan() {
            self.p = 1.0;
            self.exact = true;
            self.skip = 0;
        } else {
            self.p = p;
            self.exact = false;
            self.skip = self.next_gap();
        }
    }

    /// The effective sampling probability (1.0 when exact / disabled).
    pub fn p(&self) -> f64 {
        self.p
    }

    /// True when no sampling is applied (admit everything).
    pub fn is_exact(&self) -> bool {
        self.exact
    }

    /// Decide whether the current update is admitted. Pre-draws the gap to the
    /// next admitted update, so the amortized cost is O(1) RNG per admitted
    /// item rather than per observed item.
    pub fn admit(&mut self) -> bool {
        if self.exact {
            return true;
        }
        if self.skip > 0 {
            self.skip -= 1;
            return false;
        }
        self.skip = self.next_gap();
        true
    }

    fn next_u64(&mut self) -> u64 {
        // splitmix64
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Number of items to skip before the next admitted one: a draw from
    /// Geometric(p) via inverse-CDF, `floor(ln(u) / ln(1-p))`.
    fn next_gap(&mut self) -> i64 {
        let mut u = (self.next_u64() >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0); // (0,1)
        if u <= 0.0 {
            u = f64::MIN_POSITIVE;
        }
        ((1.0 - u).ln() / (1.0 - self.p).ln()).floor() as i64
    }
}

/// Normalize a configured `p` into the value to stamp on the wire: an exact /
/// unsampled sketch encodes `0.0` (proto3 default, dual-read as `1.0` by the
/// backend) to preserve byte-parity with the unsampled path; an actively
/// sampled sketch encodes its `p`.
pub fn wire_sample_p(p: f64) -> f64 {
    if !(p > 0.0) || p >= 1.0 || p.is_nan() {
        0.0
    } else {
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_admits_everything() {
        let mut s = GeometricSampler::new(1.0, 42);
        assert!(s.is_exact());
        for _ in 0..1000 {
            assert!(s.admit());
        }
    }

    #[test]
    fn admits_approximately_p_fraction() {
        for &p in &[0.5, 0.1, 0.01] {
            let mut s = GeometricSampler::new(p, 7);
            let n = 1_000_000;
            let mut a = 0;
            for _ in 0..n {
                if s.admit() {
                    a += 1;
                }
            }
            let frac = a as f64 / n as f64;
            assert!(
                (frac - p).abs() < 0.1 * p + 0.002,
                "p={p}: admitted fraction {frac} too far from p"
            );
        }
    }

    #[test]
    fn wire_value_preserves_unsampled_byte_parity() {
        assert_eq!(wire_sample_p(1.0), 0.0);
        assert_eq!(wire_sample_p(0.0), 0.0);
        assert_eq!(wire_sample_p(f64::NAN), 0.0);
        assert_eq!(wire_sample_p(0.25), 0.25);
    }
}
