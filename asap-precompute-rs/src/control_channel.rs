//! [`ControlChannel`] trait.
//!
//! The controller delivers
//! [`crate::config::PrecomputeConfigSet`] plans to runtime adapters
//! through implementations of this trait. Concrete implementations
//! (OpAMP, HTTP-poll, file-watch) land alongside the per-language
//! adapter glue.

use crate::config::PrecomputeConfigSet;

/// Delivers [`PrecomputeConfigSet`]s from the controller to
/// adapters at runtime.
pub trait ControlChannel: Send + Sync {
    /// Returns a non-`None` [`PrecomputeConfigSet`] if the plan has
    /// changed since the last poll. `None` means "no change."
    ///
    /// Implementations may block briefly on network I/O; callers
    /// typically call from a dedicated task / thread.
    fn poll(&self) -> Option<PrecomputeConfigSet>;

    /// Confirms acceptance of a plan version.
    ///
    /// OpAMP uses this to report effective config back to the
    /// supervisor; HTTP-poll implementations may ignore.
    fn ack(&self, plan_version: u64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// No-op stub used to prove the trait can be implemented — a
    /// minimal fake before the real OpAMP / HTTP-poll impls land.
    struct StubChannel {
        last_ack: AtomicU64,
    }

    impl ControlChannel for StubChannel {
        fn poll(&self) -> Option<PrecomputeConfigSet> {
            None
        }

        fn ack(&self, plan_version: u64) {
            self.last_ack.store(plan_version, Ordering::Release);
        }
    }

    #[test]
    fn stub_channel_implements_trait() {
        let c = StubChannel {
            last_ack: AtomicU64::new(0),
        };
        // poll returns None for the no-change case.
        assert!(c.poll().is_none());
        // ack records the version.
        c.ack(7);
        assert_eq!(c.last_ack.load(Ordering::Acquire), 7);
    }
}
