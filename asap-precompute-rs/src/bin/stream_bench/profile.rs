//! Opt-in, exclusive thread CPU accounting. Guards must never span an await.
use std::{
    cell::Cell,
    marker::PhantomData,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
};

#[derive(Clone, Copy)]
pub enum Category {
    Computation,
    PdataCodec,
    SketchCodec,
    ProcessorBookkeeping,
}
const NAMES: [&str; 4] = [
    "computation",
    "pdata_codec",
    "sketch_codec",
    "processor_bookkeeping",
];
static TOTALS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static ENABLED: OnceLock<bool> = OnceLock::new();
thread_local! { static ACTIVE: Cell<Option<(usize, u64)>> = const { Cell::new(None) }; }

fn switch(next: Option<usize>) -> Option<usize> {
    let now = super::clock_ns(libc::CLOCK_THREAD_CPUTIME_ID);
    ACTIVE.with(|active| {
        let previous = active.replace(next.map(|category| (category, now)));
        if let Some((category, started)) = previous {
            TOTALS[category].fetch_add(now - started, Ordering::Relaxed);
        }
        previous.map(|(category, _)| category)
    })
}

// !Send and !Sync: this measures one synchronous thread only.
pub struct Scope {
    previous: Option<usize>,
    enabled: bool,
    _thread: PhantomData<Rc<()>>,
}
pub fn scope(category: Category) -> Scope {
    let enabled =
        *ENABLED.get_or_init(|| std::env::var("ASAP_PROFILE_CPU").is_ok_and(|v| v == "1"));
    Scope {
        previous: if enabled {
            switch(Some(category as usize))
        } else {
            None
        },
        enabled,
        _thread: PhantomData,
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if self.enabled {
            switch(self.previous);
        }
    }
}
pub fn snapshot() -> std::collections::BTreeMap<&'static str, u64> {
    NAMES
        .into_iter()
        .zip(TOTALS.iter().map(|v| v.load(Ordering::Relaxed)))
        .collect()
}
