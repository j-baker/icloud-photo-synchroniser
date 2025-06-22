use std::{
    fmt::Display,
    sync::atomic::{AtomicU64, Ordering},
};

const ORDERING: Ordering = Ordering::SeqCst;

#[derive(Default)]
pub struct SimpleAtomicU64(AtomicU64);

impl SimpleAtomicU64 {
    pub fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn as_u64(&self) -> u64 {
        self.0.load(ORDERING)
    }

    pub fn fetch_add(&self, value: u64) -> u64 {
        self.0.fetch_add(value, ORDERING)
    }
}

impl Display for SimpleAtomicU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_u64().fmt(f)
    }
}

impl From<SimpleAtomicU64> for u64 {
    fn from(value: SimpleAtomicU64) -> Self {
        value.as_u64()
    }
}
