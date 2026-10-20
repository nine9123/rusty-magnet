//! Lock-free Bloom filter of known info hashes, so the crawler never has to
//! touch the database to decide whether a discovered hash is new.

use std::sync::atomic::{AtomicU64, Ordering};

pub struct Bloom {
    bits: Vec<AtomicU64>,
    nbits: u64,
    k: u32,
}

impl Bloom {
    /// Sized for `capacity` items at the given false-positive rate.
    pub fn new(capacity: usize, fp_rate: f64) -> Self {
        let n = capacity.max(1000) as f64;
        let ln2 = std::f64::consts::LN_2;
        let m = (-(n * fp_rate.ln()) / (ln2 * ln2)).ceil() as u64;
        let words = m.div_ceil(64).max(1);
        let k = ((m as f64 / n) * ln2).round().clamp(1.0, 16.0) as u32;
        Bloom { bits: (0..words).map(|_| AtomicU64::new(0)).collect(), nbits: words * 64, k }
    }

    pub fn memory_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    fn indexes(&self, key: &[u8; 20]) -> impl Iterator<Item = u64> + '_ {
        // Info hashes are already uniformly distributed: use them directly
        // (Kirsch–Mitzenmacher double hashing).
        let h1 = u64::from_le_bytes(key[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(key[8..16].try_into().unwrap()) | 1;
        (0..self.k as u64).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % self.nbits)
    }

    pub fn insert(&self, key: &[u8; 20]) {
        for i in self.indexes(key) {
            self.bits[(i / 64) as usize].fetch_or(1 << (i % 64), Ordering::Relaxed);
        }
    }

    pub fn contains(&self, key: &[u8; 20]) -> bool {
        self.indexes(key).all(|i| self.bits[(i / 64) as usize].load(Ordering::Relaxed) & (1 << (i % 64)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let b = Bloom::new(10_000, 0.001);
        let key = |i: u32| {
            let mut k = [0u8; 20];
            let h = sha1_smol::Sha1::from(i.to_le_bytes()).digest().bytes();
            k.copy_from_slice(&h);
            k
        };
        for i in 0..10_000 {
            b.insert(&key(i));
        }
        assert!((0..10_000).all(|i| b.contains(&key(i))));
        let fp = (10_000..110_000).filter(|i| b.contains(&key(*i))).count();
        assert!(fp < 300, "false positives: {fp}");
    }
}
