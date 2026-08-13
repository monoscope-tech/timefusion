//! HyperLogLog: a mergeable, bounded distinct-count sketch.
//!
//! The point is the same one t-digest makes for percentiles: `COUNT(DISTINCT x)`
//! is not decomposable, so a rollup cannot store it and every dashboard tile that
//! asks for one falls back to a raw scan of the whole window. A sketch IS
//! decomposable — union is associative and commutative — so a distinct count can
//! be pre-aggregated per bucket and folded across buckets, dimensions and rollup
//! tiers at read time.
//!
//! Two representations behind one type:
//!
//! * **Sparse** — the exact set of hashes, up to [`SPARSE_MAX`]. The estimate is
//!   then the set size, i.e. EXACT. Most dashboard groups (distinct users in a
//!   minute, distinct services on a span) never leave this mode, so the common
//!   case is not an approximation at all.
//! * **Dense** — [`M`] one-byte registers once the exact set outgrows the dense
//!   encoding. Standard error is `1.04/sqrt(M)` ≈ 1.6%.
//!
//! Serialized sketches are PERSISTED in rollup tables and merged months later,
//! so both the hash function and the wire format are frozen. [`hash_bytes`] is
//! written out here rather than taken from `ahash`/`RandomState` precisely so a
//! dependency bump cannot silently re-hash the world and inflate every stored
//! cardinality. Changing `SEED`, `P`, or the tag bytes requires a new rollup
//! spec name, exactly like changing a measure.

use std::collections::HashSet;

/// Register-index bits. 12 → 4096 registers, 4 KiB dense, ~1.6% standard error.
const P: u32 = 12;
/// Register count.
const M: usize = 1 << P;
/// Above this many exact hashes the sparse encoding costs more than the dense
/// one (`M` bytes), so it converts. 512 × 8 B = 4 KiB, matching `M`.
const SPARSE_MAX: usize = 512;

const TAG_SPARSE: u8 = 1;
const TAG_DENSE: u8 = 2;

const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// splitmix64's finalizer: two multiply-shift-xor rounds, full avalanche.
#[inline]
const fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Stable 64-bit hash. Frozen: see the module note on persistence.
///
/// Eight bytes per round (~0.6 cycles/byte) rather than FNV's one, which matters
/// because this runs once per row over columns like `context___trace_id`.
#[inline]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut acc = SEED ^ (bytes.len() as u64);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        acc = mix(acc ^ u64::from_le_bytes(chunk.try_into().expect("chunks_exact(8) yields 8 bytes")));
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut tail = [0u8; 8];
        tail[..remainder.len()].copy_from_slice(remainder);
        acc = mix(acc ^ u64::from_le_bytes(tail));
    }
    mix(acc)
}

/// A distinct-count sketch. `Default` is the empty sketch, estimating 0.
#[derive(Debug, Clone, PartialEq)]
pub enum Hll {
    Sparse(HashSet<u64>),
    Dense(Box<[u8; M]>),
}

impl Default for Hll {
    fn default() -> Self {
        Self::Sparse(HashSet::new())
    }
}

/// Split a hash into its register index and the 1-based position of the first
/// set bit in the remaining suffix.
#[inline]
const fn register_of(hash: u64) -> (usize, u8) {
    let index = (hash >> (64 - P)) as usize;
    // `| 1` bounds rho at 64-P+1 without a branch: the sentinel bit stops the
    // count when the whole suffix is zero.
    let rho = ((hash << P) | 1).leading_zeros() as u8 + 1;
    (index, rho)
}

impl Hll {
    /// Add one pre-hashed value.
    pub fn insert_hash(&mut self, hash: u64) {
        match self {
            Self::Sparse(hashes) => {
                hashes.insert(hash);
                if hashes.len() > SPARSE_MAX {
                    self.densify();
                }
            }
            Self::Dense(registers) => {
                let (index, rho) = register_of(hash);
                registers[index] = registers[index].max(rho);
            }
        }
    }

    fn densify(&mut self) {
        let Self::Sparse(hashes) = self else { return };
        let mut registers = Box::new([0u8; M]);
        for &hash in hashes.iter() {
            let (index, rho) = register_of(hash);
            registers[index] = registers[index].max(rho);
        }
        *self = Self::Dense(registers);
    }

    /// Union. Associative and commutative, which is the whole reason this type
    /// can live in a rollup.
    pub fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::Sparse(mine), Self::Sparse(theirs)) => {
                mine.extend(theirs.iter().copied());
                if mine.len() > SPARSE_MAX {
                    self.densify();
                }
            }
            (Self::Sparse(_), Self::Dense(_)) => {
                let mine = std::mem::replace(self, other.clone());
                self.merge(&mine);
            }
            (Self::Dense(mine), Self::Sparse(theirs)) => {
                for &hash in theirs.iter() {
                    let (index, rho) = register_of(hash);
                    mine[index] = mine[index].max(rho);
                }
            }
            (Self::Dense(mine), Self::Dense(theirs)) => {
                for (slot, &their) in mine.iter_mut().zip(theirs.iter()) {
                    *slot = (*slot).max(their);
                }
            }
        }
    }

    /// Estimated distinct count — exact while sparse.
    pub fn estimate(&self) -> u64 {
        let registers = match self {
            Self::Sparse(hashes) => return hashes.len() as u64,
            Self::Dense(registers) => registers,
        };
        let zeros = registers.iter().filter(|&&r| r == 0).count();
        // Linear counting is the better estimator while most registers are still
        // empty; it is also what keeps the seam at the sparse→dense boundary from
        // jumping.
        if zeros > 0 {
            let linear = M as f64 * (M as f64 / zeros as f64).ln();
            if linear <= 2.5 * M as f64 {
                return linear.round() as u64;
            }
        }
        // Flajolet's harmonic-mean estimator. `alpha` is the standard bias
        // constant for m >= 128.
        let alpha = 0.7213 / (1.0 + 1.079 / M as f64);
        let harmonic: f64 = registers.iter().map(|&r| f64::from(2u32).powi(-i32::from(r))).sum();
        (alpha * (M * M) as f64 / harmonic).round() as u64
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Sparse(hashes) => {
                let mut out = Vec::with_capacity(1 + hashes.len() * 8);
                out.push(TAG_SPARSE);
                out.extend(hashes.iter().flat_map(|hash| hash.to_le_bytes()));
                out
            }
            Self::Dense(registers) => {
                let mut out = Vec::with_capacity(1 + M);
                out.push(TAG_DENSE);
                out.extend_from_slice(&**registers);
                out
            }
        }
    }

    /// Decode. A malformed payload is an error rather than an empty sketch: a
    /// silently-empty sketch would under-report a cardinality forever.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        match bytes {
            [] => Ok(Self::default()),
            [TAG_SPARSE, rest @ ..] if rest.len() % 8 == 0 => {
                Ok(Self::Sparse(rest.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)"))).collect()))
            }
            [TAG_DENSE, rest @ ..] if rest.len() == M => {
                Ok(Self::Dense(Box::new(rest.try_into().map_err(|_| "hll: dense payload is not M bytes".to_string())?)))
            }
            [tag, ..] => Err(format!("hll: malformed sketch (tag {tag}, {} bytes)", bytes.len())),
        }
    }

    /// Heap footprint, for the accumulator's memory accounting.
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::Sparse(hashes) => hashes.capacity() * size_of::<u64>(),
                Self::Dense(_) => M,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch_of(range: std::ops::Range<u64>) -> Hll {
        let mut hll = Hll::default();
        for value in range {
            hll.insert_hash(hash_bytes(&value.to_le_bytes()));
        }
        hll
    }

    /// The sparse mode is not an approximation, which is what makes this safe to
    /// put behind the low-cardinality dashboard tiles (distinct services, distinct
    /// hosts) where a 2% error would be visible as a wrong integer.
    #[test]
    fn small_cardinalities_are_exact() {
        for n in [0u64, 1, 7, 100, SPARSE_MAX as u64] {
            assert_eq!(sketch_of(0..n).estimate(), n, "sparse must be exact at {n}");
        }
    }

    #[test]
    fn large_cardinalities_land_within_the_error_bound() {
        // 1.04/sqrt(4096) = 1.6% standard error; allow 3 sigma so the test is not
        // flaky against a hash-dependent but deterministic outcome.
        for n in [5_000u64, 50_000, 1_000_000] {
            let estimate = sketch_of(0..n) .estimate() as f64;
            let error = (estimate - n as f64).abs() / n as f64;
            assert!(error < 0.05, "n={n}: estimated {estimate}, error {:.3}%", error * 100.0);
        }
    }

    /// Union must be order-independent and must not double-count the overlap —
    /// the property the whole rollup design rests on.
    #[test]
    fn merge_is_a_union_not_a_sum() {
        let (mut left, right) = (sketch_of(0..30_000), sketch_of(20_000..50_000));
        left.merge(&right);
        let error = (left.estimate() as f64 - 50_000.0).abs() / 50_000.0;
        assert!(error < 0.05, "overlapping union estimated {}, want ~50000", left.estimate());
    }

    #[test]
    fn merge_crosses_the_sparse_dense_boundary_in_both_directions() {
        let (small, large) = (sketch_of(0..10), sketch_of(0..20_000));
        for (mut a, b) in [(small.clone(), large.clone()), (large.clone(), small.clone())] {
            a.merge(&b);
            let error = (a.estimate() as f64 - 20_000.0).abs() / 20_000.0;
            assert!(error < 0.05, "estimated {} from a mixed-mode merge", a.estimate());
        }
    }

    /// Sketches are persisted and merged months later, so a round-trip through
    /// bytes must be bit-identical, and a truncated payload must be loud.
    #[test]
    fn round_trips_through_bytes() {
        for n in [0u64, 10, SPARSE_MAX as u64 + 1, 100_000] {
            let sketch = sketch_of(0..n);
            assert_eq!(Hll::from_bytes(&sketch.to_bytes()).unwrap(), sketch, "n={n}");
        }
        assert!(Hll::from_bytes(&[TAG_DENSE, 0, 0]).is_err());
        assert!(Hll::from_bytes(&[TAG_SPARSE, 0, 0, 0]).is_err());
        assert!(Hll::from_bytes(&[9, 9]).is_err());
        assert_eq!(Hll::from_bytes(&[]).unwrap(), Hll::default());
    }

    /// A dense sketch is bounded no matter how many rows it sees: that bound is
    /// what lets a rollup row carry one.
    #[test]
    fn serialized_size_is_bounded() {
        assert!(sketch_of(0..10_000_000).to_bytes().len() <= M + 1);
    }

    /// Frozen hash: a dependency bump or refactor that changes these values
    /// invalidates every stored sketch on S3.
    #[test]
    fn the_hash_is_frozen() {
        assert_eq!(hash_bytes(b""), 16294208416658607535);
        assert_eq!(hash_bytes(b"timefusion"), 10501298223482614002);
    }
}
