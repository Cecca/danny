//! Module to manage hashes of which we can access the prefix

use std::cmp::Ordering;

pub trait PrefixHash {
    type PrefixType: Eq + Ord + Clone + std::fmt::Binary;
    fn max_hash_length() -> usize;
    fn prefix(&self, n: usize) -> Self::PrefixType;
    fn lex_cmp(&self, other: &Self) -> Ordering;
    fn lex_cmp_partial(&self, other: &Self, len: usize) -> Ordering;
    fn prefix_eq(&self, other: &Self, n: usize) -> bool {
        self.prefix(n) == other.prefix(n)
    }
    fn common_prefix_at_least(&self, other: &Self, l: u8) -> bool;
}

fn build_mask(l: u8) -> u32 {
    let mut mask = 0;
    for _ in 0..l {
        mask = (mask << 1) | 1;
    }
    mask
}

lazy_static! {
    static ref MASKS_32: Vec<u32> = {
        let mut masks = Vec::with_capacity(32);
        for l in 0..32 {
            masks.push(build_mask(l));
        }
        masks
    };
}

impl PrefixHash for u32 {
    type PrefixType = u32;

    fn common_prefix_at_least(&self, other: &Self, l: u8) -> bool {
        (self ^ other) & MASKS_32[l as usize] == 0
    }

    #[inline]
    fn prefix_eq(&self, other: &Self, n: usize) -> bool {
        (self & MASKS_32[n]) == (other & MASKS_32[n])
    }

    fn prefix(&self, n: usize) -> Self::PrefixType {
        assert!(n < 32);
        self & MASKS_32[n]
    }

    fn max_hash_length() -> usize {
        32
    }

    fn lex_cmp(&self, other: &Self) -> Ordering {
        let mut a: u32 = *self;
        let mut b: u32 = *other;
        if a == b {
            return Ordering::Equal;
        }
        for _ in 0..32 {
            let a_bit = a & 1;
            let b_bit = b & 1;
            if a_bit < b_bit {
                return Ordering::Less;
            } else if a_bit > b_bit {
                return Ordering::Greater;
            }
            a >>= 1;
            b >>= 1;
        }
        // We should never get here, we have an early return
        // before the loop for the equality case
        Ordering::Equal
    }

    fn lex_cmp_partial(&self, other: &Self, len: usize) -> Ordering {
        let mut a: u32 = *self;
        let mut b: u32 = *other;
        if a == b {
            return Ordering::Equal;
        }
        for _ in 0..len {
            let a_bit = a & 1;
            let b_bit = b & 1;
            if a_bit < b_bit {
                return Ordering::Less;
            } else if a_bit > b_bit {
                return Ordering::Greater;
            }
            a >>= 1;
            b >>= 1;
        }
        // We should never get here, we have an early return
        // before the loop for the equality case
        Ordering::Equal
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_prefix() {
        let hash = 0b010101100u32;
        assert_eq!(hash.prefix(3), 0b100);
        assert_eq!(hash.prefix(7), 0b0101100);
    }
}
