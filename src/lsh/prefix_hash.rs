//! Module to manage hashes of which we can access the prefix

use std::cmp::Ordering;

pub trait PrefixHash<'a> {
    type PrefixType: Eq;
    fn prefix(&'a self, n: usize) -> Self::PrefixType;
    fn lex_cmp(&self, other: &Self) -> Ordering;
    fn prefix_eq(&'a self, other: &'a Self, n: usize) -> bool {
        self.prefix(n) == other.prefix(n)
    }
}

impl<'a> PrefixHash<'a> for u32 {
    type PrefixType = u32;

    fn prefix(&'a self, n: usize) -> Self::PrefixType {
        assert!(n <= 32);
        let mut mask: u32 = 0;
        for _ in 0..n {
            mask = (mask << 1) | 1;
        }
        self & mask
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
}

impl<'a> PrefixHash<'a> for Vec<u32> {
    type PrefixType = &'a [u32];

    fn prefix(&'a self, n: usize) -> Self::PrefixType {
        assert!(n <= self.len());
        &self[0..n]
    }

    fn lex_cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
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
        let hash = vec![19, 32, 124, 41];
        assert_eq!(hash.prefix(3), &[19, 32, 124]);
        let expected: &[u32] = &[];
        assert_eq!(hash.prefix(0), expected);
        assert_eq!(hash.prefix(4), &[19, 32, 124, 41]);
    }
}
