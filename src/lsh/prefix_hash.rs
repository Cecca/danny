//! Module to manage hashes of which we can access the prefix

use crate::operators::Route;
use std::cmp::Ordering;

pub trait PrefixHash<'a> {
    type PrefixType: Eq + Ord + Clone + Route;
    fn prefix(&'a self, n: usize) -> Self::PrefixType;
    fn lex_cmp(&self, other: &Self) -> Ordering;
    fn prefix_eq(&'a self, other: &'a Self, n: usize) -> bool {
        self.prefix(n) == other.prefix(n)
    }
    fn longest_common_prefix(&self, other: &Self) -> u8;
}

impl<'a> PrefixHash<'a> for u32 {
    type PrefixType = u32;

    fn longest_common_prefix(&self, other: &Self) -> u8 {
        let mut mask = 1u32;
        let mut len = 0u8;
        while ((self & mask) == (other & mask)) && len <= 32 {
            len += 1;
            mask = (mask << 1) | 1;
        }
        len
    }

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
        assert_eq!(hash.prefix(1), &[19]);
    }

    #[test]
    fn test_prefix_sort() {
        let mut hashes = vec![
            vec![1, 3, 7, 2110],
            vec![0, 1, 3, 5],
            vec![1, 3, 5, 10],
            vec![3, 1, 5, 8],
            vec![30, 1, 5, 8],
            vec![3, 2, 5, 8],
        ];
        let expected = vec![
            vec![0, 1, 3, 5],
            vec![1, 3, 5, 10],
            vec![1, 3, 7, 2110],
            vec![3, 1, 5, 8],
            vec![3, 2, 5, 8],
            vec![30, 1, 5, 8],
        ];
        hashes.sort_unstable_by(|a, b| a.lex_cmp(&b));
        assert_eq!(hashes, expected);
    }
}
