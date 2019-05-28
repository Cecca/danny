//! Module to manage hashes of which we can access the prefix

pub trait PrefixHash<'a> {
    type PrefixType;
    fn prefix(&'a self, n: usize) -> Self::PrefixType;
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
}

impl<'a> PrefixHash<'a> for Vec<u32> {
    type PrefixType = &'a [u32];

    fn prefix(&'a self, n: usize) -> Self::PrefixType {
        assert!(n <= self.len());
        &self[0..n]
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
