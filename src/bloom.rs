use probabilistic_collections::hyperloglog::HyperLogLog;
use rand::Rng;
use siphasher::sip::SipHasher;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::mem::size_of;

pub struct BloomFilter<T> {
    num_bits: usize,
    expected_elements: usize,
    k: usize,
    bits: Vec<usize>,
    hashers: Vec<SipHasher>,
    estimated_elements: HyperLogLog<T>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Hash> Debug for BloomFilter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "BloomFilter(bits per element: {}, fpp: {}, k: {}, expected elements: {}, num bits: {})",
            self.num_bits as f64 / self.expected_elements as f64,
            self.fpp(),
            self.k,
            self.expected_elements,
            self.num_bits,
        )
    }
}

impl<T: Hash> BloomFilter<T> {
    pub fn new<R: Rng>(elements: usize, fpp: f64, rng: &mut R) -> Self {
        let word_length = size_of::<usize>() * 8;
        let num_bits = (-(elements as f64) * fpp.log2() / 2f64.ln()).ceil() as usize;
        let k = (2f64.ln() * (num_bits as f64 / elements as f64)).ceil() as usize;
        let num_words = (num_bits as f64 / word_length as f64).ceil() as usize;
        let bits = vec![0usize; num_words];
        let mut hashers = Vec::with_capacity(k);
        for _ in 0..k {
            let h = SipHasher::new_with_keys(rng.next_u64(), rng.next_u64());
            hashers.push(h);
        }
        let estimated_elements = HyperLogLog::new(0.1);

        BloomFilter {
            num_bits,
            expected_elements: elements,
            k,
            bits,
            hashers,
            estimated_elements,
            _marker: std::marker::PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn show_bits(&self) -> String {
        let mut s = String::new();
        for b in self.bits.iter() {
            s.push_str(&format!("{:64b}", b));
        }
        s
    }

    pub fn contains(&self, x: &T) -> bool {
        let word_length = size_of::<usize>() * 8;
        for hasher in self.hashers.iter() {
            let mut hasher = *hasher;
            x.hash(&mut hasher);
            let h = hasher.finish() as usize;
            let bit = h % self.num_bits;
            let word_idx = bit / word_length;
            let mask = 1 << (bit % word_length);
            if self.bits[word_idx] & mask == 0 {
                return false;
            }
        }
        true
    }

    pub fn insert(&mut self, x: &T) {
        let word_length = size_of::<usize>() * 8;
        for hasher in self.hashers.iter() {
            let mut hasher = *hasher;
            x.hash(&mut hasher);
            let h = hasher.finish() as usize;
            let bit = h % self.num_bits;
            let word_idx = bit / word_length;
            let mask = 1 << (bit % word_length);
            self.bits[word_idx] |= mask;
        }
        self.estimated_elements.insert(x);
    }

    pub fn assert_size(&self) {
        assert!(
            (self.estimated_elements.len().ceil() as usize) < self.expected_elements,
            "Estimated elements {} > {} expected",
            self.estimated_elements.len(),
            self.expected_elements
        );
    }

    fn fpp(&self) -> f64 {
        0.5_f64.powi(self.k as i32)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::RngCore;
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;

    fn test_sequential(elements: usize, p: f64) {
        let mut rng = XorShiftRng::seed_from_u64(12423);
        let mut bloom = BloomFilter::new(elements, p, &mut rng);
        println!("{:?}", bloom);
        let mut elems = Vec::with_capacity(elements);
        for _ in 0..elements {
            elems.push(rng.next_u64());
        }

        let mut false_positives = 0;
        for x in elems.iter() {
            if bloom.contains(x) {
                false_positives += 1;
            }
            bloom.insert(x);
        }

        let fpp = bloom.fpp();
        let actual_fpp = false_positives as f64 / elements as f64;
        println!(
            "k={}, predicted fpp is {}, actual {}",
            bloom.k, fpp, actual_fpp
        );
        assert!(actual_fpp <= fpp);
        for x in elems.iter() {
            let already_in = bloom.contains(x);
            assert!(already_in, "The element should be already in");
        }
    }

    #[test]
    fn test_sequential_driver() {
        test_sequential(1 << 7, 0.03);
        test_sequential(1 << 9, 0.03);
        test_sequential(1 << 11, 0.03);
        test_sequential(1 << 11, 0.01);
        test_sequential(1 << 11, 0.1);
    }
}
