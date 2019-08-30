use crate::config::Config;
use rand::Rng;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

pub trait ToBits
where
    Self: Sized,
{
    fn bytes_to_bits(self) -> usize;
    fn kb_to_bits(self) -> usize {
        self.bytes_to_bits() * 1024
    }
    fn mb_to_bits(self) -> usize {
        self.kb_to_bits() * 1024
    }
    fn gb_to_bits(self) -> usize {
        self.mb_to_bits() * 1024
    }
}

impl ToBits for usize {
    fn bytes_to_bits(self) -> usize {
        self * 8
    }
}

struct MultiplyShiftPairHash {
    alpha1: u64,
    alpha2: u64,
    beta1: u64,
    beta2: u64,
}

impl MultiplyShiftPairHash {
    pub fn new<R: Rng>(rng: &mut R) -> Self {
        Self {
            alpha1: rng.next_u64(),
            alpha2: rng.next_u64(),
            beta1: rng.next_u64(),
            beta2: rng.next_u64(),
        }
    }

    pub fn hash(&self, x: u64, y: u64) -> u64 {
        let first = self.alpha1.wrapping_mul(x).wrapping_add(self.beta1);
        let second = self.alpha2.wrapping_mul(y).wrapping_add(self.beta2);
        first.wrapping_add(second) >> 32
    }
}

pub struct AtomicBloomFilter<K>
where
    K: Into<u64> + Copy,
{
    num_bits: usize,
    k: usize,
    bits: Vec<AtomicU64>,
    word_selector: MultiplyShiftPairHash,
    hashers: Vec<MultiplyShiftPairHash>,
    _phantom: std::marker::PhantomData<K>,
}

impl<K: Into<u64> + Copy> AtomicBloomFilter<K> {
    pub fn from_config<R: Rng>(config: &Config, rng: R) -> Self {
        let num_bits = config.get_bloom_bits();
        let k = config.get_bloom_k();
        Self::new(num_bits, k, rng)
    }

    pub fn new<R: Rng>(num_bits: usize, k: usize, mut rng: R) -> Self {
        assert!(k < 64 / 2);
        let num_words = (num_bits as f64 / 64_f64).ceil() as usize;
        let mut bits = Vec::with_capacity(num_words);
        let mut hashers = Vec::with_capacity(k);
        for _ in 0..num_words {
            bits.push(AtomicU64::new(0));
        }
        for _ in 0..k {
            let h = MultiplyShiftPairHash::new(&mut rng);
            hashers.push(h);
        }
        let word_selector = MultiplyShiftPairHash::new(&mut rng);

        AtomicBloomFilter {
            num_bits,
            k,
            bits,
            word_selector,
            hashers,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn test_and_insert(&self, x: &(K, K)) -> bool {
        let x1: u64 = x.0.into();
        let x2: u64 = x.1.into();
        let word_idx = self.word_selector.hash(x1, x2) as usize % self.bits.len();
        let mut word = 0u64;
        for h in self.hashers.iter() {
            let bit_idx = h.hash(x1, x2) % 64;
            word |= 1 << bit_idx;
        }
        // Atomically insert the bits in the relevant word
        let previous = self.bits[word_idx].fetch_or(word, Ordering::Relaxed);
        // Check if *all* the bits were set before this insertion
        previous & word == word
    }
}

impl<T: Into<u64> + Copy> Debug for AtomicBloomFilter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "AtomicBloomFilter(k: {}, num bits: {})",
            self.k, self.num_bits,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use probabilistic_collections::hyperloglog::HyperLogLog;
    use rand::RngCore;
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;
    use std::hash::Hash;
    use std::hash::Hasher;
    use std::hash::SipHasher;
    use std::mem::size_of;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Barrier};
    use std::thread;

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
            let num_bits = (-(elements as f64) * fpp.log2() / 2f64.ln()).ceil() as usize;
            let k = (2f64.ln() * (num_bits as f64 / elements as f64)).ceil() as usize;
            Self::from_params(elements, num_bits, k, rng)
        }

        pub fn from_params<R: Rng>(
            elements: usize,
            num_bits: usize,
            k: usize,
            rng: &mut R,
        ) -> Self {
            let word_length = size_of::<usize>() * 8;
            let num_words = (num_bits as f64 / word_length as f64).ceil() as usize;
            let bits = vec![0usize; num_words];
            let mut hashers = Vec::with_capacity(k);
            for _ in 0..k {
                let h = SipHasher::new_with_keys(rng.next_u64(), rng.next_u64());
                hashers.push(h);
            }
            let estimated_elements = HyperLogLog::new(0.01);

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
                let mut hasher = hasher.clone();
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
            let word_length = std::mem::size_of::<usize>() * 8;
            for hasher in self.hashers.iter() {
                let mut hasher = hasher.clone();
                x.hash(&mut hasher);
                let h = hasher.finish() as usize;
                let bit = h % self.num_bits;
                let word_idx = bit / word_length;
                let mask = 1 << (bit % word_length);
                self.bits[word_idx] |= mask;
            }
            self.estimated_elements.insert(x);
        }

        fn fpp(&self) -> f64 {
            0.5_f64.powi(self.k as i32)
        }
    }

    fn test_atomic_threads(elements: usize, bits: usize, k: usize) {
        let n_threads = 4;
        let mut rng = XorShiftRng::seed_from_u64(12423);
        let bloom = Arc::new(AtomicBloomFilter::new(bits, k, &mut rng));
        println!("{:?}", bloom);

        let barrier = Arc::new(Barrier::new(n_threads));
        let false_positives = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        let mut elems = Vec::new();

        for _ in 0..n_threads {
            let bloom = bloom.clone();
            let barrier = barrier.clone();
            let false_positives = Arc::clone(&false_positives);
            let mut part_elems = Vec::new();
            for _ in 0..(elements / n_threads) {
                let x = rng.next_u64();
                elems.push(x);
                part_elems.push(x)
            }

            let handle = thread::spawn(move || {
                barrier.wait();
                for x in part_elems.iter() {
                    let x = (*x, *x);
                    let already_in = bloom.test_and_insert(&x);
                    if already_in {
                        false_positives.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles.drain(..) {
            handle.join().unwrap();
        }
        let mut check = BloomFilter::new(elements, 0.03, &mut rng);
        let mut check_false_positives = 0;
        for x in elems.iter() {
            let x = (*x, *x);
            if check.contains(&x) {
                check_false_positives += 1;
            }
            check.insert(&x);
        }
        assert!(
            false_positives.load(Ordering::Relaxed)
                <= 1 + (1.5 * check_false_positives as f64).ceil() as usize
        );

        for x in elems.iter() {
            let x = (*x, *x);
            let already_in = bloom.test_and_insert(&x);
            assert!(already_in, "The element should be already in");
        }
    }

    fn test_atomic(elements: usize, bits: usize, k: usize) {
        let mut rng = XorShiftRng::seed_from_u64(12423);
        let bloom = AtomicBloomFilter::new(bits, k, &mut rng);
        let mut check = BloomFilter::new(elements, 0.03, &mut rng);
        println!("{:?}", bloom);
        println!("{:?}", check);
        let mut elems = Vec::with_capacity(elements);
        for _ in 0..elements {
            elems.push(rng.next_u64());
        }

        let mut false_positives = 0;
        for x in elems.iter() {
            let x = (*x, *x);
            let already_in = bloom.test_and_insert(&x);
            if already_in {
                println!("False positive! {:?}", x);
                false_positives += 1;
            }
        }
        let mut check_false_positives = 0;
        for x in elems.iter() {
            let x = (*x, *x);
            if check.contains(&x) {
                check_false_positives += 1;
            }
            check.insert(&x);
        }

        println!(
            "k={}, baseline fp {}, actual fp {}",
            bloom.k, check_false_positives, false_positives
        );
        assert!(false_positives <= 1 + (1.5 * check_false_positives as f64).ceil() as usize);
        for x in elems.iter() {
            let x = (*x, *x);
            let already_in = bloom.test_and_insert(&x);
            assert!(already_in, "The element should be already in");
        }
    }

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
            let x = (*x, *x);
            if bloom.contains(&x) {
                false_positives += 1;
            }
            bloom.insert(&x);
        }

        let fpp = bloom.fpp();
        let actual_fpp = false_positives as f64 / elements as f64;
        println!(
            "k={}, predicted fpp is {}, actual {}",
            bloom.k, fpp, actual_fpp
        );
        assert!(actual_fpp <= fpp);
        for x in elems.iter() {
            let x = (*x, *x);
            let already_in = bloom.contains(&x);
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
    #[test]
    fn test_atomic_driver() {
        let ns = &[
            1 << 7,
            1 << 8,
            1 << 9,
            1 << 10,
            1 << 11,
            1 << 12,
            1 << 13,
            1 << 14,
            1 << 15,
            1 << 20,
        ];
        for n in ns.iter() {
            let bits = 10 * n;
            let k = 6;
            test_atomic(*n, bits, k);
        }
    }

    #[test]
    fn test_atomic_threads_driver() {
        let ns = &[
            1 << 7,
            1 << 8,
            1 << 9,
            1 << 10,
            1 << 11,
            1 << 12,
            1 << 13,
            1 << 14,
            1 << 15,
            1 << 20,
        ];
        for n in ns.iter() {
            let bits = 10 * n;
            let k = 6;
            test_atomic_threads(*n, bits, k);
        }
    }

    #[test]
    fn test_to_bits() {
        assert_eq!(1.bytes_to_bits(), 8);
        assert_eq!(1.mb_to_bits(), 1024 * 1024 * 8);
        assert_eq!(2.gb_to_bits(), 1024 * 2.mb_to_bits());
    }
}
