use std::fmt::Debug;

/// Maintains a pool of buckets, to which used and cleared
/// ones can be returned, in order to reuse the memory
pub struct BucketPool<H, K>
where
    H: Ord + Debug,
    K: Debug,
{
    pool: Vec<Bucket<H, K>>,
}

impl<H, K> BucketPool<H, K>
where
    H: Ord + Debug,
    K: Debug,
{
    pub fn get(&mut self) -> Bucket<H, K> {
        self.pool.pop().unwrap_or_default()
    }

    pub fn give_back(&mut self, b: Bucket<H, K>) {
        self.pool.push(b);
    }
}

impl<H, K> Default for BucketPool<H, K>
where
    H: Ord + Debug,
    K: Debug,
{
    fn default() -> Self {
        Self { pool: Vec::new() }
    }
}

pub struct Bucket<H, K>
where
    H: Ord + Debug,
    K: Debug,
{
    pub left: Vec<(H, K)>,
    right: Vec<(H, K)>,
}

impl<H, K> Bucket<H, K>
where
    H: Ord + Debug,
    K: Debug,
{
    pub fn new() -> Self {
        Self {
            left: Vec::new(),
            right: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }

    pub fn is_one_side_empty(&self) -> bool {
        self.left.is_empty() || self.right.is_empty()
    }

    pub fn push_left(&mut self, h: H, k: K) {
        self.left.push((h, k));
    }

    pub fn push_right(&mut self, h: H, k: K) {
        self.right.push((h, k));
    }

    pub fn len_left(&self) -> usize {
        self.left.len()
    }

    pub fn len_right(&self) -> usize {
        self.right.len()
    }

    pub fn clear(&mut self) {
        self.left.clear();
        self.right.clear();
    }

    fn sort_inner(&mut self) {
        self.left.sort_unstable_by(|p1, p2| p1.0.cmp(&p2.0));
        self.right.sort_unstable_by(|p1, p2| p1.0.cmp(&p2.0));
    }

    pub fn for_all<F>(&mut self, mut action: F)
    where
        F: FnMut(&K, &K) -> (),
    {
        self.sort_inner();
        let buckets_iter = BucketsIter::new(&self.left, &self.right);
        for (lb, rb) in buckets_iter {
            for l_tile in lb.chunks(8) {
                for r_tile in rb.chunks(8) {
                    for l in l_tile {
                        for r in r_tile {
                            debug_assert!(l.0 == r.0);
                            action(&l.1, &r.1);
                        }
                    }
                }
            }
        }
    }

    pub fn for_all_buckets<F>(&mut self, mut action: F)
    where
        F: FnMut(&[(H, K)], &[(H, K)]) -> (),
    {
        self.sort_inner();
        let buckets_iter = BucketsIter::new(&self.left, &self.right);
        for (lb, rb) in buckets_iter {
            action(lb, rb);
        }
    }
}

impl<H, K> Default for Bucket<H, K>
where
    H: Ord + Debug,
    K: Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

trait FindBucketEnd<'a, H, K> {
    fn find_bucket_end(&self, items: &'a [(H, K)], start: usize) -> (&'a H, usize);
}

struct BucketsIter<'a, H, K>
where
    H: PartialOrd,
    K: Debug,
{
    left: &'a [(H, K)],
    right: &'a [(H, K)],
    cur_left: usize,
    cur_right: usize,
}

impl<'a, H, K> BucketsIter<'a, H, K>
where
    H: PartialOrd,
    K: Debug,
{
    fn new(left: &'a [(H, K)], right: &'a [(H, K)]) -> Self {
        Self {
            left,
            right,
            cur_left: 0,
            cur_right: 0,
        }
    }
}

impl<'a, H, K> FindBucketEnd<'a, H, K> for BucketsIter<'a, H, K>
where
    H: PartialOrd,
    K: Debug,
{
    fn find_bucket_end(&self, items: &'a [(H, K)], start: usize) -> (&'a H, usize) {
        let start_hash = &items[start].0;
        let end = start
            + items[start..]
                .iter()
                .take_while(|p| &p.0 == start_hash)
                .count();
        (start_hash, end)
    }
}

impl<'a, H, K> Iterator for BucketsIter<'a, H, K>
where
    K: Debug,
    H: PartialOrd,
{
    type Item = (&'a [(H, K)], &'a [(H, K)]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur_left >= self.left.len() || self.cur_right >= self.right.len() {
                return None;
            }
            let lend = self.find_bucket_end(self.left, self.cur_left);
            let rend = self.find_bucket_end(self.right, self.cur_right);
            if lend.0 < rend.0 {
                self.cur_left = lend.1;
            } else if lend.0 > rend.0 {
                self.cur_right = rend.1;
            } else {
                // We are in a non empty bucket!
                let lstart = self.cur_left;
                let rstart = self.cur_right;
                self.cur_left = lend.1;
                self.cur_right = rend.1;
                return Some((
                    &self.left[lstart..self.cur_left],
                    &self.right[rstart..self.cur_right],
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::collections::HashSet;

    #[test]
    fn test_bucket() {
        let mut buckets = HashMap::new();
        buckets.insert(0, (vec![1, 2, 3], vec![10, 11, 12]));
        buckets.insert(1, (vec![], vec![19]));
        buckets.insert(2, (vec![1, 2, 3, 4], vec![]));
        buckets.insert(3, (vec![3, 4], vec![30]));

        let mut expected = HashSet::new();
        for (_k, (lks, rks)) in buckets.iter() {
            for lk in lks.iter() {
                for rk in rks.iter() {
                    expected.insert((lk.clone(), rk.clone()));
                }
            }
        }
        let mut bucket = Bucket::new();
        for (k, (lks, rks)) in buckets.iter() {
            for lk in lks.iter().cloned() {
                bucket.push_left(k, lk);
            }
            for rk in rks.iter().cloned() {
                bucket.push_right(k, rk);
            }
        }
        let mut actual = HashSet::new();
        bucket.for_all(|l, r| {
            actual.insert((l.clone(), r.clone()));
        });

        assert_eq!(expected, actual);
    }
}
