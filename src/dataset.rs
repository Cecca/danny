use crate::operators::*;
use std::fmt;
use std::fmt::Debug;
use std::ops::Index;

pub struct ChunkedDataset<K, V>
where
    K: Route,
{
    global_n: usize,
    chunks: Vec<Vec<(K, V)>>,
}

impl<K, V> ChunkedDataset<K, V>
where
    K: Route,
{
    pub fn empty() -> Self {
        ChunkedDataset {
            global_n: 0usize,
            chunks: Vec::new(),
        }
    }

    pub fn builder(num_chunks: usize) -> ChunkedDatasetBuilder<K, V> {
        let mut chunks = Vec::with_capacity(num_chunks);
        for _ in 0..num_chunks {
            chunks.push(Vec::new());
        }
        ChunkedDatasetBuilder { chunks }
    }

    pub fn len(&self) -> usize {
        self.chunks.iter().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stripe_len(
        &self,
        matrix: MatrixDescription,
        direction: MatrixDirection,
        worker: u64,
    ) -> usize {
        let (chunk_idx, stripe_idx, n_stripes) = match direction {
            MatrixDirection::Rows => {
                let (chunk_idx, stripe_idx) = matrix.row_major_to_pair(worker);
                let n_stripes = matrix.columns as usize;
                (chunk_idx, stripe_idx, n_stripes)
            }
            MatrixDirection::Columns => {
                let (stripe_idx, chunk_idx) = matrix.row_major_to_pair(worker);
                let n_stripes = matrix.rows as usize;
                (chunk_idx, stripe_idx, n_stripes)
            }
        };
        let stripe_elems = self.chunks[chunk_idx as usize].len() / n_stripes + 1;
        let start = stripe_idx as usize * stripe_elems;
        let end = std::cmp::min(
            self.chunks[chunk_idx as usize].len(),
            (stripe_idx + 1) as usize * stripe_elems,
        );
        self.chunks[chunk_idx as usize][start..end].len()
    }

    pub fn chunk_len(&self, chunk_idx: usize) -> usize {
        self.chunks[chunk_idx].len()
    }

    pub fn iter_chunk(&self, chunk_idx: usize) -> impl Iterator<Item = &(K, V)> {
        self.chunks[chunk_idx].iter()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.chunks.iter().flat_map(|c| c.iter())
    }

    pub fn iter_stripe(
        &self,
        matrix: MatrixDescription,
        direction: MatrixDirection,
        worker: u64,
    ) -> impl Iterator<Item = &(K, V)> {
        let (chunk_idx, stripe_idx, n_stripes) = match direction {
            MatrixDirection::Rows => {
                let (chunk_idx, stripe_idx) = matrix.row_major_to_pair(worker);
                let n_stripes = matrix.columns as usize;
                (chunk_idx, stripe_idx, n_stripes)
            }
            MatrixDirection::Columns => {
                let (stripe_idx, chunk_idx) = matrix.row_major_to_pair(worker);
                let n_stripes = matrix.rows as usize;
                (chunk_idx, stripe_idx, n_stripes)
            }
        };
        let stripe_elems = self.chunks[chunk_idx as usize].len() / n_stripes + 1;
        let start = stripe_idx as usize * stripe_elems;
        let end = std::cmp::min(
            self.chunks[chunk_idx as usize].len(),
            (stripe_idx + 1) as usize * stripe_elems,
        );
        debug!(
            "[w{}] {:?}: chunk {} stripe {} [{} - {}] / {}",
            worker,
            direction,
            chunk_idx,
            stripe_idx,
            start,
            end,
            self.chunks[chunk_idx as usize].len()
        );
        self.chunks[chunk_idx as usize][start..end].iter()
    }
}

impl<K, V> Debug for ChunkedDataset<K, V>
where
    K: Route + Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        writeln!(f, "{:#?}", self.chunks)
    }
}

impl<K, V> Index<&K> for ChunkedDataset<K, V>
where
    K: Route,
{
    type Output = V;

    fn index(&self, index: &K) -> &Self::Output {
        let n_chunks = self.chunks.len();
        let i = index.route() as usize;
        let chunk_id = i % n_chunks;
        let in_chunk_idx = (i - chunk_id) / n_chunks;
        &self.chunks[chunk_id][in_chunk_idx].1
    }
}

pub struct ChunkedDatasetBuilder<K, V>
where
    K: Route,
{
    chunks: Vec<Vec<(K, V)>>,
}

impl<K, V> ChunkedDatasetBuilder<K, V>
where
    K: Route,
{
    pub fn insert(&mut self, k: K, v: V) {
        let n_chunks = self.chunks.len();
        self.chunks[k.route() as usize % n_chunks].push((k, v));
    }

    pub fn finish(mut self, global_n: usize) -> ChunkedDataset<K, V> {
        for chunk in self.chunks.iter_mut() {
            chunk.sort_by_key(|pair| pair.0.route());
        }
        ChunkedDataset {
            global_n,
            chunks: self.chunks,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_chunked_dataset() {
        let chunks = 5;
        let mut actual = ChunkedDataset::builder(chunks);
        let mut expected = HashMap::new();
        for i in 0..1000 {
            if i % chunks == 1 || i % chunks == 4 {
                let i = i as u32;
                actual.insert(i, i);
                expected.insert(i, i);
            }
        }
        let actual = actual.finish();
        assert_eq!(actual.len(), expected.len());

        for i in 0..1000 {
            if i % chunks == 1 || i % chunks == 4 {
                let i = i as u32;
                assert_eq!(actual[&i], i);
            }
        }

        for (k, v) in expected.iter() {
            assert_eq!(actual[k], *v);
        }

        assert_eq!(actual.iter().count(), expected.iter().count());
    }
}
