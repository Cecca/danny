use crate::operators::Route;
use danny_base::types::ElementId;

/// Utilities to compute the (self) cartesian product of a stream.
///
/// In particular, this struct allows to replicate each item of the stream
/// to each subproblem of the cartesian product
pub struct SelfCartesian {
    groups: u8,
}

impl SelfCartesian {
    pub fn with_groups(groups: u8) -> Self {
        Self { groups }
    }

    pub fn for_peers(peers: usize) -> Self {
        if peers == 1 {
            Self::with_groups(1)
        } else {
            let groups = (0.5 + (0.25 + 2.0 * peers as f64).sqrt()).ceil() as u8;
            Self::with_groups(groups)
        }
    }

    pub fn keys_for(&self, k: ElementId) -> impl Iterator<Item = (CartesianKey, Marker)> {
        let diag_id = (k.0 % self.groups as u32) as u8;
        let diag = Some((CartesianKey(diag_id, diag_id), Marker::Both));
        let rows = (0..diag_id).map(move |i| (CartesianKey(i, diag_id), Marker::Left));
        let cols =
            ((diag_id + 1)..self.groups).map(move |j| (CartesianKey(diag_id, j), Marker::Right));
        diag.into_iter().chain(rows).chain(cols)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Abomonation)]
pub enum Marker {
    Left,
    Right,
    Both,
}

impl Marker {
    pub fn keep_left(&self) -> bool {
        match self {
            Self::Left => true,
            Self::Right => false,
            Self::Both => {
                panic!("you should not filter using keep_left when some instances are `Both`")
            }
        }
    }
    pub fn keep_right(&self) -> bool {
        match self {
            Self::Right => true,
            Self::Left => false,
            Self::Both => {
                panic!("you should not filter using keep_right when some instances are `Both`")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Abomonation)]
pub struct CartesianKey(pub u8, pub u8);

impl Route for CartesianKey {
    // Route by mapping the coordinate in diagonal major order
    fn route(&self) -> u64 {
        // let diagonal = self.1 - self.0;
        // let diagonal_position = self.1 - diagonal;
        // let diagonal_lengthV
        self.0 as u64 * 31 + self.1 as u64
    }
}

impl CartesianKey {
    pub fn on_diagonal(&self) -> bool {
        self.0 == self.1
    }
}

#[test]
fn test_diagonal_order() {
    let mut keys = Vec::new();
    let n = 5;
    for i in 0..n {
        for j in i..n {
            let k = CartesianKey(i, j);
            let r = k.route();
            keys.push((r, k));
        }
    }
    keys.sort_by_key(|p| p.0);
    for (r, k) in keys {
        println!("{:?} -> {}", k, r);
    }
    panic!();
}
