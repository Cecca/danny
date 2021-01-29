use danny_base::types::ElementId;

/// Utilities to compute the (self) cartesian product of a stream.
///
/// In particular, this struct allows to replicate each item of the stream
/// to each subproblem of the cartesian product
#[derive(Clone, Copy)]
pub struct SelfCartesian {
    groups: u8,
    num_cells: u64,
}

impl SelfCartesian {
    pub fn with_groups(groups: u8) -> Self {
        let num_cells = groups as u64 * (groups as u64 + 1) / 2;
        Self { groups, num_cells }
    }

    pub fn for_peers(peers: usize) -> Self {
        if peers == 1 {
            Self::with_groups(1)
        } else {
            // upper rounded solution to the equation
            //         g*(g+1) / 2 = p
            // where g is the number of groups we seek and p is the number of peers
            let groups = (-0.5 + (0.25 + 2.0 * peers as f64).sqrt()).ceil() as u8;
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

    /// Route by mapping the coordinate in diagonal major order
    pub fn diagonal_major(&self, k: CartesianKey) -> u64 {
        let g = self.groups as u64;
        let diag = (k.1 - k.0) as u64;
        let offset = k.1 as u64 - diag;
        let triangle_elements = ((g - diag) * ((g - diag) + 1)) / 2;
        self.num_cells - triangle_elements + offset
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

impl CartesianKey {
    pub fn on_diagonal(&self) -> bool {
        self.0 == self.1
    }
}

#[test]
fn test_for_peers() {
    let p = 40;
    let cartesian = SelfCartesian::for_peers(p);
    assert_eq!(cartesian.num_cells, 45);
    assert_eq!(cartesian.groups, 9);
}

#[test]
fn test_diagonal_order() {
    let mut keys = Vec::new();
    let n = 5;
    let cartesian = SelfCartesian::with_groups(n);
    for i in 0..n {
        for j in i..n {
            let k = CartesianKey(i, j);
            let r = cartesian.diagonal_major(k);
            keys.push((r, k));
        }
    }
    keys.sort_by_key(|p| p.0);
    let expected = vec![
        (0, CartesianKey(0, 0)),
        (1, CartesianKey(1, 1)),
        (2, CartesianKey(2, 2)),
        (3, CartesianKey(3, 3)),
        (4, CartesianKey(4, 4)),
        (5, CartesianKey(0, 1)),
        (6, CartesianKey(1, 2)),
        (7, CartesianKey(2, 3)),
        (8, CartesianKey(3, 4)),
        (9, CartesianKey(0, 2)),
        (10, CartesianKey(1, 3)),
        (11, CartesianKey(2, 4)),
        (12, CartesianKey(0, 3)),
        (13, CartesianKey(1, 4)),
        (14, CartesianKey(0, 4)),
    ];
    assert_eq!(keys, expected);
}
