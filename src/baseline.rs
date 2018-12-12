use io::ReadDataFile;
use std::path::PathBuf;

pub fn sequential<T, F>(thresh: f64, left: &[T], right: &[T], sim_fn: F)
where
    T: ReadDataFile,
    F: Fn(&T, &T) -> f64,
{
    let mut sim_cnt = 0;
    let mut cnt = 0;
    for l in left.iter() {
        for r in right.iter() {
            cnt += 1;
            let sim = sim_fn(l, r);
            if sim >= thresh {
                sim_cnt += 1;
            }
        }
    }
    println!("There are {} simlar pairs our of {}", sim_cnt, cnt);
}
