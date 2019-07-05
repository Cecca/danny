/// Generate a diverse dataset starting from the given one
/// and the distribution of its difficulties, measured either
/// as LID or expansion

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate crossbeam_channel;
extern crate danny;
extern crate env_logger;
extern crate rayon;
extern crate serde;

use danny::io::*;
use danny::types::*;
use rand::distributions::Exp1;
use rand::distributions::Normal;
use rand::distributions::Uniform;
use rand::distributions::WeightedIndex;
use rand::Rng;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

use std::io::Write;
use std::path::PathBuf;

trait Perturb {
    fn perturb<R>(&self, rng: &mut R) -> Self
    where
        R: Rng;
}

impl Perturb for UnitNormVector {
    fn perturb<R>(&self, rng: &mut R) -> UnitNormVector
    where
        R: Rng,
    {
        let noise = Normal::new(0.0, 0.01);
        let new_data: Vec<f32> = self
            .data()
            .iter()
            .map(|x| x + rng.sample(noise) as f32)
            .collect();
        UnitNormVector::new(new_data)
    }
}

impl Perturb for BagOfWords {
    fn perturb<R>(&self, rng: &mut R) -> BagOfWords
    where
        R: Rng,
    {
        let num_words = rng.sample(Exp1).ceil() as usize;
        let wordgen = Uniform::new(0u32, self.universe);
        let mut new_words = self.words().clone();
        for _ in 0..num_words {
            let w = rng.sample(wordgen);
            new_words.push(w);
        }
        new_words.sort();
        new_words.dedup();
        BagOfWords::new(self.universe, new_words)
    }
}

#[derive(Clone, Copy)]
enum DifficultyMeasure {
    Expansion,
    LID,
}

impl DifficultyMeasure {
    fn from_path(path: &PathBuf) -> Self {
        match path
            .extension()
            .expect("Path should have extension")
            .to_str()
        {
            Some("exp") => DifficultyMeasure::Expansion,
            Some("lid") => DifficultyMeasure::LID,
            _ => panic!("Unsupported extension"),
        }
    }
}

fn add_difficulties<D>(path: &PathBuf, range: f64, vecs: &mut Vec<(f64, D)>) {
    let measure = DifficultyMeasure::from_path(path);
    let f = File::open(path).expect("Problem opening difficulty file");
    let input = BufReader::new(f);
    let mut cnt = 0;
    for line in input.lines() {
        let line = line.expect("Problem reading line");
        let mut tokens = line.split_whitespace();
        let idx = tokens
            .next()
            .expect("Missing token")
            .parse::<usize>()
            .expect("Problem parsing idx");
        let r = tokens
            .next()
            .expect("Missing token")
            .parse::<f64>()
            .expect("Problem parsing range");
        let difficulty = tokens
            .next()
            .expect("Missing token")
            .parse::<f64>()
            .expect("Problem parsing difficulty");
        if r == range {
            cnt += 1;
            vecs[idx].0 = match measure {
                DifficultyMeasure::LID => difficulty,
                DifficultyMeasure::Expansion => {
                    if difficulty.is_infinite() {
                        0.0
                    } else {
                        1.0 / difficulty
                    }
                }
            };
        }
    }
    assert!(
        cnt > 0,
        "The requested range was not present in the 'difficulty' file"
    );
}

#[allow(clippy::too_many_arguments)]
fn run<D>(
    path: &PathBuf,
    outputpath: &PathBuf,
    difficulty_path: &PathBuf,
    range: f64,
    target: usize,
    buckets: usize,
    bimodal: bool,
    seed: u64,
) where
    D: ReadBinaryFile + WriteBinaryFile + Send + Sync + Default + Clone + Perturb + 'static,
{
    let n = D::num_elements(path.to_path_buf());
    let mut data: Vec<(f64, D)> = vec![(0.0f64, D::default()); n];
    D::read_binary(
        path.to_path_buf(),
        |_| true,
        |c, v| {
            data[c as usize].1 = v;
        },
    );
    info!("Loaded dataset with {} elements", data.len());
    add_difficulties(difficulty_path, range, &mut data);
    data.sort_by(|t1, t2| t1.0.partial_cmp(&t2.0).expect("Problem doing comparison"));
    info!("Sorted by local intrinsic dimensionality");
    // Find the first index where the difficulty score is non zero. We will bucket all these
    // vectors on their own. Otherwise we get with many vectors with LID 0 in
    // the output.
    let gt_0_index = data.iter().take_while(|p| p.0 == 0.0).count();
    let mut bucks: Vec<&[(f64, D)]> = Vec::new();
    bucks.push(&data[0..gt_0_index]);
    let mut cur_idx = gt_0_index;
    let mut bucket_width = (data.len() - gt_0_index) / (buckets - 1);
    if bucket_width == 0 {
        warn!("Too many buckets requested! Using buckets of width 1");
        bucket_width = 1;
    }
    assert!(bucket_width > 0);
    while cur_idx < n {
        info!("Current index is {}", cur_idx);
        let buck_end = std::cmp::min(cur_idx + bucket_width, n);
        bucks.push(&data[cur_idx..buck_end]);
        cur_idx += bucket_width;
    }
    info!("Built buckets");

    let mut cnt = 0;
    let mut rng = XorShiftRng::seed_from_u64(seed);
    let weights = if bimodal {
        let mut ws = vec![0; bucks.len()];
        ws[0] = 1;
        ws[1] = 1;
        ws[bucks.len() - 1] = 1;
        ws
    } else {
        vec![1; bucks.len()]
    };
    // let bucket_distr = Uniform::new(0usize, bucks.len());
    let bucket_distr = WeightedIndex::new(weights).expect("Problem creating the distribution");
    info!("Extremal difficulties: {} and {}", data[0].0, data[n - 1].0);

    info!("Start sampling");
    let output_iter = std::iter::from_fn(move || {
        if cnt >= target {
            return None;
        }
        cnt += 1;
        let idx = rng.sample(&bucket_distr);
        let b = bucks[idx];
        let d = Uniform::new(0, b.len());
        let i = rng.sample(&d);
        let res = b[i].1.perturb(&mut rng);
        Some(res)
    });

    WriteBinaryFile::write_binary(
        outputpath.to_path_buf(),
        D::num_chunks(path.to_path_buf()),
        output_iter,
    );
}

fn main() {
    let matches = clap_app!(gendiverse =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Build a dataset with a diverse distribution of local intrinsic dimensionalities")
        (@arg TYPE: -t +takes_value +required "The type of data, either bag-of-words or unit-norm-vector")
        (@arg RANGE: -r +takes_value +required "The range for the difficulty selection")
        (@arg BUCKETS: -b +takes_value "Number of buckets required for the random sampling, default 100")
        (@arg TARGET: -s +takes_value +required "Target size")
        (@arg SEED: --seed +takes_value "Seed for the random number generator")
        (@arg BIMODAL: --bimodal "Wether to use a bimodal distribution rather than a uniform one")
        (@arg INPUT: +required "The input path")
        (@arg DIFFICULTY: +required "The 'difficulty file' path. Can be either a LID or an expansion file")
        (@arg OUTPUT: +required "The output path")
    )
    .get_matches();

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let difficulty_path: PathBuf = matches.value_of("DIFFICULTY").unwrap().into();
    let output: PathBuf = matches.value_of("OUTPUT").unwrap().into();
    let buckets: usize = matches
        .value_of("BUCKETS")
        .unwrap_or("100")
        .parse::<usize>()
        .expect("Problem parsing the number of buckets");
    let target: usize = matches
        .value_of("TARGET")
        .unwrap()
        .parse::<usize>()
        .expect("Problem parsing the target");
    let seed: u64 = matches
        .value_of("SEED")
        .unwrap_or("140981350987")
        .parse::<u64>()
        .expect("Problem parsing the seed");
    let range: f64 = matches
        .value_of("RANGE")
        .unwrap()
        .parse::<f64>()
        .expect("Problem parsing the seed");
    let bimodal: bool = matches.is_present("BIMODAL");
    let datatype: String = matches.value_of("TYPE").unwrap().to_owned();
    match datatype.as_ref() {
        "bag-of-words" => run::<BagOfWords>(
            &input,
            &output,
            &difficulty_path,
            range,
            target,
            buckets,
            bimodal,
            seed,
        ),
        "unit-norm-vector" => run::<UnitNormVector>(
            &input,
            &output,
            &difficulty_path,
            range,
            target,
            buckets,
            bimodal,
            seed,
        ),
        e => panic!("Unsupported measure {}", e),
    };
    info!("Done!");
}
