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

fn add_difficulties<D>(path: &PathBuf, range: f64, vecs: &mut Vec<(f64, D)>) {
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
            vecs[idx].0 = difficulty;
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
    seed: u64,
) where
    D: ReadBinaryFile
        + WriteBinaryFile
        + serde::Serialize
        + Send
        + Sync
        + Default
        + Clone
        + Perturb
        + 'static,
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
    let mut last_index = data.len() - 1;
    while data[last_index].0.is_infinite() {
        last_index -= 1;
    }
    let data = &data[..last_index];
    info!("Sorted by difficulty");
    let mut weights = Vec::new();
    let mut last_difficulty = 0.0;
    let mut i = 0;
    while i < data.len() {
        // find a run of equal difficulties
        let mut j = i + 1;
        let difficulty = data[i].0;
        while j < data.len() && data[j].0 == difficulty {
            j += 1;
        }
        let num_equal = (j - i) as f64;
        let diff = difficulty - last_difficulty;
        // Distribute the weight across all equal difficulty points
        let weight = diff / num_equal;
        while i < j {
            weights.push(weight);
            i += 1;
        }
        last_difficulty = difficulty;
    }
    assert!(weights.len() == data.len());

    let mut rng = XorShiftRng::seed_from_u64(seed);
    let index_distribution =
        WeightedIndex::new(weights).expect("Problem setting up the distribution");
    info!("Start sampling {} elements", target);
    let mut cnt = 0;
    let output_iter = std::iter::from_fn(move || {
        if cnt >= target {
            info!("Written {} elements", cnt);
            return None;
        }
        cnt += 1;
        let idx = rng.sample(&index_distribution);
        Some(data[idx].1.clone())
        // Some(data[idx].1.perturb(&mut rng))
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
        (@arg TARGET: -s +takes_value +required "Target size")
        (@arg SEED: --seed +takes_value "Seed for the random number generator")
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
    let datatype: String = matches.value_of("TYPE").unwrap().to_owned();
    match datatype.as_ref() {
        "bag-of-words" => run::<BagOfWords>(&input, &output, &difficulty_path, range, target, seed),
        "unit-norm-vector" => {
            run::<UnitNormVector>(&input, &output, &difficulty_path, range, target, seed)
        }
        e => panic!("Unsupported measure {}", e),
    };
    info!("Done!");
}
