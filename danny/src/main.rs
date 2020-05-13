#[macro_use]
extern crate rusqlite;
#[macro_use]
extern crate log;
#[macro_use]
extern crate danny;

use danny::baseline;
use danny::baseline::Baselines;
use danny::config::*;
use danny::experiment::Experiment;
use danny::io::*;
use danny::logging::*;
use danny::lsh::algorithms::*;
use danny::operators::*;
use danny_base::lsh::*;
use danny_base::measure::*;
use danny_base::sketch::*;
use danny_base::types::*;
use std::collections::HashMap;
use std::fmt::Debug;

#[cfg(feature = "one-round-lsh")]
fn run_one_round_lsh<SV>(config: &Config, experiment: &mut Experiment) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut hasher_rng = config.get_random_generator(0);
    let mut sketcher_rng = config.get_random_generator(1);
    match content_type(&config.left_path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(config.left_path.clone().into()).dim();
            let threshold = config.threshold;
            let sketch_bits = config.sketch_bits;
            let sketcher = SV::from_cosine(dim, &mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.sketch_epsilon);
            let k = config.k.expect("K is needed on the command line");
            one_round_lsh::<Vector, _, _, _, _, _, _>(
                &config.left_path,
                &config.right_path,
                threshold,
                k,
                Hyperplane::builder(dim),
                sketcher,
                sketch_predicate,
                move |a, b| InnerProduct::inner_product(a, b) >= threshold,
                &mut hasher_rng,
                &config,
                experiment,
            )
        }
        ContentType::BagOfWords => {
            let k = config.k.expect("K is needed on the command line");
            let threshold = config.threshold;
            let sketch_bits = config.sketch_bits;
            let sketcher = SV::from_jaccard(&mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.sketch_epsilon);
            one_round_lsh::<BagOfWords, _, _, _, _, _, _>(
                &config.left_path,
                &config.right_path,
                threshold,
                k,
                OneBitMinHash::builder(),
                sketcher,
                sketch_predicate,
                move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                &mut hasher_rng,
                &config,
                experiment,
            )
        }
    }
}

#[cfg(feature = "two-round-lsh")]
fn run_two_round_lsh<SV>(config: &Config, experiment: &mut Experiment) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut hasher_rng = config.get_random_generator(0);
    let mut sketcher_rng = config.get_random_generator(1);
    let threshold = config.threshold;
    let sketch_bits = config.sketch_bits;
    let k = config.k.expect("K is needed on the command line");
    let k2 = config.k2.expect("k2 is needed on the command line");

    match content_type(&config.left_path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(config.left_path.clone().into()).dim();
            let sketcher = SV::from_cosine(dim, &mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.sketch_epsilon);
            two_round_lsh::<Vector, _, _, _, _, _, _>(
                &config.left_path,
                &config.right_path,
                threshold,
                k,
                k2,
                Hyperplane::builder(dim),
                sketcher,
                sketch_predicate,
                move |a, b| InnerProduct::inner_product(a, b) >= threshold,
                &mut hasher_rng,
                &config,
                experiment,
            )
        }
        ContentType::BagOfWords => {
            let k = config.k.expect("K is needed on the command line");
            let threshold = config.threshold;
            let sketch_bits = config.sketch_bits;
            let sketcher = SV::from_jaccard(&mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.sketch_epsilon);
            two_round_lsh::<BagOfWords, _, _, _, _, _, _>(
                &config.left_path,
                &config.right_path,
                threshold,
                k,
                k2,
                OneBitMinHash::builder(),
                sketcher,
                sketch_predicate,
                move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                &mut hasher_rng,
                &config,
                experiment,
            )
        }
    }
}

#[cfg(feature = "two-round-lsh")]
fn run_hu_et_al<SV>(config: &Config, experiment: &mut Experiment) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut hasher_rng = config.get_random_generator(0);
    let mut sketcher_rng = config.get_random_generator(1);
    let threshold = config.threshold;
    let sketch_bits = config.sketch_bits;
    let k = config.k.expect("K is needed on the command line");

    match content_type(&config.left_path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(config.left_path.clone().into()).dim();
            let sketcher = SV::from_cosine(dim, &mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.sketch_epsilon);
            hu_baseline::<Vector, _, _, _, _, _, _>(
                &config.left_path,
                &config.right_path,
                threshold,
                k,
                Hyperplane::builder(dim),
                sketcher,
                sketch_predicate,
                move |a, b| Vector::inner_product(a, b) >= threshold,
                &mut hasher_rng,
                &config,
                experiment,
            )
        }
        ContentType::BagOfWords => {
            let threshold = config.threshold;
            let sketch_bits = config.sketch_bits;
            let sketcher = SV::from_jaccard(&mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.sketch_epsilon);
            hu_baseline::<BagOfWords, _, _, _, _, _, _>(
                &config.left_path,
                &config.right_path,
                threshold,
                k,
                OneBitMinHash::builder(),
                sketcher,
                sketch_predicate,
                move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                &mut hasher_rng,
                &config,
                experiment,
            )
        }
    }
}

fn main() {
    let config = Config::get();
    init_logging(&config);
    if config.no_dedup {
        warn!("Running with NO DUPLICATE ELIMINATION");
    }
    let mut experiment = Experiment::from_config(config.clone());

    debug!("Starting...");
    debug!("Initial memory {}", proc_mem!());

    let threshold = config.threshold;
    let start = std::time::Instant::now();

    let count: usize = match config.algorithm.as_ref() {
        #[cfg(feature = "one-round-lsh")]
        "one-round-lsh" => match config.sketch_bits {
            0 => run_one_round_lsh::<Sketch0>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            64 => run_one_round_lsh::<Sketch64>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            128 => run_one_round_lsh::<Sketch128>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            256 => run_one_round_lsh::<Sketch256>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            512 => run_one_round_lsh::<Sketch512>(&config, &mut experiment),
            bits => panic!("Unsupported number of sketch bits: {}", bits),
        },
        #[cfg(feature = "two-round-lsh")]
        "two-round-lsh" => match config.sketch_bits {
            0 => run_two_round_lsh::<Sketch0>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            64 => run_two_round_lsh::<Sketch64>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            128 => run_two_round_lsh::<Sketch128>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            256 => run_two_round_lsh::<Sketch256>(&config, &mut experiment),
            #[cfg(feature = "sketching")]
            512 => run_two_round_lsh::<Sketch512>(&config, &mut experiment),
            bits => panic!("Unsupported number of sketch bits: {}", bits),
        },
        #[cfg(feature = "hu-et-al")]
        "hu-et-al" => match content_type(&config.left_path) {
            ContentType::Vector => match config.sketch_bits {
                0 => run_hu_et_al::<Sketch0>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                64 => run_hu_et_al::<Sketch64>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                128 => run_hu_et_al::<Sketch128>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                256 => run_hu_et_al::<Sketch256>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                512 => run_hu_et_al::<Sketch512>(&config, &mut experiment),
                bits => panic!("Unsupported number of sketch bits: {}", bits),
            },
            ContentType::BagOfWords => match config.sketch_bits {
                0 => run_hu_et_al::<Sketch0>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                64 => run_hu_et_al::<Sketch64>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                128 => run_hu_et_al::<Sketch128>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                256 => run_hu_et_al::<Sketch256>(&config, &mut experiment),
                #[cfg(feature = "sketching")]
                512 => run_hu_et_al::<Sketch512>(&config, &mut experiment),
                bits => panic!("Unsupported number of sketch bits: {}", bits),
            },
        },
        #[cfg(feature = "all-2-all")]
        "all-2-all" => match content_type(&config.left_path) {
            ContentType::Vector => baseline::all_pairs_parallel::<Vector, _>(
                config.threshold,
                &config.left_path,
                &config.right_path,
                move |a, b| InnerProduct::inner_product(a, b) >= threshold,
                &config,
            ),
            ContentType::BagOfWords => baseline::all_pairs_parallel::<BagOfWords, _>(
                config.threshold,
                &config.left_path,
                &config.right_path,
                move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                &config,
            ),
        },
        #[cfg(feature = "seq-all-2-all")]
        "seq-all-2-all" => match content_type(&config.left_path) {
            ContentType::Vector => baseline::sequential::<Vector, _>(
                config.threshold,
                &config.left_path,
                &config.right_path,
                InnerProduct::inner_product,
            ),
            ContentType::BagOfWords => baseline::sequential::<BagOfWords, _>(
                config.threshold,
                &config.left_path,
                &config.right_path,
                Jaccard::jaccard,
            ),
        },
        "" => panic!(), // This is here just for type checking when no features are selected
        _ => unimplemented!("Unknown algorithm {}", config.algorithm),
    };

    info!("Final count: {}", count);

    let end = std::time::Instant::now();
    let total_time_d = end - start;
    let total_time = total_time_d.as_secs() * 1000 + u64::from(total_time_d.subsec_millis());
    if config.is_master() {
        // let baselines = Baselines::new(&config);
        // let recall = baselines
        //     .recall(
        //         &config.left_path,
        //         &config.right_path,
        //         config.threshold,
        //         count,
        //     )
        //     .unwrap_or(-1.0);
        unimplemented!("compute speedup and recall starting from the database");
        // let speedup = baselines
        //     .speedup(
        //         &config.left_path,
        //         &config.right_path,
        //         config.threshold,
        //         total_time as f64 / 1000.0,
        //     )
        //     .unwrap_or(-1.0);
        info!(
            "pairs above similarity {} are {} (time {:?})",
            config.threshold, count, total_time_d
        );
        experiment.set_output_size(count);
        experiment.set_total_time_ms(total_time);
        experiment.save();
    }
}
