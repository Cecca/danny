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
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;

#[cfg(feature = "one-round-lsh")]
fn run_one_round_lsh<SV>(
    args: &CmdlineConfig,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut hasher_rng = config.get_random_generator(0);
    let mut sketcher_rng = config.get_random_generator(1);
    match content_type(&args.left_path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(args.left_path.clone().into()).dim();
            let threshold = args.threshold;
            let sketch_bits = args.sketch_bits.expect("Sketches are mandatory");
            let sketcher = SV::from_cosine(dim, &mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.get_sketch_epsilon());
            let k = args.k.expect("K is needed on the command line");
            one_round_lsh::<Vector, _, _, _, _, _, _>(
                &args.left_path,
                &args.right_path,
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
            let k = args.k.expect("K is needed on the command line");
            let threshold = args.threshold;
            let sketch_bits = args.sketch_bits.expect("Sketch bits are mandatory");
            let sketcher = SV::from_jaccard(&mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.get_sketch_epsilon());
            one_round_lsh::<BagOfWords, _, _, _, _, _, _>(
                &args.left_path,
                &args.right_path,
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
fn run_two_round_lsh<SV>(
    args: &CmdlineConfig,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut hasher_rng = config.get_random_generator(0);
    let mut sketcher_rng = config.get_random_generator(1);
    let threshold = args.threshold;
    let sketch_bits = args.sketch_bits.expect("Sketches are mandatory");
    let k = args.k.expect("K is needed on the command line");
    let k2 = args.k2.expect("k2 is needed on the command line");

    match content_type(&args.left_path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(args.left_path.clone().into()).dim();
            let sketcher = SV::from_cosine(dim, &mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.get_sketch_epsilon());
            two_round_lsh::<Vector, _, _, _, _, _, _>(
                &args.left_path,
                &args.right_path,
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
            let k = args.k.expect("K is needed on the command line");
            let threshold = args.threshold;
            let sketch_bits = args.sketch_bits.expect("Sketch bits are mandatory");
            let sketcher = SV::from_jaccard(&mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.get_sketch_epsilon());
            two_round_lsh::<BagOfWords, _, _, _, _, _, _>(
                &args.left_path,
                &args.right_path,
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
fn run_hu_et_al<SV>(args: &CmdlineConfig, config: &Config, experiment: &mut Experiment) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut hasher_rng = config.get_random_generator(0);
    let mut sketcher_rng = config.get_random_generator(1);
    let threshold = args.threshold;
    let sketch_bits = args.sketch_bits.expect("Sketches are mandatory");
    let k = args.k.expect("K is needed on the command line");

    match content_type(&args.left_path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(args.left_path.clone().into()).dim();
            let sketcher = SV::from_cosine(dim, &mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.get_sketch_epsilon());
            hu_baseline::<Vector, _, _, _, _, _, _>(
                &args.left_path,
                &args.right_path,
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
            let threshold = args.threshold;
            let sketch_bits = args.sketch_bits.expect("Sketch bits are mandatory");
            let sketcher = SV::from_jaccard(&mut sketcher_rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.get_sketch_epsilon());
            hu_baseline::<BagOfWords, _, _, _, _, _, _>(
                &args.left_path,
                &args.right_path,
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
    let args = CmdlineConfig::get();
    let mut experiment = Experiment::from_config(&config, &args);

    debug!("Starting...");
    debug!("Initial memory {}", proc_mem!());

    let threshold = args.threshold;
    let start = std::time::Instant::now();

    let count: usize = match args.algorithm.as_ref() {
        #[cfg(feature = "one-round-lsh")]
        "one-round-lsh" => match args.sketch_bits {
            Some(0) | None => run_one_round_lsh::<Sketch0>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(64) => run_one_round_lsh::<Sketch64>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(128) => run_one_round_lsh::<Sketch128>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(256) => run_one_round_lsh::<Sketch256>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(512) => run_one_round_lsh::<Sketch512>(&args, &config, &mut experiment),
            Some(bits) => panic!("Unsupported number of sketch bits: {}", bits),
        },
        #[cfg(feature = "two-round-lsh")]
        "two-round-lsh" => match args.sketch_bits {
            Some(0) | None => run_two_round_lsh::<Sketch0>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(64) => run_two_round_lsh::<Sketch64>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(128) => run_two_round_lsh::<Sketch128>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(256) => run_two_round_lsh::<Sketch256>(&args, &config, &mut experiment),
            #[cfg(feature = "sketching")]
            Some(512) => run_two_round_lsh::<Sketch512>(&args, &config, &mut experiment),
            Some(bits) => panic!("Unsupported number of sketch bits: {}", bits),
        },
        #[cfg(feature = "hu-et-al")]
        "hu-et-al" => match content_type(&args.left_path) {
            ContentType::Vector => match args.sketch_bits {
                Some(0) | None => run_hu_et_al::<Sketch0>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(64) => run_hu_et_al::<Sketch64>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(128) => run_hu_et_al::<Sketch128>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(256) => run_hu_et_al::<Sketch256>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(512) => run_hu_et_al::<Sketch512>(&args, &config, &mut experiment),
                Some(bits) => panic!("Unsupported number of sketch bits: {}", bits),
            },
            ContentType::BagOfWords => match args.sketch_bits {
                Some(0) | None => run_hu_et_al::<Sketch0>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(64) => run_hu_et_al::<Sketch64>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(128) => run_hu_et_al::<Sketch128>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(256) => run_hu_et_al::<Sketch256>(&args, &config, &mut experiment),
                #[cfg(feature = "sketching")]
                Some(512) => run_hu_et_al::<Sketch512>(&args, &config, &mut experiment),
                Some(bits) => panic!("Unsupported number of sketch bits: {}", bits),
            },
        },
        #[cfg(feature = "all-2-all")]
        "all-2-all" => match content_type(&args.left_path) {
            ContentType::Vector => baseline::all_pairs_parallel::<Vector, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                move |a, b| InnerProduct::inner_product(a, b) >= threshold,
                &config,
            ),
            ContentType::BagOfWords => baseline::all_pairs_parallel::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                &config,
            ),
        },
        #[cfg(feature = "seq-all-2-all")]
        "seq-all-2-all" => match content_type(&args.left_path) {
            ContentType::Vector => baseline::sequential::<Vector, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                InnerProduct::inner_product,
            ),
            ContentType::BagOfWords => baseline::sequential::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Jaccard::jaccard,
            ),
        },
        "" => panic!(), // This is here just for type checking when no features are selected
        _ => unimplemented!("Unknown algorithm {}", args.algorithm),
    };

    info!("Final count: {}", count);

    let end = std::time::Instant::now();
    let total_time_d = end - start;
    let total_time = total_time_d.as_secs() * 1000 + u64::from(total_time_d.subsec_millis());
    if config.is_master() {
        let baselines = Baselines::new(&config);
        let recall = baselines
            .recall(&args.left_path, &args.right_path, args.threshold, count)
            .unwrap_or(-1.0);
        let speedup = baselines
            .speedup(
                &args.left_path,
                &args.right_path,
                args.threshold,
                total_time as f64 / 1000.0,
            )
            .unwrap_or(-1.0);
        info!(
            "Pairs above similarity {} are {} (time {:?}, recall {}, speedup {})",
            args.threshold, count, total_time_d, recall, speedup
        );
        experiment.append(
            "result",
            row!("output_size" => count, "total_time_ms" => total_time, "recall" => recall, "speedup" => speedup),
        );
        experiment.save_csv();
    }
}
