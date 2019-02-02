#[macro_use]
extern crate log;
#[macro_use]
extern crate danny;

use danny::baseline;
use danny::baseline::Baselines;
use danny::config::*;
use danny::experiment::Experiment;
use danny::io::ReadDataFile;
use danny::lsh;
use danny::lsh::LSHFunction;
use danny::measure::*;
use danny::sketch::*;
use danny::types::*;
use serde_json::Value;
use std::collections::HashMap;

fn main() {
    let config = Config::get();
    danny::logging::init_logging(&config);
    let args = CmdlineConfig::get();
    let mut experiment = Experiment::from_config(&config, &args);

    info!("Starting...");
    let mut rng = config.get_random_generator(0);

    let start = std::time::Instant::now();
    let count = match args.algorithm.as_ref() {
        "fixed-lsh" => match args.measure.as_ref() {
            "cosine" => {
                let k = args.k.expect("K is needed on the command line");
                let repetitions = lsh::Hyperplane::repetitions_at_range(args.threshold, k);
                let dim = UnitNormVector::peek_first(&args.left_path.clone().into()).dim();
                let threshold = args.threshold;
                let hash_funs = lsh::Hyperplane::collection(k, repetitions, dim, &mut rng);
                let sketcher_pair = args.sketch_bits.map(|bits| {
                    (
                        LongSimHash::new(bits, dim, &mut rng),
                        SketchPredicate::cosine(bits, threshold, config.get_sketch_epsilon()),
                    )
                });
                lsh::fixed_param_lsh::<UnitNormVector, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    hash_funs,
                    sketcher_pair,
                    move |a, b| InnerProduct::cosine(a, b) >= threshold,
                    &config,
                    &mut experiment,
                )
            }
            "jaccard" => {
                let k = args.k.expect("K is needed on the command line");
                let repetitions = lsh::MinHash::repetitions_at_range(args.threshold, k);
                let threshold = args.threshold;
                let hash_funs = lsh::MinHash::collection(k, repetitions, &mut rng);
                let sketcher_pair = args.sketch_bits.map(|bits| {
                    (
                        OneBitMinHash::new(bits, &mut rng),
                        SketchPredicate::jaccard(bits, threshold, config.get_sketch_epsilon()),
                    )
                });
                lsh::fixed_param_lsh::<BagOfWords, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    hash_funs,
                    sketcher_pair,
                    move |a, b| Jaccard::jaccard(a, b) >= threshold,
                    &config,
                    &mut experiment,
                )
            }
            _ => unimplemented!("Unknown measure {}", args.measure),
        },
        "all-2-all" => match args.measure.as_ref() {
            "cosine" => baseline::all_pairs_parallel::<UnitNormVector, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                InnerProduct::cosine,
                &config,
            ),
            "jaccard" => baseline::all_pairs_parallel::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Jaccard::jaccard,
                &config,
            ),
            _ => unimplemented!(),
        },
        "seq-all-2-all" => match args.measure.as_ref() {
            "cosine" => baseline::sequential::<UnitNormVector, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                InnerProduct::cosine,
            ),
            "jaccard" => baseline::sequential::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Jaccard::jaccard,
            ),
            _ => unimplemented!(),
        },
        _ => unimplemented!("Unknown algorithm {}", args.algorithm),
    };
    let end = std::time::Instant::now();
    let total_time = end - start;
    let total_time = total_time.as_secs() * 1000 + total_time.subsec_millis() as u64;
    if config.is_master() {
        let recall = Baselines::new(&config)
            .recall(&args.left_path, &args.right_path, args.threshold, count)
            .expect("Could not compute the recall! Missing entry in the baseline file?");
        println!(
            "Pairs above similarity {} are {} (recall {})",
            args.threshold, count, recall
        );
        experiment.append(
            "result",
            row!("output_size" => count, "total_time_ms" => total_time, "recall" => recall),
        );
        experiment.save();
    }
}
