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
use danny::lsh;

use danny::measure::*;
use danny::sketch::*;
use danny::types::*;
use serde_json::Value;
use std::collections::HashMap;

fn main() {
    let config = Config::get();
    init_logging(&config);
    let args = CmdlineConfig::get();
    let mut experiment = Experiment::from_config(&config, &args);

    debug!("Starting...");
    debug!("Initial memory {}", proc_mem!());
    let mut rng = config.get_random_generator(0);

    let threshold = args.threshold;
    let start = std::time::Instant::now();
    let count = match args.algorithm.as_ref() {
        "lsh" => match args.measure.as_ref() {
            "cosine" => {
                let k = args.k.expect("K is needed on the command line");
                let dim = UnitNormVector::peek_one(args.left_path.clone().into()).dim();
                let threshold = args.threshold;
                let hash_funs_builder = lsh::Hyperplane::collection_builder(threshold, dim);
                let (sketcher, sketch_predicate) = args
                    .sketch_bits
                    .map(|bits| {
                        (
                            LongSimHash::new(bits, dim, &mut rng),
                            SketchPredicate::cosine(bits, threshold, config.get_sketch_epsilon()),
                        )
                    })
                    .expect("FIXME: Make the sketches mandatory");
                lsh::fixed_param_lsh::<UnitNormVector, _, _, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    k,
                    hash_funs_builder,
                    sketcher,
                    sketch_predicate,
                    move |a, b| InnerProduct::cosine(a, b) >= threshold,
                    &mut rng,
                    &config,
                    &mut experiment,
                )
            }
            "jaccard" => {
                let k = args.k.expect("K is needed on the command line");
                let threshold = args.threshold;
                let hash_funs_builder = lsh::OneBitMinHash::collection_builder(threshold);
                let (sketcher, sketch_predicate) = args
                    .sketch_bits
                    .map(|bits| {
                        (
                            LongOneBitMinHash::new(bits, &mut rng),
                            SketchPredicate::jaccard(bits, threshold, config.get_sketch_epsilon()),
                        )
                    })
                    .expect("FIXME: Make the sketches mandatory");
                lsh::fixed_param_lsh::<BagOfWords, _, _, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    k,
                    hash_funs_builder,
                    sketcher,
                    sketch_predicate,
                    move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                    &mut rng,
                    &config,
                    &mut experiment,
                )
            }
            _ => unimplemented!("Unknown measure {}", args.measure),
        },
        "local-lsh" => match args.measure.as_ref() {
            "cosine" => {
                let k = args.k.expect("K is needed on the command line");
                let dim = UnitNormVector::peek_one(args.left_path.clone().into()).dim();
                let threshold = args.threshold;
                let hash_funs_builder = lsh::Hyperplane::collection_builder(threshold, dim);
                lsh::local_lsh::<UnitNormVector, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    k,
                    hash_funs_builder,
                    move |a, b| InnerProduct::cosine(a, b) >= threshold,
                    &config,
                    &mut rng,
                    &mut experiment,
                )
            }
            "jaccard" => {
                let k = args.k.expect("K is needed on the command line");
                let threshold = args.threshold;
                let hash_funs_builder = lsh::OneBitMinHash::collection_builder(threshold);
                lsh::local_lsh::<BagOfWords, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    k,
                    hash_funs_builder,
                    move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                    &config,
                    &mut rng,
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
                move |a, b| InnerProduct::cosine(a, b) >= threshold,
                &config,
            ),
            "jaccard" => baseline::all_pairs_parallel::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
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
    let total_time_d = end - start;
    let total_time = total_time_d.as_secs() * 1000 + u64::from(total_time_d.subsec_millis());
    if config.is_master() {
        let baselines = Baselines::new(&config);
        let recall = baselines
            .recall(&args.left_path, &args.right_path, args.threshold, count)
            .expect("Could not compute the recall! Missing entry in the baseline file?");
        let speedup = baselines
            .speedup(
                &args.left_path,
                &args.right_path,
                args.threshold,
                total_time as f64 / 1000.0,
            )
            .expect("Could not compute the speedup! Missing entry in the baseline file?");
        info!(
            "Pairs above similarity {} are {} (time {:?}, recall {}, speedup {})",
            args.threshold, count, total_time_d, recall, speedup
        );
        experiment.append(
            "result",
            row!("output_size" => count, "total_time_ms" => total_time, "recall" => recall, "speedup" => speedup),
        );
        experiment.save();
    }
}
