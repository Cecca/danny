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
use danny::lsh::algorithms::distributed_lsh;
use danny::lsh::algorithms::simple_adaptive;
use danny::lsh::algorithms::simple_fixed;
use danny::operators::*;
use danny_base::lsh::*;
use danny_base::measure::*;
use danny_base::sketch::*;
use danny_base::types::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;

fn run_lsh<SV>(args: &CmdlineConfig, config: &Config, experiment: &mut Experiment) -> usize
where
    SV: BitBasedSketch + FromCosine + FromJaccard + SketchData + Debug,
{
    let mut rng = config.get_random_generator(0);
    match args.measure.as_ref() {
        "cosine" => {
            let dim = UnitNormVector::peek_one(args.left_path.clone().into()).dim();
            let threshold = args.threshold;
            let sketch_bits = args.sketch_bits.expect("Sketches are mandatory");
            let sketcher = SV::from_cosine(dim, &mut rng);
            let sketch_predicate =
                SketchPredicate::cosine(sketch_bits, threshold, config.get_sketch_epsilon());
            let k = args.k.expect("K is needed on the command line");
            match args.rounds {
                Rounds::One => match k {
                    ParamK::Fixed(k) => simple_fixed::<UnitNormVector, _, _, _, _, _, _>(
                        &args.left_path,
                        &args.right_path,
                        threshold,
                        k,
                        Hyperplane::builder(dim),
                        sketcher,
                        sketch_predicate,
                        move |a, b| InnerProduct::cosine(a, b) >= threshold,
                        &mut rng,
                        &config,
                        experiment,
                    ),
                    ParamK::Adaptive(_, max_k) => {
                        simple_adaptive::<UnitNormVector, _, _, _, _, _, _>(
                            &args.left_path,
                            &args.right_path,
                            threshold,
                            max_k,
                            Hyperplane::builder(dim),
                            sketcher,
                            sketch_predicate,
                            move |a, b| InnerProduct::cosine(a, b) >= threshold,
                            &mut rng,
                            &config,
                            experiment,
                        )
                    }
                },
                Rounds::Multi => distributed_lsh::<UnitNormVector, _, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    threshold,
                    k,
                    Hyperplane::builder(dim),
                    sketcher,
                    sketch_predicate,
                    move |a, b| InnerProduct::cosine(a, b) >= threshold,
                    &mut rng,
                    &config,
                    experiment,
                ),
            }
        }
        "jaccard" => {
            let k = args.k.expect("K is needed on the command line");
            let threshold = args.threshold;
            let sketch_bits = args.sketch_bits.expect("Sketch bits are mandatory");
            let sketcher = SV::from_jaccard(&mut rng);
            let sketch_predicate =
                SketchPredicate::jaccard(sketch_bits, threshold, config.get_sketch_epsilon());
            match args.rounds {
                Rounds::One => match k {
                    ParamK::Fixed(k) => simple_fixed::<BagOfWords, _, _, _, _, _, _>(
                        &args.left_path,
                        &args.right_path,
                        threshold,
                        k,
                        OneBitMinHash::builder(),
                        sketcher,
                        sketch_predicate,
                        move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                        &mut rng,
                        &config,
                        experiment,
                    ),
                    ParamK::Adaptive(_, max_k) => simple_adaptive::<BagOfWords, _, _, _, _, _, _>(
                        &args.left_path,
                        &args.right_path,
                        threshold,
                        max_k,
                        OneBitMinHash::builder(),
                        sketcher,
                        sketch_predicate,
                        move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                        &mut rng,
                        &config,
                        experiment,
                    ),
                },
                Rounds::Multi => distributed_lsh::<BagOfWords, _, _, _, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    threshold,
                    k,
                    OneBitMinHash::builder(),
                    sketcher,
                    sketch_predicate,
                    move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
                    &mut rng,
                    &config,
                    experiment,
                ),
            }
        }
        _ => unimplemented!("Unknown measure {}", args.measure),
    }
}

fn main() {
    let config = Config::get();
    init_logging(&config);
    let args = CmdlineConfig::get();
    let mut experiment = Experiment::from_config(&config, &args);

    debug!("Starting...");
    debug!("Initial memory {}", proc_mem!());

    let threshold = args.threshold;
    let start = std::time::Instant::now();

    let count = match args.algorithm.as_ref() {
        "lsh" => match args.sketch_bits {
            Some(0) | None => run_lsh::<Sketch0>(&args, &config, &mut experiment),
            Some(64) => run_lsh::<Sketch64>(&args, &config, &mut experiment),
            Some(128) => run_lsh::<Sketch128>(&args, &config, &mut experiment),
            Some(256) => run_lsh::<Sketch256>(&args, &config, &mut experiment),
            Some(512) => run_lsh::<Sketch512>(&args, &config, &mut experiment),
            Some(bits) => panic!("Unsupported number of sketch bits: {}", bits),
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
        experiment.save_csv();
    }
}
