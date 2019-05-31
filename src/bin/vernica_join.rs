#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate crossbeam_channel;
#[macro_use]
extern crate danny;
extern crate env_logger;
extern crate rayon;
extern crate serde;

use danny::baseline::*;
use danny::config::*;
use danny::experiment::*;
use danny::io::*;
use danny::logging::*;
use danny::measure::*;
use danny::operators::Route;
use danny::types::*;
use rand::distributions::Exp1;
use rand::distributions::Normal;
use rand::distributions::Uniform;
use rand::Rng;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::timestamp::Timestamp;
use timely::Data;
use timely::ExchangeData;

/// Broadcast a stream of tokens and ranks
fn rank_tokens<G: Scope>(stream: &Stream<G, (u64, BagOfWords)>) -> Stream<G, (u32, u32)> {
    stream
        .unary_frontier(PipelinePact, "rank_tokens_map", move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut accum = HashMap::new();
            move |input, output| {
                input.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    let entry = accum.entry(t.time().clone()).or_insert_with(HashMap::new);
                    for (_, bow) in data.drain(..) {
                        for &token in bow.words() {
                            *entry.entry(token).or_insert(0usize) += 1;
                        }
                    }
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(&[input.frontier()], |time, _notificator| {
                    let mut session = output.session(&time);
                    if let Some(mut accum) = accum.remove(&time) {
                        session.give_iterator(accum.drain());
                    }
                });
            }
        })
        .unary_frontier(
            ExchangePact::new(|_| 0u64),
            "rank_tokens_reduce",
            move |_, _| {
                let mut notificator = FrontierNotificator::new();
                let mut accum = HashMap::new();
                move |input, output| {
                    input.for_each(|t, data| {
                        let mut data = data.replace(Vec::new());
                        let entry = accum.entry(t.time().clone()).or_insert_with(HashMap::new);
                        for (token, count) in data.drain(..) {
                            *entry.entry(token).or_insert(0usize) += count;
                        }
                        notificator.notify_at(t.retain());
                    });

                    notificator.for_each(&[input.frontier()], |time, _notificator| {
                        let mut session = output.session(&time);
                        if let Some(mut accum) = accum.remove(&time) {
                            let mut counts_vec: Vec<(u32, usize)> = accum.into_iter().collect();
                            counts_vec.sort_by_key(|pair| pair.1); // sort by increasing frequency
                            for (rank, (token, _)) in counts_vec.into_iter().enumerate() {
                                session.give((token, rank as u32));
                            }
                        }
                    });
                }
            },
        )
        .broadcast()
}

type PrefixToken = u32;

#[inline]
fn prefix_len(bow: &BagOfWords, range: f64) -> usize {
    bow.len() - (bow.len() as f64 * range).ceil() as usize + 1
}

fn by_prefix_token<G: Scope>(
    stream: &Stream<G, (u64, BagOfWords)>,
    ranks: &Stream<G, (u32, u32)>,
    range: f64,
) -> Stream<G, (PrefixToken, (u64, BagOfWords))> {
    stream.binary_frontier(
        ranks,
        PipelinePact,
        PipelinePact,
        "emit_by_prefix",
        move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut stash_input = HashMap::new();
            let mut stash_ranks = HashMap::new();
            move |input_stream, input_ranks, output| {
                input_stream.for_each(|t, data| {
                    stash_input
                        .entry(t.time().clone())
                        .or_insert_with(Vec::new)
                        .append(&mut data.replace(Vec::new()));
                    notificator.notify_at(t.retain());
                });
                input_ranks.for_each(|t, data| {
                    stash_ranks
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new)
                        .extend(data.replace(Vec::new()));
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(
                    &[input_stream.frontier(), input_ranks.frontier()],
                    |t, _| {
                        let mut session = output.session(&t);
                        let ranks = stash_ranks.remove(&t).expect("there should be this time");
                        info!("There are {} distinct tokens", ranks.len());
                        let mut bows = stash_input.remove(&t).expect("there sholud be this time");
                        for (c, mut bow) in bows.drain(..) {
                            bow.remap_tokens(&ranks);
                            let prefix = bow.words().iter().take(prefix_len(&bow, range));
                            for &token in prefix {
                                session.give((token, (c, bow.clone())));
                            }
                        }
                    },
                );
            }
        },
    )
}

type PrefixCandidate = (PrefixToken, (u64, BagOfWords));

#[inline]
fn overlap(r: &BagOfWords, s: &BagOfWords, sim: f64) -> usize {
    let t_unrounded = sim * (r.len() + s.len()) as f64 / (1.0 + sim);
    let t_rounded = t_unrounded.round();
    // The rounding below with the comparison with EPS is needed to counter the
    // floating point errors introduced by the division
    let t = if (t_rounded - t_unrounded).abs() < 0.000_000_000_000_01 {
        t_rounded
    } else {
        t_unrounded
    };
    t as usize
}

fn first_match_position(r: &BagOfWords, s: &BagOfWords) -> Option<(usize, usize)> {
    let mut r_iter = r.words().iter().enumerate();
    let mut s_iter = s.words().iter().enumerate();
    let mut cur_r = r_iter.next();
    let mut cur_s = s_iter.next();
    loop {
        if cur_r.is_none() || cur_s.is_none() {
            return None;
        }
        let (idx_r, tok_r) = cur_r.unwrap();
        let (idx_s, tok_s) = cur_s.unwrap();
        if tok_r < tok_s {
            cur_r = r_iter.next();
        } else if tok_r > tok_s {
            cur_s = s_iter.next();
        } else {
            return Some((idx_r, idx_s));
        }
    }
}

/// Return true if the pair should be considered further
fn positional_filter(r: &BagOfWords, s: &BagOfWords, threshold: f64) -> bool {
    if let Some((idx_r, idx_s)) = first_match_position(r, s) {
        let olap = overlap(r, s, threshold);
        olap + idx_r <= r.len() && olap + idx_s <= s.len()
    } else {
        false
    }
}

/// Return true if the pair should be discarded
fn suffix_filter(r: &BagOfWords, s: &BagOfWords, range: f64) -> bool {
    if let Some((idx_r, idx_s)) = first_match_position(r, s) {
        let olap = overlap(r, s, range);
        let suffix_r = &r.words()[idx_r..];
        let suffix_s = &s.words()[idx_s..];
        let pivot_idx_r = suffix_r.len() / 2;
        let pivot = &suffix_r[pivot_idx_r];
        if let Ok(pivot_idx_s) = suffix_s.binary_search(pivot) {
            let max_match_lower = std::cmp::min(pivot_idx_r - idx_r, pivot_idx_s - idx_s);
            let max_match_upper = std::cmp::min(r.len() - pivot_idx_r, s.len() - pivot_idx_s);
            // The 2 takes into account the first match and the pivot
            let max_match = 2 + max_match_lower + max_match_upper;
            max_match > olap
        } else {
            true
        }
    } else {
        false
    }
}

fn filter_candidates<G: Scope>(
    stream_left: &Stream<G, PrefixCandidate>,
    stream_right: &Stream<G, PrefixCandidate>,
    range: f64,
) -> Stream<G, (u64, u64)> {
    stream_left.binary_frontier(
        &stream_right,
        ExchangePact::new(|pair: &PrefixCandidate| pair.0 as u64),
        ExchangePact::new(|pair: &PrefixCandidate| pair.0 as u64),
        "candidate_filtering",
        move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut stash = HashMap::new();
            move |input_left, input_right, output| {
                input_left.for_each(|t, data| {
                    let entry = stash.entry(t.time().clone()).or_insert_with(HashMap::new);
                    for (token, pair) in data.replace(Vec::new()) {
                        entry
                            .entry(token)
                            .or_insert_with(|| (Vec::new(), Vec::new()))
                            .0
                            .push(pair);
                    }
                    notificator.notify_at(t.retain());
                });
                input_right.for_each(|t, data| {
                    let entry = stash.entry(t.time().clone()).or_insert_with(HashMap::new);
                    for (token, pair) in data.replace(Vec::new()) {
                        entry
                            .entry(token)
                            .or_insert_with(|| (Vec::new(), Vec::new()))
                            .1
                            .push(pair);
                    }
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(&[input_left.frontier(), input_right.frontier()], |t, _| {
                    let mut collisions = stash.remove(&t).expect("This time should be present");
                    let mut session = output.session(&t);
                    info!(
                        "Considering candidates: there are {} prefix tokens",
                        collisions.len()
                    );

                    for (_prefix, (left, right)) in collisions.drain() {
                        for (l, l_bow) in left.iter() {
                            for (r, r_bow) in right.iter() {
                                if positional_filter(l_bow, r_bow, range)
                                    && suffix_filter(l_bow, r_bow, range)
                                    && BagOfWords::jaccard_predicate(l_bow, r_bow, range)
                                {
                                    session.give((*l, *r));
                                }
                            }
                        }
                    }
                })
            }
        },
    )
}

fn count_distinct<G: Scope>(stream: &Stream<G, (u64, u64)>) -> Stream<G, usize> {
    stream
        .unary_frontier(
            ExchangePact::new(|pair: &(u64, u64)| pair.route()),
            "distinct",
            |_, _| {
                let mut notificator = FrontierNotificator::new();
                let mut stash = HashMap::new();
                move |input, output| {
                    input.for_each(|t, data| {
                        stash
                            .entry(t.time().clone())
                            .or_insert_with(HashSet::new)
                            .extend(data.replace(Vec::new()));
                        notificator.notify_at(t.retain());
                    });

                    notificator.for_each(&[input.frontier()], |t, _| {
                        if let Some(stash) = stash.remove(&t) {
                            output.session(&t).give(stash.len());
                        }
                    });
                }
            },
        )
        .exchange(|_| 0u64)
        .accumulate(0usize, |sum, data| {
            for &cnt in data.iter() {
                *sum += cnt;
            }
        })
}

fn run(left_path: PathBuf, right_path: PathBuf, range: f64, config: Config) {
    let mut experiment = Experiment::from_env(&config)
        .tag("left", left_path.to_str().unwrap())
        .tag("right", right_path.to_str().unwrap())
        .tag("threshold", range);

    let timely_builder = config.get_timely_builder();
    let start = Instant::now();
    let left_path_2 = left_path.clone();
    let right_path_2 = right_path.clone();

    let result = timely::execute::execute_from(timely_builder.0, timely_builder.1, move |worker| {
        let (mut input_left, mut input_right, probe, captured) =
            worker.dataflow::<u32, _, _>(move |scope| {
                let mut probe = ProbeHandle::<u32>::new();
                let (input_left, stream_left) = scope.new_input();
                let (input_right, stream_right) = scope.new_input();

                let ranks = rank_tokens(&stream_left.concat(&stream_right));
                let by_tokens_left = by_prefix_token(&stream_left, &ranks, range);
                let by_tokens_right = by_prefix_token(&stream_right, &ranks, range);
                let filtered = filter_candidates(&by_tokens_left, &by_tokens_right, range);

                let captured = count_distinct(&filtered).probe_with(&mut probe).capture();

                (input_left, input_right, probe, captured)
            });
        info!("Created dataflow");

        let worker_id = worker.index();
        let num_workers = worker.peers();
        BagOfWords::read_binary(
            left_path.clone(),
            |l| l % num_workers == worker_id,
            |idx, bow| input_left.send((idx, bow)),
        );
        BagOfWords::read_binary(
            right_path.clone(),
            |l| l % num_workers == worker_id,
            |idx, bow| input_right.send((idx, bow)),
        );
        input_left.close();
        input_right.close();

        worker.step_while(|| !probe.done());
        info!("Finished stepping");
        captured
    })
    .expect("Problems running the dataflow");

    let mut matching_count = 0usize;
    for guard in result.join() {
        let result = guard.expect("Error getting the result").extract();
        if !result.is_empty() {
            let sum: usize = result.into_iter().flat_map(|pair| pair.1).sum::<usize>();
            matching_count += sum;
        }
    }

    let end = std::time::Instant::now();
    let total_time_d = end - start;
    let total_time = total_time_d.as_secs() * 1000 + u64::from(total_time_d.subsec_millis());
    if config.is_master() {
        let baselines = Baselines::new(&config);
        let recall = baselines
            .recall(
                &left_path_2.to_str().unwrap(),
                &right_path_2.to_str().unwrap(),
                range,
                matching_count,
            )
            .expect("Could not compute the recall! Missing entry in the baseline file?");
        let speedup = baselines
            .speedup(
                &left_path_2.to_str().unwrap(),
                &right_path_2.to_str().unwrap(),
                range,
                total_time as f64 / 1000.0,
            )
            .expect("Could not compute the speedup! Missing entry in the baseline file?");
        info!(
            "Pairs above similarity {} are {} (time {:?}, recall {}, speedup {})",
            range, matching_count, total_time_d, recall, speedup
        );
        experiment.append(
            "result",
            row!("output_size" => matching_count, "total_time_ms" => total_time, "recall" => recall, "speedup" => speedup),
        );
        experiment.save();
    }
}

fn main() {
    let matches = clap_app!(vernica_join =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Run the Vernica et al distributed join algorithm (just Jaccard similarity)")
        (@arg RANGE: -r +takes_value +required "The similarity threshold")
        (@arg LEFT: +required "The left input path")
        (@arg RIGHT: +required "The right input path")
    )
    .get_matches();

    let input_left: PathBuf = matches.value_of("LEFT").unwrap().into();
    let input_right: PathBuf = matches.value_of("RIGHT").unwrap().into();
    let range: f64 = matches
        .value_of("RANGE")
        .unwrap()
        .parse::<f64>()
        .expect("Problem parsing the seed");

    let config = Config::get();
    init_logging(&config);

    info!("Starting");
    run(input_left, input_right, range, config);

    info!("Done!");
}
