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
use danny::operators::Route;
use danny_base::measure::*;
use danny_base::types::*;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::*;
use timely::dataflow::*;

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
                        if let Some(accum) = accum.remove(&time) {
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
    num_groups: u32,
) -> Stream<G, (PrefixToken, (u64, BagOfWords))> {
    let worker = stream.scope().index();
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
                        if worker == 0 {
                            info!("Routing vectors by prefix group");
                        }
                        let mut bows = stash_input.remove(&t).expect("there sholud be this time");
                        for (c, mut bow) in bows.drain(..) {
                            bow.remap_tokens(&ranks);
                            let prefix = bow.words().iter().take(prefix_len(&bow, range));
                            let mut already_sent = HashSet::new();
                            for &token in prefix {
                                let token_group = token % num_groups;
                                if !already_sent.contains(&token_group) {
                                    session.give((token_group, (c, bow.clone())));
                                    already_sent.insert(token_group);
                                }
                            }
                        }
                    },
                );
            }
        },
    )
}

type PrefixCandidate = (PrefixToken, (u64, BagOfWords));

struct InvertedIndexRecord<'a> {
    id: u64,
    bow: &'a BagOfWords,
    token_pos: usize,
}

fn build_inverted_index<'a>(
    bows: &'a [(u64, BagOfWords)],
    range: f64,
) -> HashMap<u32, Vec<InvertedIndexRecord<'a>>> {
    let mut output = HashMap::new();
    for (id, bow) in bows {
        let p = prefix_len(bow, range);
        for (token_pos, token) in bow.words().iter().take(p).enumerate() {
            output
                .entry(*token)
                .or_insert_with(Vec::new)
                .push(InvertedIndexRecord {
                    id: *id,
                    bow,
                    token_pos,
                })
        }
    }
    output
}

#[inline]
fn intersection(l: &[u32], r: &[u32]) -> usize {
    let mut intersection = 0;

    let mut l_iter = l.iter();
    let mut r_iter = r.iter();
    let mut l_token = l_iter.next();
    let mut r_token = r_iter.next();

    loop {
        if l_token.is_none() || r_token.is_none() {
            break;
        }
        let l_t = l_token.unwrap();
        let r_t = r_token.unwrap();
        if l_t < r_t {
            l_token = l_iter.next();
        } else if l_t > r_t {
            r_token = r_iter.next();
        } else {
            intersection += 1;
            l_token = l_iter.next();
            r_token = r_iter.next();
        }
    }

    intersection
}

#[inline]
fn equivalent_overlap(r: &BagOfWords, s: &BagOfWords, sim: f64) -> f64 {
    let t_unrounded = sim * (r.len() + s.len()) as f64 / (1.0 + sim);
    let t_rounded = t_unrounded.round();
    // The rounding below with the comparison with EPS is needed to counter the
    // floating point errors introduced by the division
    let t = if (t_rounded - t_unrounded).abs() < 0.000_000_000_000_01 {
        t_rounded
    } else {
        t_unrounded
    };
    t.ceil()
}

fn verify<A>(
    l: u64,
    l_bow: &BagOfWords,
    overlap_map: &HashMap<u64, usize>,
    right: &[(u64, BagOfWords)],
    range: f64,
    action: &mut A,
) where
    A: FnMut(u64, u64),
{
    let pl = prefix_len(l_bow, range);
    for (r, r_bow) in right {
        // FIXME: This computation is wrong in some cases: BagOfWords { universe: 10000, words: [9949] } <-> BagOfWords { universe: 10000, words: [9949, 9991] }
        // This is an f64 rather than usize because otherwise we accept some cases like:
        //    BagOfWords { universe: 10000, words: [9949] } <-> BagOfWords { universe: 10000, words: [9949, 9991] }
        let target_overlap = equivalent_overlap(l_bow, r_bow, range);
        // let target_overlap =
        //     ((range / (1.0 + range)) * ((l_bow.len() + r_bow.len()) as f64).ceil()) as usize;
        if let Some(mut overlap) = overlap_map.get(r).cloned() {
            // if BagOfWords::jaccard_predicate(l_bow, r_bow, range) {
            //     action(l, *r);
            // }

            let pr = prefix_len(r_bow, range);
            let last_token_l = l_bow.words()[pl - 1];
            let last_token_r = r_bow.words()[pr - 1];
            let (upper_bound, start_l, start_r) = if last_token_l < last_token_r {
                (overlap + l_bow.len() - pl, pl, overlap)
            } else {
                (overlap + r_bow.len() - pr, overlap, pr)
            };
            let mut inter = 0;
            if upper_bound as f64 >= target_overlap
                && start_l <= l_bow.len()
                && start_r <= r_bow.len()
            {
                inter = intersection(&l_bow.words()[start_l..], &r_bow.words()[start_r..]);
                overlap += inter;
            }

            if overlap as f64 >= target_overlap {
                debug_assert!(
                    BagOfWords::jaccard_predicate(l_bow, r_bow, range),
                    "false positive \n{:?}\n{:?} \n(target {} pl {} pr {} overlap {} jaccard sim {})",
                    l_bow,
                    r_bow,
                    target_overlap,
                    pl,
                    pr,
                    overlap,
                    BagOfWords::jaccard(l_bow, r_bow)
                );
                action(l, *r);
            } else {
                debug_assert!(
                    !BagOfWords::jaccard_predicate(l_bow, r_bow, range),
                    "false negative \n{:?}\n{:?} \n(target {} upper bound {} intersection of suffxs {} pl {} pr {} overlap {} jaccard sim {})",
                    l_bow,
                    r_bow,
                    target_overlap,
                    upper_bound,
                    inter,
                    pl,
                    pr,
                    overlap,
                    BagOfWords::jaccard(l_bow, r_bow)
                );
            }
        }
    }
}

fn ppjoin<A>(left: &[(u64, BagOfWords)], right: &[(u64, BagOfWords)], range: f64, mut action: A)
where
    A: FnMut(u64, u64),
{
    let empty_vec = Vec::new();
    let inverted_index = build_inverted_index(right, range);
    let mut progress_logger = progress_logger::ProgressLogger::builder()
        .with_frequency(std::time::Duration::from_secs(60))
        .with_items_name("'left sets'")
        .with_expected_updates(left.len() as u64)
        .start();
    let mut overlap_map: HashMap<u64, usize> = HashMap::new();
    for (l, l_bow) in left {
        overlap_map.clear();
        for (token_pos, token) in l_bow
            .words()
            .iter()
            .take(prefix_len(l_bow, range))
            .enumerate()
        {
            for record in inverted_index.get(token).unwrap_or(&empty_vec) {
                if record.bow.len() >= (range * l_bow.len() as f64) as usize {
                    let target_overlap = ((range / (1.0 + range))
                        * ((l_bow.len() + record.bow.len()) as f64).ceil())
                        as usize;
                    let upper_bound = 1 + std::cmp::min(
                        l_bow.len() - token_pos,
                        record.bow.len() - record.token_pos,
                    );
                    let overlap_entry = overlap_map.entry(record.id).or_insert(0);
                    if *overlap_entry + upper_bound >= target_overlap {
                        *overlap_entry += 1;
                    } else {
                        overlap_map.remove(&record.id);
                    }
                }
            }
        }
        verify(*l, l_bow, &overlap_map, right, range, &mut action);
        progress_logger.update_light(1u64);
    }
    progress_logger.stop();
}

fn filter_candidates<G: Scope>(
    stream_left: &Stream<G, PrefixCandidate>,
    stream_right: &Stream<G, PrefixCandidate>,
    range: f64,
) -> Stream<G, (u64, u64)> {
    let worker = stream_left.scope().index();
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
                    if worker == 0 {
                        info!("Verifying candidate pairs");
                    }

                    for (_group, (mut left, mut right)) in collisions.drain() {
                        left.sort_by_key(|pair| pair.1.len());
                        right.sort_by_key(|pair| pair.1.len());
                        ppjoin(&left, &right, range, |l, r| session.give((l, r)));
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

fn run(left_path: PathBuf, right_path: PathBuf, range: f64, num_groups: u32, config: Config) {
    let mut experiment = Experiment::from_env(&config)
        .tag("algorithm", "Vernica_join")
        .tag("num_groups", num_groups)
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
                let by_tokens_left = by_prefix_token(&stream_left, &ranks, range, num_groups);
                let by_tokens_right = by_prefix_token(&stream_right, &ranks, range, num_groups);
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
            row!("timed_out" => false, "output_size" => matching_count, "total_time_ms" => total_time, "recall" => recall, "speedup" => speedup),
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
        (@arg NUM_GROUPS: -g +takes_value +required "The number of token groups used")
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
        .expect("Problem parsing the range");
    let num_groups: u32 = matches
        .value_of("NUM_GROUPS")
        .unwrap()
        .parse::<u32>()
        .expect("Problem parsing the number of groups");

    let config = Config::get();
    init_logging(&config);
    if let Some(timeout) = config.get_timeout() {
        let mut timed_out_experiment = Experiment::from_env(&config)
            .tag("algorithm", "Vernica_join")
            .tag("num_groups", num_groups)
            .tag("left", input_left.to_str().unwrap())
            .tag("right", input_right.to_str().unwrap())
            .tag("threshold", range);
        timed_out_experiment.append(
            "result",
            row!("timed_out" => true, "total_time_ms" => timeout.as_millis() as u64),
        );

        start_terminator(timeout, move || {
            timed_out_experiment.save();
        });
    }

    info!("Starting");
    run(input_left, input_right, range, num_groups, config);

    info!("Done!");
}

/// Starts a thread that kills the entire application
/// if the program runs for too long
fn start_terminator<F>(timeout: Duration, callback: F)
where
    F: FnOnce() + Sync + Send + 'static,
{
    info!("Starting killer ({:?})", timeout);
    std::thread::spawn(move || {
        let start = Instant::now();
        let sleep_time = Duration::from_secs(10);
        loop {
            std::thread::sleep(sleep_time);
            if start.elapsed() > timeout {
                callback();
                warn!("Killing the application!!");
                std::process::exit(1);
            }
        }
    });
}
