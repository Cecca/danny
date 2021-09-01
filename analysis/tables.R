source("packages.R")

baseinfo <- tribble(
    ~dataset, ~n, ~dim,
    # # Sampled datasets
    # "Glove-sample-200000.bin", 199778, 200,
    # "Livejournal-sample-200000.bin", 200035, 7489073,
    # "Orkut-sample-200000.bin", 199588, 8730857,
    # "sift-100nn-0.5-sample-200000.bin", 199786, 128,
    # Original datasets
    "glove.twitter.27B.200d.bin", 1193514, 200,
    "Orkut.bin", 2783196, 8730857,
    "Livejournal.bin", 3201203, 7489073,
    "sift-100-0.5.bin", 1000000, 128
) %>%
    mutate(n_pairs = choose(n, 2))


order_datasets <- function(data) {
    mutate(data, dataset = factor(dataset, levels = c("Glove", "SIFT", "Livejournal", "Orkut"), ordered = T))
}

recode_algorithms <- function(data) {
    mutate(data,
        algorithm = case_when(
            algorithm == "cartesian" ~ "Cartesian",
            algorithm == "local-lsh" ~ "LocalLSH",
            algorithm == "two-level-lsh" ~ "TwoLevelLSH",
            algorithm == "one-level-lsh" ~ "OneLevelLSH"
        )
    )
}

add_messages_size <- function(data) {
  data %>%
    mutate(
      sketch_bytes_per_element = sketch_bits / 8,
      id_bytes_per_element = 4, # 32 bits
      data_bytes_per_element = case_when(
        dataset == "SIFT" ~ 128 * 4,
        dataset == "Glove" ~ 200 * 4,
        dataset == "Livejournal" ~ 32 * 4, # average length 32
        dataset == "Orkut" ~ 117 * 4, # average length 117
        T ~ 0
      ),
      # sqrt(k) * 2 * sizeof(u16)
      pools_bytes_per_element = 4 * (ceiling(sqrt(k)) + ceiling(sqrt(k2))),
      bytes_per_element = 
        sketch_bytes_per_element +
        id_bytes_per_element +
        data_bytes_per_element +
        pools_bytes_per_element,
      load_bytes = bytes_per_element * Load,
      sketch_bytes_fraction = sketch_bytes_per_element / bytes_per_element,
      id_bytes_fraction = id_bytes_per_element / bytes_per_element,
      data_bytes_fraction = data_bytes_per_element / bytes_per_element,
      pools_bytes_fraction = pools_bytes_per_element / bytes_per_element
    )
}

table_sketch_quality <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    # The load is the maximum load among all the workers in a given experiment
    all <- tbl(db, "result_recent") %>%
        collect() %>%
        mutate(
            timed_out = is.na(output_size) # && !is.na(total_time_ms)
        ) %>%
        filter(hosts == "10.1.1.1:2001__10.1.1.2:2001__10.1.1.3:2001__10.1.1.4:2001__10.1.1.5:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(!str_detect(path, "sample-200000.bin")) %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        # filter(algorithm != "two-level-lsh" | (repetition_batch >= 1000)) %>%
        mutate(dataset = basename(path)) %>%
        inner_join(baseinfo) %>%
        mutate(
            dry_run = as.logical(dry_run),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            )
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        filter(algorithm == "Cartesian") %>%
        select(-total_time_ms)

    baseline <- filter(all, sketch_bits == 0) %>%
        select(dataset, threshold, base_output_size = output_size)

    all <- inner_join(all, baseline) %>%
        mutate(
            lost_pairs = base_output_size - output_size,
            lost_fraction = lost_pairs / output_size
        )


    DBI::dbDisconnect(db)
    all
}

table_search_best <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    # The load is the maximum load among all the workers in a given experiment
    load <- tbl(db, "counters") %>%
        filter(kind %in% c("Load")) %>%
        group_by(id, kind, worker) %>%
        # The computation of the rounds is overlapped, so we consider the total number of messages received by each worker
        summarise(count = sum(count)) %>%
        ungroup() %>%
        group_by(id, kind) %>%
        summarise(count = max(count, na.rm = T)) %>%
        collect() %>%
        pivot_wider(names_from = kind, values_from = count)
    candidates <- tbl(db, "counters") %>%
        filter(kind %in% c("CandidatePairs", "SelfPairsDiscarded")) %>%
        group_by(id, kind) %>%
        summarise(count = sum(count)) %>%
        collect() %>%
        pivot_wider(names_from = kind, values_from = count)
    all <- tbl(db, "result_recent") %>%
        collect() %>%
        mutate(
            timed_out = is.na(output_size) # && !is.na(total_time_ms)
        ) %>%
        filter(hosts == "10.1.1.1:2001__10.1.1.2:2001__10.1.1.3:2001__10.1.1.4:2001__10.1.1.5:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(!str_detect(path, "sample-200000.bin")) %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        # filter(algorithm != "two-level-lsh" | (repetition_batch >= 1000)) %>%
        left_join(load) %>%
        left_join(candidates) %>%
        mutate(dataset = basename(path)) %>%
        inner_join(baseinfo) %>%
        mutate(
            dry_run = as.logical(dry_run),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            fraction_candidates = (CandidatePairs - SelfPairsDiscarded) / n_pairs
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)

    baselines <- all %>%
        filter(algorithm == "Cartesian", sketch_bits == 0) %>%
        select(dataset, threshold, baseline_output_size = output_size)

    all <- inner_join(all, baselines) %>%
        mutate(recall = output_size / baseline_output_size) %>%
        add_messages_size()

    DBI::dbDisconnect(db)
    all
}

table_sketches <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    all <- tbl(db, "result_recent") %>%
        filter(!dry_run) %>%
        collect() %>%
        mutate(
            timed_out = is.na(output_size) # && !is.na(total_time_ms)
        ) %>%
        filter(hosts == "10.1.1.1:2001__10.1.1.2:2001__10.1.1.3:2001__10.1.1.4:2001__10.1.1.5:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(!str_detect(path, "sample-200000.bin")) %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        mutate(dataset = basename(path)) %>%
        inner_join(baseinfo) %>%
        mutate(
            dry_run = as.logical(dry_run),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            )
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        filter(case_when(
            # algorithm == "LocalLSH" ~ k %in% c(19, 20),
            TRUE ~ TRUE
        )) %>%
        select(-total_time_ms)

    baselines <- all %>%
        filter(algorithm == "Cartesian", sketch_bits == 0) %>%
        select(dataset, threshold, baseline_output_size = output_size)

    all <- inner_join(all, baselines) %>%
        mutate(recall = output_size / baseline_output_size)

    DBI::dbDisconnect(db)
    all
}



table_candidates <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    counters <- tbl(db, "counters") %>%
        filter(kind %in% c(
            "SelfPairsDiscarded",
            "SimilarityDiscarded",
            "CandidatePairs",
            "OutputPairs",
            "SketchDiscarded",
            "DuplicatesDiscarded"
        )) %>%
        group_by(id, worker, step, kind) %>%
        summarise(count = sum(count, na.rm = TRUE)) %>%
        ungroup() %>%
        collect() %>%
        pivot_wider(names_from = "kind", values_from = "count")
    all <- tbl(db, "result_recent") %>%
        filter(hosts == "10.1.1.1:2001__10.1.1.2:2001__10.1.1.3:2001__10.1.1.4:2001__10.1.1.5:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "Glove") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            )
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        inner_join(counters) %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_best <- function() {
    table_search_best() %>%
        # filter(algorithm != "all-2-all") %>%
        filter(!timed_out) %>%
        group_by(dataset, threshold, algorithm) %>%
        slice_min(total_time)
}

table_load <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    load <- tbl(db, "counters") %>%
        filter(kind == "Load") %>%
        group_by(id, worker) %>%
        summarise(Load = sum(count, na.rm = TRUE)) %>%
        ungroup() %>%
        collect()
    all <- tbl(db, "result_recent") %>%
        filter(hosts == "sss00:2001__sss01:2001__sss02:2001__sss03:2001__sss04:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        collect() %>%
        inner_join(load) %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "Glove") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            )
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_full <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    # The load is the maximum load among all the workers in a given experiment
    load <- tbl(db, "counters") %>%
        filter(kind == "Load") %>%
        group_by(id, kind) %>%
        summarise(count = max(count, na.rm = T)) %>%
        collect() %>%
        pivot_wider(names_from = kind, values_from = count)
    all <- tbl(db, "result_recent") %>%
        filter(hosts == "sss00:2001__sss01:2001__sss02:2001__sss03:2001__sss04:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(
            (path %LIKE% "%glove.twitter.27B.200d.bin") |
                (path %LIKE% "%Orkut.bin") |
                (path %LIKE% "%Livejournal.bin") |
                (path %LIKE% "%sift-100-0.5.bin")
        ) %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        collect() %>%
        inner_join(load) %>%
        mutate(dataset = basename(path)) %>%
        mutate(
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut",
                TRUE ~ dataset
            )
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_data_info <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    info <- tbl(db, "result_recent") %>%
        filter(algorithm == "cartesian", sketch_bits == 0) %>%
        collect() %>%
        mutate(dataset = basename(path)) %>%
        select(dataset, threshold, output_size) %>%
        inner_join(baseinfo) %>%
        # order_datasets() %>%
        mutate(
            n_pairs = choose(n, 2),
            selectivity = output_size / choose(n, 2)
        )
    DBI::dbDisconnect(db)
    info
}

table_recall_experiment <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    all <- tbl(db, "result_recent") %>%
        filter(hosts == "sss00:2001__sss01:2001__sss02:2001__sss03:2001__sss04:2001") %>%
        filter(profile_frequency == 0) %>%
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall %in% c(0.5, 0.8, 0.9)) %>%
        filter(threshold %in% c(0.5)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        filter(algorithm != "all-2-all") %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "Glove") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            )
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_profile <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    profile <- tbl(db, "profile") %>%
        filter(thread %like% "%worker%") %>%
        collect() %>%
        mutate(
            top_level = str_split(name, "->") %>% sapply(magrittr::extract2, 1),
            name = case_when(
                str_detect(name, "_predicate") ~ "verify",
                str_detect(name, "already_seen") ~ "deduplicate",
                str_detect(name, "different_bits") ~ "sketch",
                str_detect(name, "hashbrown") ~ "hashmap",
                str_detect(top_level, "timely::progress") ~ "timely progress (self)",
                str_detect(top_level, "pthread_mutex") ~ "mutex lock/unlock (self)",
                str_detect(top_level, "(Self)?Joiner") ~ "local join (self)",
                str_detect(top_level, "timely_communication") ~ "communication (self)",
                str_detect(name, "Vec.*spec_extend") ~ "extend vector",
                TRUE ~ "other"
            )
        ) %>%
        # filter(name != "timely progress") %>%
        group_by(id, hostname, thread, name) %>%
        summarise(frame_count = sum(frame_count)) %>%
        ungroup() %>%
        arrange(id, hostname, thread, desc(frame_count))
    all <- tbl(db, "result_recent") %>%
        filter(hosts == "sss00:2001__sss01:2001__sss02:2001__sss03:2001__sss04:2001") %>%
        filter(profile_frequency == 9998) %>%
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        filter(algorithm != "all-2-all") %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "Glove") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            )
        ) %>%
        inner_join(profile) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_normalized_profile <- function() {
    profile <- table_profile() %>%
        filter(name %in% c("verify", "deduplicate", "sketch")) %>%
        group_by(id, dataset, algorithm, threshold, name) %>%
        summarise(frame_count = sum(frame_count)) %>%
        ungroup() %>%
        order_datasets() %>%
        pivot_wider(names_from = name, values_from = frame_count)

    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    tbl(db, sql("select * from counters where id in (select id from result_recent where profile_frequency > 0)")) %>%
        group_by(id, kind) %>%
        summarise(count = sum(count, na.rm = TRUE)) %>%
        collect() %>%
        pivot_wider(names_from = kind, values_from = count) %>%
        transmute(
            sketch_input = CandidatePairs,
            verify_input = sketch_input - SketchDiscarded,
            dedup_input = verify_input - SimilarityDiscarded
        ) %>%
        inner_join(profile) %>%
        # recode_algorithms() %>%
        mutate(
            # compute the PairsPerFrame
            sketch_ppf = sketch_input / sketch,
            verify_ppf = verify_input / verify,
            dedup_ppf = dedup_input / deduplicate
        )
}

table_scalability <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results-scalability.sqlite")
    all <- tbl(db, "result_recent") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.7)) %>%
        collect()


    selected <- all %>% # semi_join(all, fast_params) %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            nhosts = str_count(hosts, ":"),
            workers = nhosts * threads
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    selected
}

table_bench <- function() {
    read_csv("bench.csv.gz") %>%
        mutate(
            dataset = case_when(
                str_detect(path, "Livejournal") ~ "Jaccard",
                str_detect(path, "glove") ~ "Cosine"
            )
        )
}

table_sysmonitor <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results-scalability.sqlite")
    system <- tbl(db, "system") %>% collect()
    result <- tbl(db, "result_recent") %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            nhosts = str_count(hosts, ":"),
            workers = nhosts * threads
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        filter(workers == 40, dataset=="Glove") %>% 
        group_by(dataset, algorithm) %>%
        slice_min(total_time_ms)
    inner_join(result, system)
}

table_datastructures_bytes <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results-scalability.sqlite")
    system <- tbl(db, "datastructures_bytes") %>% collect()
    result <- tbl(db, "result_recent") %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            nhosts = str_count(hosts, ":"),
            workers = nhosts * threads,
            coll_prob_half_k = (1.0 - acos(threshold) / pi)^(k/2),
            # the number of repetitions executed by the outer layer of LSH (which is the only
            # one for LocalLSH and OneLevelLSH)
            outer_repetitions = log((1.0 - sqrt(required_recall)))^2 / log(1.0 - coll_prob_half_k)^2,
            outer_repetitions = as.integer(ceiling(outer_repetitions))
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        filter(dataset=="Glove") %>% 
        group_by(dataset, algorithm, workers) %>%
        # slice_min(total_time_ms) %>% 
        ungroup()
    res <- inner_join(result, system) %>%
        mutate(
            datastructures_mb = as.double(datastructures_bytes) / (1024*1024),
            mb_per_repetition = datastructures_mb / outer_repetitions,
            mb_per_worker = datastructures_mb / threads,
            mb_per_repetition_per_worker = mb_per_repetition / threads
        )
    DBI::dbDisconnect(db)
    res
}

table_scalability_load <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results-scalability.sqlite")
    all <- tbl(db, "result_recent") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.7)) %>%
        collect()
    load <- tbl(db, "counters") %>%
        filter(kind %in% c("Load")) %>%
        group_by(id, worker) %>%
        # The computation of the rounds is overlapped, so we consider the total number of messages received by each worker
        summarise(load = sum(count, na.rm=TRUE)) %>%
        ungroup() %>%
        group_by(id) %>% 
        summarise(
            max_load = max(load),
            min_load = min(load)
        ) %>%
        collect()
    net <- tbl(db, "network") %>%
        group_by(id) %>% 
        summarise(
            net_tx = sum(transmitted) / (1024*1024),
            net_rx = sum(received) / (1024*1024),
        ) %>%
        collect()

    selected <- all %>% # semi_join(all, fast_params) %>%
        inner_join(load) %>%
        inner_join(net) %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            nhosts = str_count(hosts, ":"),
            workers = nhosts * threads
        ) %>%
        order_datasets() %>%
        recode_algorithms() %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    selected
}


table_scalability_load() %>%
    filter(algorithm == "LocalLSH", dataset=="Glove") %>%
    filter(workers %in% c(40, 72)) %>%
    filter(k == 12) %>%
    group_by(workers) %>%
    summarise(
        total_time = mean(total_time),
        max_load = mean(max_load),
        min_load = mean(min_load),
        net_tx_mb = mean(net_tx),
        net_rx_mb = mean(net_rx)
    )
