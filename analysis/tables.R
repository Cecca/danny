source("packages.R")

table_search_best <- function() {
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
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        collect() %>%
        inner_join(load) %>%
        (function(d) {print(distinct(d, path)); d}) %>%
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
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_best <- function() {
    table_search_best() %>%
        # filter(algorithm != "all-2-all") %>%
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
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_data_info <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    baseinfo <- tribble(
        ~dataset, ~n, ~dim,
        # Sampled datasets
        "Glove-sample-200000.bin", 199778, 200,
        "Livejournal-sample-200000.bin", 200035, 7489073,
        "Orkut-sample-200000.bin", 199588, 8730857,
        "sift-100nn-0.5-sample-200000.bin", 199786, 128,
        # Original datasets
        "glove.twitter.27B.200d.bin", 1193514, 200,
        "Orkut.bin", 2783196, 8730857,
        "Livejournal.bin", 3201203, 7489073,
        "sift-100-0.5.bin", 1000000, 128
    )

    info <- tbl(db, "result_recent") %>%
        filter(algorithm == "all-2-all", sketch_bits == 0) %>%
        collect() %>%
        mutate(dataset = basename(path)) %>%
        select(dataset, threshold, output_size) %>%
        inner_join(baseinfo) %>%
        mutate(selectivity = output_size / choose(n, 2))
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
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_profile <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    profile <- tbl(db, "profile") %>%
        # there is one type of frame which has twice the number of the total!
        filter(frame_count <= frame_total) %>%
        filter(
            (name %LIKE% "%_predicate") |
            (name %LIKE% "%already_seen") |
            (name %LIKE% "%different_bits")
        ) %>%
        collect() %>%
        mutate(
            name = case_when(
                str_detect(name, "_predicate") ~ "verify",
                str_detect(name, "already_seen") ~ "deduplicate",
                str_detect(name, "different_bits") ~ "sketch"
            )
        ) %>%
        pivot_wider(names_from=name, values_from=frame_count) %>%
        replace_na(list(
            deduplicate = 0,
            sketch = 0,
            verify = 0
        )) %>%
        mutate(other = frame_total - (deduplicate + sketch + verify))
    all <- tbl(db, "result_recent") %>%
        filter(hosts == "sss00:2001__sss01:2001__sss02:2001__sss03:2001__sss04:2001") %>%
        filter(profile_frequency == 998) %>%
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
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}

table_scalability <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    selector <- tbl(db, "result_recent") %>%
        filter(hosts == "sss00:2001") %>%
        select(path, algorithm, threshold, k, k2, sketch_bits)
    all <- tbl(db, "result_recent") %>%
        filter(profile_frequency == 0) %>%
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        filter(algorithm != "two-round-lsh" | (repetition_batch >= 1000)) %>%
        semi_join(selector) %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms"),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "Glove") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            nhosts = str_count(hosts, "sss"),
            workers = nhosts * threads
        ) %>%
        select(-total_time_ms)
    DBI::dbDisconnect(db)
    all
}