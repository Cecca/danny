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

table_best <- function() {
    table_search_best() %>%
        filter(algorithm != "all-2-all") %>%
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