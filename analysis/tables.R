source("packages.R")

table_search_best <- function() {
    db <- DBI::dbConnect(RSQLite::SQLite(), "danny-results.sqlite")
    all <- tbl(db, "result_recent") %>%
        filter(path %LIKE% "%sample-200000.bin") %>%
        filter(required_recall == 0.8) %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        filter(!no_verify, !no_dedup) %>%
        collect() %>%
        mutate(
            dataset = basename(path),
            total_time = set_units(total_time_ms, "ms")
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

table_best()