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

table_search_best() %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    mutate(dataset = case_when(
        str_detect(dataset, "sift") ~ "SIFT",
        str_detect(dataset, "Livejournal") ~ "Livejournal",
        str_detect(dataset, "Glove") ~ "Glove",
        str_detect(dataset, "Orkut") ~ "Orkut"
    )) %>%
    ggplot(aes(recall, total_time, color = algorithm)) +
    geom_point(data = function(d) {
        filter(d, algorithm != "all-2-all")
    }) +
    geom_hline(
        data = function(d) {
            filter(d, algorithm == "all-2-all")
        },
        mapping = aes(yintercept = total_time),
        color = "black"
    ) +
    scale_y_log10() +
    facet_grid(vars(dataset), vars(threshold))
ggsave("best.png")