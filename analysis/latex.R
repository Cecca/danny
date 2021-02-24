source("tables.R")

latex_table_best <- function(data) {
    best_runs <- data %>%
        ungroup() %>%
        group_by(dataset, threshold) %>%
        slice_min(total_time) %>%
        pull(id)

    data %>%
        ungroup() %>%
        group_by(dataset, threshold) %>%
        mutate(
            dataset = str_remove(dataset, "-sample-*"),
            k = if_else(algorithm == "two-round-lsh",
                str_c(k, " [k2=", k2, "]"),
                as.character(k)
            ),
            total_time_num = drop_units(total_time),
            total_time = total_time %>%
                set_units("s") %>%
                drop_units() %>%
                scales::number(accuracy = 0.01),
            recall = scales::number(recall, accuracy = 0.01),
            total_time = cell_spec(total_time,
                # background = spec_color(total_time_num, direction = -1),
                # color = "white",
                # underline = total_time == min(total_time),
                underline = id %in% best_runs,
                format = "latex"
            )
        ) %>%
        ungroup() %>%
        select(dataset, threshold, algorithm, total_time, recall, k, sketch_bits) %>%
        pivot_wider(names_from = threshold, values_from = total_time:sketch_bits) %>%
        select(
            dataset, algorithm,
            total_time_0.5, recall_0.5, k_0.5, sketch_bits_0.5,
            total_time_0.7, recall_0.7, k_0.7, sketch_bits_0.7
        ) %>%
        kbl(
            format = "latex",
            align = "ll rrll rrll",
            escape = F,
            booktabs = T,
            linesep = c("", "", "", "\\addlinespace"),
            col.names = c(
                "dataset", "algorithm",
                "total time (s)", "recall", "k", "b",
                "total time (s)", "recall", "k", "b"
            )
        ) %>%
        add_header_above(c(" " = 2, "0.5" = 4, "0.7" = 4))
}

table_best() %>%
    latex_table_best() %>%
    write_file("tex/best.tex")

latex_table_info <- function(data) {
    tbl_data <- data %>%
        ungroup() %>%
        filter(threshold %in% c(0.5, 0.7)) %>%
        select(dataset, threshold, n, dim, output_size) %>%
        mutate(
            sample = as.integer(str_match(dataset, "sample-(\\d+)")[, 2]),
            sample = if_else(is.na(sample), "Full dataset", str_c("Sample of ", sample)),
            dataset = case_when(
                str_detect(dataset, "sift") ~ "SIFT",
                str_detect(dataset, "Livejournal") ~ "Livejournal",
                str_detect(dataset, "[Gg]love") ~ "Glove",
                str_detect(dataset, "Orkut") ~ "Orkut"
            ),
            # selectivity = scales::percent(selectivity, accuracy = 0.00001),
            avg_neighbors = scales::number(output_size / n, accuracy = 0.01, big.mark = "\\\\,")
        ) %>%
        select(-output_size) %>%
        pivot_wider(names_from = threshold, values_from = avg_neighbors) %>%
        arrange(sample)

    print(tbl_data)

    tbl_data %>%
        select(-sample) %>%
        mutate(
            n = scales::number(n, big.mark = "\\\\,", accuracy = 1),
            dim = scales::number(dim, big.mark = "\\\\,", accuracy = 1)
        ) %>%
        kbl(
            format = "latex",
            escape = F,
            booktabs = T,
            # linesep = c("", "", "", "\\addlinespace")
            linesep = ""
        ) %>%
        # kable_styling() %>%
        add_header_above(c(" " = 1, "  " = 1, "   " = 1, "average neighbors" = 2)) %>%
        pack_rows("Full dataset", 1, 4) %>%
        pack_rows("Sample of 200000 vectors", 5, 8)
}

table_data_info() %>%
    latex_table_info() %>%
    write_file("tex/info.tex")


latex_normalized_profile <- function(data) {
    data %>%
        select(-ends_with("input"), -sketch, -verify, -deduplicate) %>%
        pivot_longer(ends_with("ppf"), names_to = "component", values_to = "ppf") %>%
        mutate(
            component = str_remove(component, "_ppf"),
            component = if_else(component == "dedup", "deduplicate", component),
            component = factor(component,
                levels = c("sketch", "verify", "deduplicate"),
                ordered = T
            ),
            ppf = scales::number(ppf, big.mark = "\\\\,", scale=0.001, accuracy = 1),
            algorithm = factor(algorithm, ordered = T, levels = c(
                "LocalLSH",
                "OneLevelLSH",
                "TwoLevelLSH"
            ))
        ) %>%
        ungroup() %>%
        select(-id, -threshold) %>%
        pivot_wider(names_from=c(algorithm, component), values_from=ppf) %>%
        select(dataset, 
            LocalLSH_sketch, LocalLSH_verify, LocalLSH_deduplicate,
            OneLevelLSH_sketch, OneLevelLSH_verify, OneLevelLSH_deduplicate,
            TwoLevelLSH_sketch, TwoLevelLSH_verify, TwoLevelLSH_deduplicate
        ) %>%
        kbl(format = "latex", booktabs = T, escape = F,
            col.names = c(
                "dataset",
                "sketching", "verify", "dedup.",
                "sketching", "verify", "dedup.",
                "sketching", "verify", "dedup."
            )
        ) %>%
        add_header_above(c(" " = 1, "\\\\local" = 3, "\\\\onelevel" = 3, "\\\\twolevel" = 3), escape = F)
}

table_normalized_profile() %>%
    latex_normalized_profile() %>%
    write_file("tex/profiling.tex")

latex_bench <- function() {
    tbldata <- table_bench() %>%
        group_by(dataset, classification) %>%
        summarise(
            mean_sketch = mean(sketch) %>% scales::number(accuracy = 0.1),
            median_sketch = median(sketch) %>% scales::number(accuracy = 0.1),
            max_sketch = max(sketch) %>% scales::number(accuracy = 0.1),
            mean_dedup = mean(dedup) %>% scales::number(accuracy = 0.1),
            median_dedup = median(dedup) %>% scales::number(accuracy = 0.1),
            max_dedup = max(dedup) %>% scales::number(accuracy = 0.1),
            mean_verify = mean(verify) %>% scales::number(accuracy = 0.1),
            median_verify = median(verify) %>% scales::number(accuracy = 0.1),
            max_verify = max(verify) %>% scales::number(accuracy = 0.1)
        ) %>%
        ungroup()
    

    tbldata %>%
        select(dataset, classification, mean_sketch, median_sketch, max_sketch, mean_dedup, median_dedup, max_dedup, mean_verify, median_verify, max_verify) %>%
        kbl(format = "latex", escape = F, booktabs = TRUE,
            col.names = c(
                "data type", "pair type", 
                "mean", "median", "max",
                "mean", "median", "max",
                "mean", "median", "max"
            )
        ) %>%
        add_header_above(c(" " = 1, "  " = 1, "sketch" = 3, "deduplication" = 3, "similarity" = 3))
}

latex_bench() %>% write_file("tex/bench.tex")


