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
            linesep = c("", "", "", "\\addlinesep"),
            col.names = c(
                "dataset", "algorithm",
                "total time (s)", "recall", "k", "b",
                "total time (s)", "recall", "k", "b"
            )
        ) %>%
        kable_styling() %>%
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
        kable_styling() %>%
        add_header_above(c(" " = 1, "  " = 1, "   " = 1, "average neighbors" = 2)) %>%
        pack_rows("Full dataset", 1, 4) %>%
        pack_rows("Sample of 200000 vectors", 5, 8)
}

table_data_info() %>%
    latex_table_info() %>%
    write_file("tex/info.tex")
