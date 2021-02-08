source("tables.R")

latex_table_best <- function(data) {
    data %>%
        ungroup() %>%
        mutate(
            dataset = str_remove(dataset, "-sample-*"),
            k = if_else(algorithm == "two-round-lsh",
                str_c(k, " [k2=", k2, "]"),
                as.character(k)
            ),
            total_time = total_time %>%
                set_units("s") %>%
                drop_units() %>%
                scales::number(accuracy = 0.01),
            recall = scales::number(recall, accuracy = 0.01)
            # total_time = cell_spec(total_time,
            #     color = spec_color(total_time)
            # )
        ) %>%
        select(dataset, threshold, algorithm, total_time, recall, k, sketch_bits) %>%
        pivot_wider(names_from = threshold, values_from = total_time:sketch_bits) %>%
        select(
            dataset, algorithm,
            total_time_0.5, recall_0.5, k_0.5, sketch_bits_0.5,
            total_time_0.7, recall_0.7, k_0.7, sketch_bits_0.7
        ) %>%
        kbl(
            format = "latex",
            booktabs = T,
            linesep = "",
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