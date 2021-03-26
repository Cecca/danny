source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_full() %>%
    filter(threshold == 0.5) %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    select(dataset, algorithm, sketch_bits, k, k2, total_time, recall, Load)

plot_load <- function(data) {
    baseline <- data %>% filter(algorithm == "Cartesian")
    data %>%
    filter(algorithm != "Cartesian") %>%
    group_by(dataset, algorithm) %>%
    mutate(label = if_else(algorithm != "LocalLSH",
        k,
        if_else(total_time %in% c(max(total_time), min(total_time)),
            k,
            as.integer(NA)
        )
    )) %>%
    ungroup() %>%
    ggplot(aes(x = Load, y = total_time, color = algorithm)) +
        geom_hline(
            data = baseline,
            mapping = aes(yintercept = total_time)
        ) +
        geom_point() +
        geom_label_repel(
            aes(label = label),
            size = 3,
            label.padding = 0.1,
            show.legend = F
        ) +
        facet_wrap(vars(dataset), ncol = 4) +
        scale_y_log10() +
        scale_x_continuous(labels=scales::number_format(
            scale = 1/1000000,
        )) +
        labs(
            x = "Load (millions of messages)",
            y = "Total time (s)"
        ) +
        scale_color_algorithm() +
        theme_paper()
}

plot_load(plotdata)
ggsave("imgs/full.png", width = 8, height = 3)
