source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_full() %>%
    filter(threshold == 0.5) %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    select(dataset, algorithm, sketch_bits, k, k2, total_time, recall, Load)

plot_load <- function(data) {
    baseline <- data %>% filter(algorithm == "all-2-all")
    data %>%
    filter(algorithm != "all-2-all") %>%
    ggplot(aes(x = Load, y = total_time, color = algorithm)) +
        geom_hline(
            data = baseline,
            mapping = aes(yintercept = total_time)
        ) +
        geom_point() +
        geom_label_repel(aes(label = k),
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
