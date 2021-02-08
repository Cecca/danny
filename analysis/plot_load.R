source("tables.R")
source("plots.R")

plotdata <- table_load() %>%
    filter(sketch_bits == 0) %>%
    group_by(dataset, threshold, algorithm) %>%
    # Select the fastest configuration on each workload
    filter(total_time == min(total_time)) %>%
    ungroup() %>%
    filter(threshold == 0.5)

ggplot(plotdata, aes(y = Load, x = worker, fill = algorithm)) +
    geom_col() +
    facet_grid(vars(algorithm), vars(dataset), switch = "y") +
    theme_paper() +
    theme(
        axis.text.y = element_blank(),
        axis.title.y = element_blank(),
        axis.ticks.y = element_blank(),
        strip.text.y = element_text(angle = 180)
    )

ggsave("imgs/load.png", width = 8, height = 4)