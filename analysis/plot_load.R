source("tables.R")
source("plots.R")

plotdata <- table_load() %>%
    filter(sketch_bits == 0) %>%
    filter(algorithm %in% c("hu-et-al", "one-round-lsh")) %>%
    group_by(dataset, threshold, algorithm) %>%
    # Select the fastest configuration on each workload
    filter(total_time == min(total_time)) %>%
    ungroup() %>%
    filter(threshold == 0.5)

ggplot(plotdata, aes(y = Load, x = worker, fill = algorithm)) +
    geom_col(
        data = function(d) filter(d, algorithm == "hu-et-al"),
        show.legend = F
    ) +
    geom_col(
        data = function(d) filter(d, algorithm == "one-round-lsh"),
        mapping = aes(y = -Load),
        show.legend = F,
    ) +
    guides(color=F) +
    facet_wrap(vars(dataset), ncol=4) +
    theme_void()

ggsave("imgs/load.png", width = 8, height = 4)
