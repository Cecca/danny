source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_recall_experiment()

maxmin <- plotdata %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    filter(threshold == 0.5) %>%
    filter(sketch_bits == 0) %>%
    group_by(dataset, algorithm, required_recall) %>%
    slice_min(total_time) %>%
    ungroup() %>%
    group_by(dataset, required_recall) %>%
    summarise(
        ymin = min(total_time),
        ymax = max(total_time)
    ) %>%
    ungroup()

plotdata %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    filter(threshold == 0.5) %>%
    filter(sketch_bits == 0) %>%
    group_by(dataset, algorithm, threshold, required_recall) %>%
    slice_min(total_time) %>%
    ggplot(aes(
        xend = recall,
        x = required_recall,
        y = total_time,
        yend = total_time,
        color = algorithm
    )) +
    geom_linerange(
        data = maxmin,
        mapping = aes(
            x = required_recall,
            ymin = ymin,
            ymax = ymax,
            group = factor(required_recall)
        ),
        color = "gray40",
        inherit.aes = F
    ) +
    geom_link(aes(size = stat(index)), show.legend = F) +
    geom_point(aes(x = recall)) +
    scale_size_continuous(range = c(0.1, 1.2)) +
    scale_y_log10() +
    facet_wrap(vars(dataset), ncol = 4) +
    labs(
        x = "recall",
        y = "time (s)"
    ) +
    theme_paper()

ggsave("imgs/recall_experiment.png",
    width = 8,
    height = 3
)
