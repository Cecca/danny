source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_scalability() %>%
    filter(threshold == 0.5) %>%
    filter(sketch_bits == 256, k2 %in% c(0, 6)) %>%
    group_by(algorithm, dataset, workers, k, k2, sketch_bits) %>%
    summarise(total_time = mean(total_time)) %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    ungroup() %>%
    select(algorithm, dataset, workers, k, k2, sketch_bits, total_time)

fast_params <- plotdata %>%
    group_by(algorithm, dataset, workers) %>%
    slice_min(total_time) %>%
    ungroup() %>%
    select(algorithm, dataset, workers, k, k2, sketch_bits)

best_40_params <- plotdata %>%
    filter(workers == 40) %>%
    group_by(algorithm, dataset, workers) %>%
    slice_min(total_time) %>%
    ungroup() %>%
    select(algorithm, dataset, k, k2, sketch_bits)

with_best_40 <- semi_join(plotdata, best_40_params) %>%
    mutate(conf = "best40")

fast_runs <- semi_join(plotdata, fast_params) %>%
    mutate(conf = "bestWorker")

plotdata <- bind_rows(
    with_best_40,
    fast_runs
) %>%
select(-k, -k2, -sketch_bits) %>%
pivot_wider(names_from=conf, values_from=total_time)

ggplot(
    plotdata,
    aes(
        x = workers,
        ymin = bestWorker,
        ymax = best40,
        color = algorithm
    )
) +
    geom_linerange(
        position=position_dodge(2),
        size = 1
    ) +
    geom_point(
        mapping = aes(y = bestWorker),
        data=~filter(.x, bestWorker == best40),
        shape=18,
        position = position_dodge(2),
        size = 3
    ) +
    facet_wrap(vars(dataset), scales = "free", ncol = 4) +
    scale_shape_manual(values = c(2, 6)) +
    scale_color_algorithm() +
    scale_x_continuous(breaks = c(1:5 * 8)) +
    labs(
        x = "number of workers",
        y = "total time (s)"
    ) +
    theme_paper()

ggsave("imgs/scalability.png", width = 8, height = 3)
