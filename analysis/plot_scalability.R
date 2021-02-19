source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_scalability() %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units())

ideal <- plotdata %>% 
    filter(workers == 8) %>%
    group_by(algorithm, dataset, workers) %>%
    summarise(total_time = mean(total_time)) %>%
    ungroup() %>%
    rowwise() %>%
    summarise(
        algorithm = algorithm,
        dataset = dataset,
        machines = 1:5,
        workers = machines * 8,
        total_time = total_time / machines
    )

ggplot(
    plotdata,
    aes(
        x = workers,
        y = total_time,
        color = algorithm
    )
) +
    geom_line(
        data = ideal,
        linetype = "dashed",
        size = 0.2
    ) +
    geom_line(stat="summary", fun.data = mean_se) +
    geom_linerange(stat="summary", fun.data = mean_cl_boot, position = position_dodge(1)) +
    geom_point(position=position_jitter(height=0, width=1), alpha=0.5, size=0.5) +
    facet_wrap(vars(dataset), ncol = 4, scales = "free_y") +
    scale_color_algorithm() +
    scale_x_continuous(breaks = c(1:5 * 8)) +
    labs(
        x = "number of workers",
        y = "total time (s)"
    ) +
    theme_paper()

ggsave("imgs/scalability.png", width = 8, height = 3)
