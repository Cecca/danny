source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_scalability() %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units())

ggplot(
    plotdata,
    aes(
        x = workers,
        y = total_time,
        color = algorithm
    )
) +
    geom_segment(
        aes(xend = 40, yend = stat(y) / 5),
        data = ~filter(.x, workers == 8),
        linetype = "dashed",
        stat = "summary",
        size = 0.2
    ) +
    geom_line(stat="summary") +
    geom_point(stat="summary") +
    facet_wrap(vars(dataset), ncol = 4, scales = "free_y") +
    scale_color_algorithm() +
    scale_x_continuous(breaks = c(1:5 * 8)) +
    labs(
        x = "number of workers",
        y = "total time (s)"
    ) +
    theme_paper()

ggsave("imgs/scalability.png", width = 8, height = 4)