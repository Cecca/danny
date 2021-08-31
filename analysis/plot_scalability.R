source("packages.R")
source("tables.R")
source("plots.R")

plotdata <- table_scalability() %>%
    filter(dataset == "Glove") %>%
    group_by(algorithm, dataset, workers, k, k2, sketch_bits) %>%
    summarise(total_time = mean(total_time)) %>%
    mutate(total_time = set_units(total_time, "s") %>% drop_units()) %>%
    ungroup() %>%
    select(algorithm, dataset, workers, k, k2, sketch_bits, total_time) %>% 
    group_by(algorithm, dataset, workers) %>%
    slice_min(total_time) %>% 
    ungroup()

labels <- plotdata %>% 
    arrange(algorithm, k) %>%
    group_by(algorithm) %>%
    filter((k > lag(k)) | (workers == 8)) %>%
    ungroup()  %>% 
    mutate(
        y = case_when(
            (algorithm == "LocalLSH") & workers == 8 ~ total_time - 100,
            (algorithm == "LocalLSH") & workers == 40 ~ total_time + 30,
            (algorithm == "OneLevelLSH") & workers == 8 ~ total_time + 1500,
            (algorithm == "TwoLevelLSH") & workers == 8 ~ total_time + 200,
            (algorithm == "OneLevelLSH") & workers == 16 ~ total_time + 800,
            (algorithm == "TwoLevelLSH") & workers == 16 ~ total_time - 350,
            (algorithm == "OneLevelLSH") & workers == 40 ~ total_time + 200,
            (algorithm == "TwoLevelLSH") & workers == 40 ~ total_time + 20,
            # (algorithm == "LocalLSH") & workers == 40 ~ total_time + 30,
            T ~ total_time
        )
    )

p <- ggplot(
    plotdata,
    aes(
        x = workers,
        y = total_time,
        color = algorithm,
        shape = algorithm
    )
) +
    geom_line() +
    geom_point() +
    geom_text(
        aes(label=scales::number(total_time, suffix=" s")),
        data=function(d) {filter(d, workers == 8)},
        vjust=0.5,
        hjust=1,
        nudge_x=-1,
        size=2.5,
        show.legend=F,
        color = "black"
    ) +
    geom_text(
        aes(label=scales::number(total_time, suffix=" s")),
        data=function(d) {filter(d, workers == 72)},
        # data=function(d) {filter(d, workers == 72, !((dataset == "Glove") & (algorithm=="OneLevelLSH")))},
        vjust=0.5,
        hjust=0,
        nudge_x=1,
        size=2.5,
        show.legend=F,
        color = "black"
    ) +
    geom_text(
        aes(label=scales::number(total_time, suffix=" s")),
        data=function(d) {filter(d, workers == 40)},
        # data=function(d) {filter(d, workers == 40, !((dataset == "Glove") & (algorithm=="OneLevelLSH")))},
        vjust=0.5,
        hjust=1,
        nudge_y=-0.05,
        nudge_x=-1,
        size=2.5,
        show.legend=F,
        color = "black"
    ) +
    geom_label(
        aes(label=k, y=y), 
        data = labels,
        size=2, 
        show.legend=F
    ) +
    # facet_wrap(vars(dataset), scales = "free", ncol = 2) +
    scale_color_algorithm() +
    scale_x_continuous(
        breaks = c(1:9 * 8),
        expand=expansion(mult=c(0.15,0.15))
    ) +
    scale_y_log10() +
    # scale_y_continuous(
    #     breaks = c(272, 1412, 3351)
    # ) +
    labs(
        x = "number of workers",
        y = "total time (s)"
    ) +
    theme_paper() +
    theme(
        panel.grid.minor = element_blank(),
        panel.grid.major = element_blank(),
        legend.position = c(0.8, 0.75),
        legend.title = element_blank(),
        panel.border = element_blank(),
        axis.line = element_line(size = 0.3, colour = "black")
    )

ggsave("imgs/scalability.png", width = 4, height = 2.2)
