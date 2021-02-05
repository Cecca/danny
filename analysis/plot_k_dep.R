source("tables.R")
source("plots.R")

plotdata <- table_search_best() %>%
    # we focus just on experiments with no sketches, to see the effect of k
    filter(sketch_bits == 0) %>%
    filter(between(k, 4, 12)) %>%
    mutate(
        total_time = set_units(total_time, "s") %>% drop_units(),
        dataset = case_when(
            str_detect(dataset, "sift") ~ "SIFT",
            str_detect(dataset, "Livejournal") ~ "Livejournal",
            str_detect(dataset, "Glove") ~ "Glove",
            str_detect(dataset, "Orkut") ~ "Orkut"
        )
    ) %>%
    group_by(dataset, algorithm, threshold) %>%
    mutate(
        ismin = total_time == min(total_time)
    )

plot_threshold <- function(data, t, ylabs = TRUE) {
    data <- filter(data, threshold == t)
    p_time <- ggplot(
        data,
        aes(k, total_time,
            color = algorithm,
            group = interaction(algorithm, k2)
        )
    ) +
        geom_hline(
            data = function(d) {
                filter(d, algorithm == "all-2-all", sketch_bits == 0)
            },
            mapping = aes(yintercept = total_time)
        ) +
        geom_point(aes(shape = ismin, size = ismin)) +
        geom_line(aes(linetype = factor(k2))) +
        scale_y_log10(labels = scales::number_format()) +
        scale_shape_manual(values = c(3, 18)) +
        scale_size_manual(values = c(2, 3)) +
        scale_color_algorithm() +
        facet_wrap(vars(dataset), ncol = 4) +
        guides(shape = FALSE, linetype = FALSE, size = FALSE) +
        theme_paper() +
        labs(
            title = str_c("Threshold ", t),
            y = "Total time (s)"
        ) +
        theme(
            axis.text.x = element_blank(),
            axis.title.x = element_blank(),
            strip.background = element_blank(),
            strip.text = element_text(hjust = 0)
        )

    p_load <- ggplot(
        data,
        aes(k, Load, color = algorithm, group = interaction(algorithm, k2))
    ) +
        geom_point(aes(shape = ismin, size = ismin)) +
        geom_line(aes(linetype = factor(k2))) +
        scale_y_log10(
            labels = scales::number_format(
                scale = 1 / 1000000,
                accuracy = 1
            ),
        ) +
        scale_shape_manual(values = c(3, 18)) +
        scale_size_manual(values = c(2, 3)) +
        scale_color_algorithm() +
        facet_wrap(vars(dataset), ncol = 4) +
        guides(shape = FALSE, linetype = FALSE, size = FALSE) +
        labs(
            y = "Load (millions)"
        ) +
        theme_paper() +
        theme(
            strip.text = element_blank(),
            strip.background = element_blank()
        )

    if (!ylabs) {
        p_time <- p_time + theme(
            axis.text.y = element_blank(),
            axis.title.y = element_blank()
        )
        p_load <- p_load + theme(
            axis.text.y = element_blank(),
            axis.title.y = element_blank()
        )
    }

    (p_time / p_load)
}

(plot_threshold(plotdata, 0.5) |
    plot_threshold(plotdata, 0.7, ylabs = FALSE)) /
    guide_area() +
    plot_layout(
        guides = "collect",
        heights = c(5, 1)
    )

ggsave("imgs/dep_k.png", width = 8, height = 4)