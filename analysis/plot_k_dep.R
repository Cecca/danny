source("tables.R")
source("plots.R")

plotdata <- table_search_best() %>%
    # we focus just on experiments with no sketches, to see the effect of k
    filter(sketch_bits == 0) %>%
    filter((k == 0) || between(k, 4, 16)) %>%
    filter(k2 %in% c(0,6)) %>%
    mutate(
        total_time = set_units(total_time, "s") %>% drop_units()
    ) %>%
    group_by(dataset, algorithm, threshold) %>%
    mutate(
        ismin = total_time == min(total_time)
    )

plot_threshold <- function(data, t, ytitle = TRUE, yticks = TRUE, title = TRUE) {
    baseline <- filter(data, algorithm == "Cartesian") %>%
        ungroup() %>%
        select(dataset, threshold, base_time = total_time)
    data <- filter(data, algorithm != "Cartesian") %>%
        ungroup() %>%
        inner_join(baseline) %>%
        filter(total_time < 2 * base_time)

    # compute the limits before filtering, so that all plots share the same scale
    # limits_time <- pull(data, total_time) %>% range()
    # limits_load <- pull(data, Load) %>% range()

    data <- filter(data, threshold == t) 
    baseline <- filter(baseline, threshold == t)

    p_time <- ggplot(
        data,
        aes(k, total_time,
            color = algorithm,
            group = interaction(algorithm, k2)
        )
    ) +
        geom_hline(
            data = baseline,
            mapping = aes(yintercept = base_time)
        ) +
        # geom_point(aes(shape = ismin, size = ismin)) +
        geom_point(aes(size = ismin)) +
        geom_line() +
        # scale_y_continuous(labels = scales::number_format(), limits = limits_time) +
        scale_y_continuous(labels = scales::number_format()) +
        scale_shape_manual(values = c(3, 18)) +
        scale_size_manual(values = c(0.5, 1)) +
        scale_color_algorithm() +
        facet_wrap(vars(dataset), ncol = 4) +
        guides(shape = FALSE, linetype = FALSE, size = FALSE) +
        theme_paper() +
        labs(
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
        geom_point(aes(size = ismin)) +
        # geom_point(aes(shape = ismin, size = ismin)) +
        geom_line() +
        scale_y_log10(
            labels = scales::number_format(
                accuracy = 1,
                scale = 1/1000
            )
            # limits = limits_load
        ) +
        scale_shape_manual(values = c(3, 18)) +
        scale_size_manual(values = c(0.5, 1)) +
        scale_color_algorithm() +
        facet_wrap(vars(dataset), ncol = 4) +
        guides(shape = FALSE, size = FALSE) +
        labs(
            y = TeX(str_c("Load (msg $\\cdot 10^3$)"))
        ) +
        theme_paper() +
        theme(
            strip.text = element_blank(),
            strip.background = element_blank()
        )

    if (title) {
        p_time <- p_time + labs(title = str_c("Threshold ", t))
    }

    if (!ytitle) {
        p_time <- p_time + theme(
            axis.title.y = element_blank()
        )
        p_load <- p_load + theme(
            axis.title.y = element_blank()
        )
    }
    if (!yticks) {
        p_time <- p_time + theme(
            axis.text.y = element_blank(),
        )
        p_load <- p_load + theme(
            axis.text.y = element_blank(),
        )
    }

    (p_time / p_load)
}

(
    plot_threshold(filter(plotdata, dataset %in% c("Glove", "SIFT")), 0.5) |
    plot_threshold(filter(plotdata, dataset %in% c("Livejournal", "Orkut")), 0.5, title=F, ytitle=F) |
    plot_threshold(filter(plotdata, dataset %in% c("Glove", "SIFT")), 0.7, ytitle=F) |
    plot_threshold(filter(plotdata, dataset %in% c("Livejournal", "Orkut")), 0.7, title=F, ytitle=F)
    # plot_threshold(plotdata, 0.7, ytitle = FALSE)
) /
    guide_area() +
    plot_layout(
        guides = "collect",
        heights = c(5, 1)
    )

ggsave("imgs/dep_k.png", width = 8, height = 3)