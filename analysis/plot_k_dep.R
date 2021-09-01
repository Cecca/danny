source("tables.R")
source("plots.R")

plotdata <- table_search_best() %>%
    # we focus just on experiments with no sketches, to see the effect of k
    filter(sketch_bits == 0) %>%
    mutate(
        total_time = set_units(total_time, "min") %>% drop_units()
    ) %>%
    group_by(dataset, algorithm, threshold) %>%
    mutate(
        ismin = fraction_candidates == min(fraction_candidates)
    )

restricted <- plotdata %>% filter(k <= 12)

plot_counters <- function(data, t, ylabels = FALSE, scale=1, legend = FALSE) {
    data <- filter(data, threshold == t, dry_run) %>%
        group_by(dataset, threshold, algorithm, k) %>%
        # select the parameter k2 minimizing the candidate ratio
        slice_min(fraction_candidates)

    dotsize <- 1 * scale
    linesize <- 0.7 * scale
    maintextsize <- 2.5*scale
    secondarytextsize <- 2*scale

    maxk <- summarise(ungroup(data), max(k)) %>% pull()
    maxy <- summarise(ungroup(data), max(fraction_candidates)) %>% pull()

    pairs <- data %>% distinct(dataset, n_pairs)

    if (ylabels) {
        ytext <- element_text(size=6*scale)
    } else {
        ytext <- element_blank()
    }

    p_candidates <- ggplot(data, aes(
        x = k,
        y = fraction_candidates,
        color = algorithm,
        shape = algorithm
    )) +
        geom_line(size = linesize, alpha = 0.5, position = position_dodge(width = 0.5)) +
        geom_point(size = dotsize, position = position_dodge(width = 0.5)) +
        geom_hline(
            yintercept = 1,
            linetype = "dashed",
            color = "gray40"
        ) +
        geom_text(aes(label = scales::number(n_pairs, scale = 1e-9)),
            size = maintextsize,
            x = maxk, y = if (t == 0.5) {1.3} else {1.15}, data = pairs, inherit.aes = FALSE,
            hjust = 1, vjust = 0,
            color = "gray40"
        ) +
        geom_text(
            label = "billion pairs",
            x = maxk, y = if (t == 0.5) {1.08} else {1.05}, data = pairs, inherit.aes = FALSE,
            size = secondarytextsize,
            hjust = 1, vjust = 0,
            color = "gray40"
        ) +
        facet_wrap(vars(dataset), ncol = 4) +
        labs(
            title = str_c("Threshold ", t),
            y = "candidate ratio"
        ) +
        scale_color_algorithm() +
        scale_x_continuous(breaks = scales::pretty_breaks()) +
        scale_y_continuous(limits = c(0, max(maxy, 1.2))) +
        coord_cartesian(clip="off") +
        theme_paper() +
        theme(
            title = element_text(size = 7*scale),
            axis.title.x = element_blank(),
            axis.text.x = element_blank(),
            axis.title.y = ytext,
            strip.background = element_blank(),
            strip.text = element_text(size=6*scale),
            legend.position = "none",
            legend.direction = "horizontal"
        ) +
        annotate("segment", x=-Inf, xend=Inf, y=-Inf, yend=-Inf, size = 0.4)+
        annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=+Inf, size = 0.4)

    if (legend) {
        p_candidates <- p_candidates +
            theme(
                legend.position = c(0.6, 1.4),
                legend.title = element_blank()
            )
    }

    p_load <- ggplot(data, aes(
        x = k,
        y = Load,
        color = algorithm,
        shape = algorithm
    )) +
        geom_line(size = linesize, alpha = 0.5, position = position_dodge(width = 0.5)) +
        geom_point(size = dotsize, position = position_dodge(width = 0.5)) +
        facet_wrap(vars(dataset), ncol = 4) +
        labs(
            y = "load"
        ) +
        scale_color_algorithm() +
        scale_x_continuous(breaks = scales::pretty_breaks()) +
        scale_y_continuous(labels = scales::number_format(scale = 1e-3)) +
        theme_paper() +
        theme(
            plot.margin = unit(c(0,0,0,0), units="mm"),
            strip.text = element_blank(),
            strip.background = element_blank(),
            axis.title.y = ytext,
            legend.position = "none"
        ) + 
        annotate("segment", x=-Inf, xend=Inf, y=-Inf, yend=-Inf, size = 0.8)+
        annotate("segment", x=-Inf, xend=-Inf, y=-Inf, yend=+Inf, size = 0.8)


    (p_candidates / p_load) & theme(
        axis.text = element_text(size=5*scale),
        panel.grid.minor = element_blank(),
        panel.border = element_blank()
    )
}

composed <- (plot_counters(restricted, 0.5, ylabels = TRUE) | plot_counters(restricted, 0.7, legend = T)) + #/ guide_area() +
    plot_layout(guides = "keep", heights = c(10, 1))
ggsave("imgs/counters.png", plot=composed, width = 8, height = 2.5)

composed <- (plot_counters(plotdata, 0.5, ylabels = TRUE, scale=2) | plot_counters(plotdata, 0.7, legend = T, scale=2)) + #/ guide_area() +
    plot_layout(guides = "keep", heights = c(10, 1))
ggsave("imgs/counters-full.png", plot=composed, width = 8*2, height = 3.7*2)