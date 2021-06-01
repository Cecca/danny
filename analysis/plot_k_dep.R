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

plot_counters <- function(data, t, ylabels = FALSE) {
    data <- filter(data, threshold == t, dry_run) %>%
        group_by(dataset, threshold, algorithm, k) %>%
        # select the parameter k2 minimizing the candidate ratio
        slice_min(fraction_candidates)

    maxk <- summarise(ungroup(data), max(k)) %>% pull()
    maxy <- summarise(ungroup(data), max(fraction_candidates)) %>% pull()

    pairs <- data %>% distinct(dataset, n_pairs)

    if (ylabels) {
        ytext <- element_text()
    } else {
        ytext <- element_blank()
    }

    p_candidates <- ggplot(data, aes(
        x = k,
        y = fraction_candidates,
        color = algorithm
    )) +
        geom_line(size = 0.4, alpha = 0.5, position = position_dodge(width = 0.5)) +
        geom_point(size = 0.5, position = position_dodge(width = 0.5)) +
        # geom_rug(data = function (d) filter(d, ismin)) +
        geom_hline(
            yintercept = 1,
            linetype = "dashed",
            color = "gray40"
        ) +
        geom_text(aes(label = scales::number(n_pairs, scale = 1e-9)),
            size = 3,
            x = maxk, y = 1.05, data = pairs, inherit.aes = FALSE,
            hjust = 1, vjust = 0,
            color = "gray40"
        ) +
        geom_text(
            label = "billion pairs",
            x = maxk, y = 0.95, data = pairs, inherit.aes = FALSE,
            size = 2,
            hjust = 1, vjust = 1,
            color = "gray40"
        ) +
        facet_wrap(vars(dataset), ncol = 4) +
        labs(
            title = str_c("Threshold ", t),
            y = "candidate ratio"
        ) +
        scale_color_algorithm() +
        scale_x_continuous() +
        scale_y_continuous(limits = c(0, max(maxy, 1.2))) +
        theme_paper() +
        theme(
            title = element_text(size = 9),
            axis.title.x = element_blank(),
            axis.text.x = element_blank(),
            axis.title.y = ytext,
            strip.background = element_blank()
        )

    p_load <- ggplot(data, aes(
        x = k,
        y = Load,
        color = algorithm
    )) +
        geom_line(size = 0.4, alpha = 0.5, position = position_dodge(width = 0.5)) +
        geom_point(size = 0.5, position = position_dodge(width = 0.5)) +
        facet_wrap(vars(dataset), ncol = 4) +
        labs(
            y = "load"
        ) +
        scale_color_algorithm() +
        scale_x_continuous() +
        scale_y_continuous(labels = scales::number_format(scale = 1e-3)) +
        theme_paper() +
        theme(
            strip.text = element_blank(),
            strip.background = element_blank(),
            axis.title.y = ytext,
            legend.position = "none"
        )

    (p_candidates / p_load) # / guide_area() +
    # plot_layout(
    #     guides = "collect",
    #     heights = c(2, 2, 1)
    # )
}

(plot_counters(plotdata, 0.5, ylabels = TRUE) | plot_counters(plotdata, 0.7)) / guide_area() +
    plot_layout(guides = "collect", heights = c(10, 1))

ggsave("imgs/counters.png", width = 8, height = 4)

plotdata %>%
    filter(dataset == "Orkut", threshold == 0.7, algorithm == "OneLevelLSH") %>%
    select(k, fraction_candidates, Load) %>%
    arrange(Load)

# plot_threshold <- function(data, t, ytitle = TRUE, yticks = TRUE, title = TRUE) {
#     baseline <- filter(data, algorithm == "Cartesian") %>%
#         ungroup() %>%
#         select(dataset, threshold, base_time = total_time)
#     data <- filter(data, algorithm != "Cartesian") %>%
#         ungroup() %>%
#         inner_join(baseline) %>%
#         filter(total_time < 4 * base_time)

#     # compute the limits before filtering, so that all plots share the same scale
#     # limits_time <- pull(data, total_time) %>% range()
#     # limits_load <- pull(data, Load) %>% range()

#     data <- filter(data, threshold == t)
#     baseline <- filter(baseline, threshold == t)

#     p_time <- ggplot(
#         data,
#         aes(k, total_time,
#             color = algorithm,
#             group = interaction(algorithm, k2)
#         )
#     ) +
#         geom_hline(
#             data = baseline,
#             mapping = aes(yintercept = base_time)
#         ) +
#         # geom_point(aes(shape = ismin, size = ismin)) +
#         geom_point(aes(size = ismin)) +
#         geom_line() +
#         # scale_y_continuous(labels = scales::number_format(), limits = limits_time) +
#         scale_y_continuous(labels = scales::number_format()) +
#         scale_shape_manual(values = c(3, 18)) +
#         scale_size_manual(values = c(0.5, 1)) +
#         scale_color_algorithm() +
#         facet_wrap(vars(dataset), ncol = 4) +
#         guides(shape = FALSE, linetype = FALSE, size = FALSE) +
#         theme_paper() +
#         labs(
#             y = "Total time (min)"
#         ) +
#         theme(
#             axis.text.x = element_blank(),
#             axis.title.x = element_blank(),
#             strip.background = element_blank(),
#             strip.text = element_text(hjust = 0)
#         )

#     p_load <- ggplot(
#         data,
#         aes(k, Load, color = algorithm, group = interaction(algorithm, k2))
#     ) +
#         geom_point(aes(size = ismin)) +
#         # geom_point(aes(shape = ismin, size = ismin)) +
#         geom_line() +
#         scale_y_log10(
#             labels = scales::number_format(
#                 accuracy = 1,
#                 scale = 1 / 1000
#             )
#             # limits = limits_load
#         ) +
#         scale_shape_manual(values = c(3, 18)) +
#         scale_size_manual(values = c(0.5, 1)) +
#         scale_color_algorithm() +
#         facet_wrap(vars(dataset), ncol = 4) +
#         guides(shape = FALSE, size = FALSE) +
#         labs(
#             y = TeX(str_c("Load (msg $\\cdot 10^3$)"))
#         ) +
#         theme_paper() +
#         theme(
#             strip.text = element_blank(),
#             strip.background = element_blank()
#         )

#     if (title) {
#         p_time <- p_time + labs(title = str_c("Threshold ", t))
#     }

#     if (!ytitle) {
#         p_time <- p_time + theme(
#             axis.title.y = element_blank()
#         )
#         p_load <- p_load + theme(
#             axis.title.y = element_blank()
#         )
#     }
#     if (!yticks) {
#         p_time <- p_time + theme(
#             axis.text.y = element_blank(),
#         )
#         p_load <- p_load + theme(
#             axis.text.y = element_blank(),
#         )
#     }

#     (p_time / p_load)
# }

# (
#     plot_threshold(filter(plotdata, dataset %in% c("Glove", "SIFT")), 0.5) |
#         plot_threshold(filter(plotdata, dataset %in% c("Livejournal", "Orkut")), 0.5, title = F, ytitle = F) |
#         plot_threshold(filter(plotdata, dataset %in% c("Glove", "SIFT")), 0.7, ytitle = F) |
#         plot_threshold(filter(plotdata, dataset %in% c("Livejournal", "Orkut")), 0.7, title = F, ytitle = F)
#     # plot_threshold(plotdata, 0.7, ytitle = FALSE)
# ) /
#     guide_area() +
#     plot_layout(
#         guides = "collect",
#         heights = c(5, 1)
#     )

# ggsave("imgs/dep_k.png", width = 8, height = 3)