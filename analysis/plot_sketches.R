source("tables.R")
source("plots.R")

alldata <- table_search_best() %>%
    filter(threshold == 0.5)

nosketch <- alldata %>%
    filter(sketch_bits == 0) %>%
    select(dataset, threshold, algorithm, k, k2, nosketch_time = total_time)

plotdata <- alldata %>%
    group_by(dataset, threshold, algorithm, k, k2) %>%
    mutate(is_best = total_time == min(total_time)) %>%
    ungroup() %>%
    group_by(dataset, threshold, algorithm) %>%
    mutate(is_algo_best = total_time == min(total_time)) %>%
    ungroup() %>%
    inner_join(nosketch) %>%
    mutate(sketch_speedup = nosketch_time / total_time)

## What is interesting in the visualization below, when considering
## algorithms that partition data according to LSH, is that using larger
## sketches results in higher running times, most likely because of the
## larger number of bits to be transmitted in a large number of iterations

plotdata %>%
    # filter(sketch_bits > 0) %>%
    ggplot(aes(
        x = interaction(sketch_bits, k2, k),
        y = set_units(total_time, "s") %>% drop_units(),
        group = interaction(k, k2),
    )) +
    geom_line() +
    geom_point(aes(color = is_algo_best)) +
    scale_y_log10() +
    scale_color_manual(values = c("black", "red")) +
    facet_grid(vars(dataset), vars(algorithm), scales = "free") +
    guides(color = FALSE, shape = FALSE) +
    labs(
        x = "params",
        y = "time (s)"
    ) +
    theme_paper() +
    theme(axis.text.x = element_text(angle = 90))

# ggsave("imgs/sketches.png", width = 8, height = 4)

plot_one_algo <- function(data, algorithm_name, groups, ylabs = FALSE, strip_text = FALSE) {
    active_groups <- data %>%
        filter(algorithm == algorithm_name) %>%
        distinct({{ groups }}) %>%
        pull()
    p <- data %>%
        filter(algorithm == algorithm_name) %>%
        ggplot(aes(
            x = factor(sketch_bits),
            y = set_units(total_time, "s") %>% drop_units(),
            group = {{ groups }},
        )) +
        geom_line() +
        geom_point(aes(color = is_algo_best)) +
        # Add transparent points from all the algorithms to align axes
        geom_point(
            data = data %>% filter({{ groups }} %in% active_groups),
            alpha = 0
        ) +
        scale_y_log10() +
        scale_color_manual(values = c("black", "red")) +
        facet_grid(
            vars(dataset),
            vars({{ groups }}),
            scales = "free",
            switch = "x"
        ) +
        guides(
            color = FALSE,
            shape = FALSE
        ) +
        coord_cartesian(clip = "off") +
        labs(
            title = algorithm_name,
            x = "params"
        ) +
        theme_paper() +
        theme(
            strip.placement = "outside",
            panel.spacing.x = unit(0, "line")
        )
    if (ylabs) {
        p <- p +
            labs(y = "time (s)")
    } else {
        p <- p +
            theme(
                axis.text.y = element_blank(),
                axis.ticks.y = element_blank(),
                axis.title.y = element_blank()
            )
    }
    if (!strip_text) {
        p <- p +
            theme(
                strip.text.y = element_blank()
            )
    }
    p
}

p_all2all <- plotdata %>%
    plot_one_algo("all-2-all", "", ylabs = T)

p_hu_et_al <- plotdata %>%
    mutate(
        groups = fct_reorder(str_c("k=", k), k)
    ) %>%
    plot_one_algo("hu-et-al", groups)

p_one_round <- plotdata %>%
    mutate(
        groups = fct_reorder(str_c("k=", k), k)
    ) %>%
    plot_one_algo("one-round-lsh", groups)

p_two_round <- plotdata %>%
    mutate(
        groups = fct_reorder(str_c("k=", k, " k2=", k2), k)
    ) %>%
    plot_one_algo("two-round-lsh", groups, strip_text = T)

(p_all2all | p_hu_et_al | p_one_round | p_two_round) +
    plot_layout(
        guides = "collect",
        widths = c(1, 3, 3, 6)
    ) &
    theme(
        text = element_text(size = 10),
        panel.border = element_blank(),
        axis.line = element_line(color = "black", size = 0.1),
    )

ggsave("imgs/sketches.png", width = 10, height = 3)