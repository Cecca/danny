source("tables.R")
source("plots.R")

selectors <- tribble(
    ~dataset, ~threshold, ~algorithm, ~k, ~k2,
    "Glove", 0.5, "Cartesian", 0, 0,
    "Glove", 0.5, "OneLevelLSH", 4, 0,
    "Glove", 0.5, "OneLevelLSH", 6, 0,
    "Glove", 0.5, "LocalLSH", 8, 0,
    "Glove", 0.5, "LocalLSH", 20, 0,
    "Glove", 0.5, "TwoLevelLSH", 2, 12,
    "Glove", 0.5, "TwoLevelLSH", 3, 12,
    # ------------------------------------
    "SIFT", 0.5, "Cartesian", 0, 0,
    "SIFT", 0.5, "OneLevelLSH", 6, 0,
    "SIFT", 0.5, "OneLevelLSH", 8, 0,
    "SIFT", 0.5, "LocalLSH", 8, 0,
    "SIFT", 0.5, "LocalLSH", 20, 0,
    "SIFT", 0.5, "TwoLevelLSH", 2, 12,
    "SIFT", 0.5, "TwoLevelLSH", 3, 12,
    # ------------------------------------
    "Livejournal", 0.5, "Cartesian", 0, 0,
    "Livejournal", 0.5, "OneLevelLSH", 8, 0,
    "Livejournal", 0.5, "OneLevelLSH", 10, 0,
    "Livejournal", 0.5, "LocalLSH", 8, 0,
    "Livejournal", 0.5, "LocalLSH", 17, 0,
    "Livejournal", 0.5, "TwoLevelLSH", 6, 10,
    "Livejournal", 0.5, "TwoLevelLSH", 8, 10,
    # ------------------------------------
    "Orkut", 0.5, "Cartesian", 0, 0,
    "Orkut", 0.5, "OneLevelLSH", 8, 0,
    "Orkut", 0.5, "OneLevelLSH", 6, 0,
    "Orkut", 0.5, "LocalLSH", 8, 0,
    "Orkut", 0.5, "LocalLSH", 20, 0,
    "Orkut", 0.5, "TwoLevelLSH", 4, 12,
    "Orkut", 0.5, "TwoLevelLSH", 6, 12,
)

alldata <- table_sketches() %>%
    filter(threshold == 0.5)

nosketch <- alldata %>%
    filter(sketch_bits == 0) %>%
    select(dataset, threshold, algorithm, k, k2, nosketch_time = total_time)

plotdata <- alldata %>%
    # filter(k2 %in% c(0, 6)) %>%
    group_by(dataset, algorithm, threshold, k, k2, sketch_bits) %>%
    slice_min(total_time) %>%
    ungroup() %>%
    group_by(dataset, threshold, algorithm, k, k2) %>%
    mutate(is_best = total_time == min(total_time)) %>%
    ungroup() %>%
    group_by(dataset, threshold, algorithm) %>%
    mutate(is_algo_best = total_time == min(total_time)) %>%
    ungroup() %>%
    # filter(dataset == "Livejournal", algorithm == "TwoLevelLSH") %>% select(k, k2, sketch_bits) %>%print()
    semi_join(selectors)

## What is interesting in the visualization below, when considering
## algorithms that partition data according to LSH, is that using larger
## sketches results in higher running times, most likely because of the
## larger number of bits to be transmitted in a large number of iterations

plot_one_algo <- function(data, algorithm_name, groups, ylabs = FALSE, strip_text = FALSE) {
    active_groups <- data %>%
        filter(algorithm == algorithm_name) %>%
        distinct({{ groups }}) %>%
        pull()
    p <- data %>%
        filter(algorithm == algorithm_name) %>%
        ggplot(aes(
            x = factor(sketch_bits),
            y = set_units(total_time, "min") %>% drop_units(),
            group = {{ groups }},
        )) +
        geom_line() +
        geom_point(aes(color = is_algo_best)) +
        geom_text_repel(aes(label = total_time %>% set_units("min") %>% drop_units() %>% scales::number(accuracy = 1)), size = 2) +
        # Add transparent points from all the algorithms to align axes
        geom_point(
            data = data %>% filter({{ groups }} %in% active_groups),
            alpha = 0
        ) +
        scale_x_discrete(
            labels = c("0", "256", "512"),
            breaks = c("0", "256", "512")
            # labels = c("0", "", "128", "", "512"),
            # breaks = c("0", "64", "128", "256", "512")
        ) +
        # scale_y_log10() +
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
            x = "sketch bits"
        ) +
        theme_paper() +
        theme(
            strip.placement = "outside",
            panel.spacing.x = unit(0, "line")
        )
    if (ylabs) {
        p <- p +
            labs(y = "time (min)")
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
    plot_one_algo("Cartesian", "", ylabs = T)

p_hu_et_al <- plotdata %>%
    mutate(
        groups = fct_reorder(str_c("k=", k), k)
    ) %>%
    plot_one_algo("OneLevelLSH", groups)

p_one_round <- plotdata %>%
    mutate(
        groups = fct_reorder(str_c("k=", k), k)
    ) %>%
    plot_one_algo("LocalLSH", groups)

p_two_round <- plotdata %>%
    mutate(
        groups = fct_reorder(str_c("k=", k), k)
    ) %>%
    plot_one_algo("TwoLevelLSH", groups, strip_text = T)

(p_all2all | p_hu_et_al | p_one_round | p_two_round) +
    plot_layout(
        guides = "collect",
        widths = c(1, 2, 2, 2)
    ) &
    theme(
        text = element_text(size = 10),
        panel.border = element_blank(),
        axis.line = element_line(color = "black", size = 0.1),
    )

ggsave("imgs/sketches.png", width = 10, height = 3)

# print("Effect of sketching on the accuracy")
# table_search_best() %>%
#     filter(algorithm == "all-2-all") %>%
#     select(dataset, sketch_bits, output_size, threshold) %>%
#     arrange(sketch_bits) %>%
#     pivot_wider(names_from = sketch_bits, values_from = output_size) %>%
#     mutate(
#         loss64 = (`0` - `64`) / `0`,
#         loss128 = (`0` - `128`) / `0`,
#         loss256 = (`0` - `256`) / `0`,
#         loss512 = (`0` - `512`) / `0`
#     ) %>%
#     select(dataset, threshold, loss64, loss128, loss256, loss512) %>%
#     summarise(across(loss64:loss512, ~max(.x) %>% scales::percent(accuracy=0.001)))