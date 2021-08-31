source("tables.R")
source("plots.R")

selectors <- tribble(
    ~dataset, ~threshold, ~algorithm, ~k, ~k2, ~opt,
    "Glove", 0.5, "Cartesian", 0, 0, T,
    "Glove", 0.5, "OneLevelLSH", 4, 0, F,
    "Glove", 0.5, "OneLevelLSH", 6, 0, T,
    "Glove", 0.5, "LocalLSH", 8, 0, F,
    "Glove", 0.5, "LocalLSH", 20, 0, T,
    "Glove", 0.5, "TwoLevelLSH", 4, 12, F,
    "Glove", 0.5, "TwoLevelLSH", 3, 12, T,
    # ------------------------------------
    "SIFT", 0.5, "Cartesian", 0, 0, T,
    "SIFT", 0.5, "OneLevelLSH", 6, 0, F,
    "SIFT", 0.5, "OneLevelLSH", 8, 0, T,
    "SIFT", 0.5, "LocalLSH", 8, 0, F,
    "SIFT", 0.5, "LocalLSH", 20, 0, T,
    "SIFT", 0.5, "TwoLevelLSH", 4, 12, F,
    "SIFT", 0.5, "TwoLevelLSH", 3, 12, T,
    # ------------------------------------
    "Livejournal", 0.5, "Cartesian", 0, 0, T,
    "Livejournal", 0.5, "OneLevelLSH", 8, 0, T,
    "Livejournal", 0.5, "OneLevelLSH", 6, 0, F,
    "Livejournal", 0.5, "LocalLSH", 8, 0, F,
    "Livejournal", 0.5, "LocalLSH", 17, 0, T,
    "Livejournal", 0.5, "TwoLevelLSH", 4, 12, T,
    "Livejournal", 0.5, "TwoLevelLSH", 6, 12, F,
    # ------------------------------------
    "Orkut", 0.5, "Cartesian", 0, 0, T,
    "Orkut", 0.5, "OneLevelLSH", 8, 0, T,
    "Orkut", 0.5, "OneLevelLSH", 6, 0, F,
    "Orkut", 0.5, "LocalLSH", 8, 0, F,
    "Orkut", 0.5, "LocalLSH", 20, 0, T,
    "Orkut", 0.5, "TwoLevelLSH", 4, 12, T,
    "Orkut", 0.5, "TwoLevelLSH", 6, 12, F
) %>%
    group_by(dataset, threshold, algorithm, k, k2, opt) %>%
    summarise(sketch_bits = c(0, 256, 512)) %>%
    ungroup()

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
    right_join(selectors)

ranges <- plotdata %>%
    select(dataset, threshold, total_time) %>%
    drop_na() %>%
    group_by(dataset, threshold) %>%
    summarise(
        range = list(range(drop_units(set_units(total_time, "min"))))
    )

## What is interesting in the visualization below, when considering
## algorithms that partition data according to LSH, is that using larger
## sketches results in higher running times, most likely because of the
## larger number of bits to be transmitted in a large number of iterations

plot_one_algo <- function(data, algorithm_name, groups, ylabs = FALSE, strip_text = FALSE, strip_text_x = FALSE, short=FALSE) {
    doplot <- function(dname) {
        r <- ranges %>%
            filter(dataset == dname) %>%
            pull(range) %>%
            first()
        pdata <- data %>%
            filter(algorithm == algorithm_name) %>%
            filter(dataset == dname)
        p <- pdata %>%
            ggplot(aes(
                x = factor(sketch_bits),
                y = set_units(total_time, "min") %>% drop_units(),
                group = {{ groups }},
            )) +
            geom_line() +
            geom_point(aes(color = is_algo_best)) +
            geom_text_repel(aes(label = total_time %>% set_units("min") %>% drop_units() %>% scales::number(accuracy = 1)), size = 2) +
            scale_x_discrete(
                labels = c("0", "256", "512"),
                breaks = c("0", "256", "512")
            ) +
            scale_y_continuous(
                limits = r,
                breaks = scales::extended_breaks()
            ) +
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
                x = "sketch bits"
            ) +
            theme_paper() +
            theme(
                panel.grid = element_blank(),
                strip.placement = "outside",
                panel.spacing.x = unit(0, "line"),
                axis.title.x = element_blank(),
                plot.title = element_text(size=8),
                plot.margin = unit(c(0,0,0,0),units="mm")
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
    if (short) {
        (doplot("Glove") + labs(title = algorithm_name)) /
            doplot("Livejournal")
    } else {
        (doplot("Glove") + labs(title = algorithm_name)) /
            doplot("SIFT") /
            doplot("Livejournal") /
            doplot("Orkut")
    }
}

create_groups <- function(data) {
    data %>%
        mutate(
            lab = if_else(opt, str_c("*k=", k), str_c("k=", k)),
            groups = fct_reorder(lab, k)
        )
}

do_plot_sketches <- function(plotdata, short=FALSE)  {
    p_all2all <- plotdata %>%
        plot_one_algo("Cartesian", "", ylabs = T, short=short)

    p_hu_et_al <- plotdata %>%
        create_groups() %>%
        # mutate(
        #     groups = fct_reorder(str_c("k=", k), k)
        # ) %>%
        plot_one_algo("OneLevelLSH", groups, short=short)

    p_one_round <- plotdata %>%
        create_groups() %>%
        # mutate(
        #     groups = fct_reorder(str_c("k=", k), k)
        # ) %>%
        plot_one_algo("LocalLSH", groups, short=short)

    p_two_round <- plotdata %>%
        create_groups() %>%
        # mutate(
        #     groups = fct_reorder(str_c("k=", k), k)
        # ) %>%
        plot_one_algo("TwoLevelLSH", groups, strip_text = T, short=short)


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
}

do_plot_sketches(plotdata, short=F)
ggsave("imgs/sketches-all-datasets.png", width = 10, height = 5, dpi=300)
do_plot_sketches(plotdata, short=T)
ggsave("imgs/sketches.png", width = 10, height = 2.5)

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

table_sketch_quality() %>%
    filter(sketch_bits > 0) %>%
    select(dataset, threshold, sketch_bits, lost_pairs, lost_fraction) %>%
    ggplot(aes(x = factor(sketch_bits), y = lost_fraction)) +
    geom_point() +
    geom_segment(aes(xend = factor(sketch_bits)), yend = 0) +
    geom_hline(yintercept = 0.01) +
    scale_y_continuous(labels = scales::percent_format()) +
    labs(
        x = "sketch bits",
        y = "false negative rate"
    ) +
    facet_wrap(vars(dataset, threshold), ncol = 8) +
    theme_paper()

ggsave("imgs/sketch_loss.png", width = 10, height = 2)