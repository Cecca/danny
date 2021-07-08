source("tables.R")
source("plots.R")

sizes <- tribble(
    ~dataset, ~data_size,
    "SIFT", 1000000,
    "Livejournal", 3201203,
    "Orkut", 2783196,
    "Glove", 1193514
) %>%
    mutate(
        max_pairs = data_size * (data_size - 1) / 2
    )

plotdata <- table_candidates() %>%
    # we focus just on experiments with no sketches, to see the effect of k
    filter(sketch_bits == 0) %>%
    # filter((k == 0) || between(k, 4, 16))  %>%
    filter(k2 %in% c(0, 6)) %>%
    mutate(
        total_time = set_units(total_time, "s") %>% drop_units()
    ) %>%
    group_by(dataset, algorithm, threshold) %>%
    filter(
        total_time == min(total_time)
    ) %>%
    mutate(CandidatePairs = as.double(CandidatePairs - SelfPairsDiscarded))

plot_threshold <- function(data, t, ytitle = TRUE, yticks = TRUE, title = TRUE) {
    first_iter <- filter(data, threshold == t, step == 0)
    worker_total <- filter(data, threshold == t) %>%
        group_by(algorithm, dataset, worker) %>%
        summarise(CandidatePairs = sum(CandidatePairs))
    totals <- data %>%
        group_by(algorithm, dataset) %>%
        summarise(CandidatePairs = sum(CandidatePairs))

    draw <- function(d) {
        ggplot(
            d,
            aes(algorithm, CandidatePairs,
                color = algorithm
            )
        ) +
            geom_boxplot(outlier.size = 0.2) +
            geom_point(position = "jitter", size = 0.1) +
            scale_y_continuous() +
            scale_color_algorithm() +
            facet_wrap(vars(dataset), ncol = 4) +
            guides(shape = FALSE, linetype = FALSE, size = FALSE) +
            theme_paper() +
            theme(
                axis.text.x = element_blank(),
                axis.title.x = element_blank(),
                strip.background = element_blank(),
                strip.text = element_text(hjust = 0)
            )
    }

    p_first_iter <- draw(first_iter) + labs(
        y = "Candidates per worker, first iteration"
    )
    p_candidates <- draw(worker_total) + labs(
        y = "Total candidates per worker"
    )
    p_averages <- draw(totals) + labs(
        y = "Average candidates per worker"
    )

    if (title) {
        p_first_iter <- p_first_iter + labs(title = str_c("Threshold ", t))
    }

    if (!ytitle) {
        # p_averages <- p_averages + theme(
        #     axis.title.y = element_blank()
        # )
        # p_candidates <- p_candidates + theme(
        #     axis.title.y = element_blank()
        # )
        # p_first_iter <- p_first_iter + theme(
        #     axis.title.y = element_blank()
        # )
    }
    if (!yticks) {
        # p_averages <- p_averages + theme(
        #     axis.text.y = element_blank(),
        # )
        # p_candidates <- p_candidates + theme(
        #     axis.text.y = element_blank(),
        # )
        # p_first_iter <- p_first_iter + theme(
        #     axis.text.y = element_blank(),
        # )
    }

    p_first_iter / p_candidates / p_averages
}

(
    plot_threshold(filter(plotdata, dataset %in% c("Glove", "SIFT")), 0.5) |
        plot_threshold(filter(plotdata, dataset %in% c("Livejournal", "Orkut")), 0.5, title = F, ytitle = F) |
        plot_threshold(filter(plotdata, dataset %in% c("Glove", "SIFT")), 0.7, ytitle = F) |
        plot_threshold(filter(plotdata, dataset %in% c("Livejournal", "Orkut")), 0.7, title = F, ytitle = F)
) /
    guide_area() +
    plot_layout(
        guides = "collect",
        heights = c(5, 1)
    )

ggsave("imgs/subproblem_size.png", width = 8, height = 6)