source("tables.R")
source("plots.R")

plotdata <- table_search_best() %>%
    filter(sketch_bits == 0, algorithm == "TwoLevelLSH")

plot_threshold <- function(dat, thr) {
    pd <- dat %>% filter(threshold == thr)

    fracs <- ggplot(
        pd,
        aes(
            x = k2,
            y = fraction_candidates,
            color = factor(k)
        )
    ) +
        geom_point() +
        geom_line(alpha = 0.5, size = 0.5) +
        geom_hline(yintercept = 1) +
        facet_wrap(vars(dataset), ncol = 4, scales = "fixed") +
        theme_paper()

    loads <- ggplot(
        pd,
        aes(
            x = k2,
            y = Load,
            color = factor(k)
        )
    ) +
        geom_point() +
        geom_line(alpha = 0.5, size = 0.5) +
        facet_wrap(vars(dataset), ncol = 4, scales = "free") +
        theme_paper()

    fracs / loads
}

plot_threshold(plotdata, 0.5) / guide_area() +
    plot_layout(guides = "collect", heights = c(3, 3, 1))

ggsave("imgs/two_level.png", width = 10, height = 5)
