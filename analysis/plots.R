# Common functionality for plots
source("packages.R")

theme_paper <- function() {
    theme_bw() +
        theme(
            legend.position = "bottom",
            strip.background = element_blank()
        )
}

scale_color_algorithm <- function() {
    scale_color_manual(values = c(
        "one-round-lsh" = "#4e79a7",
        "two-round-lsh" = "#f28e2c",
        "hu-et-al" = "#e15759",
        "all-2-all" = "#76b7b2"
    ), aesthetics = c("fill", "color"))
}

scale_color_profile <- function() {
    scale_color_manual(values = c(
        "deduplicate" = "#4e79a7",
        "sketch" = "#f28e2c",
        "verify" = "#e15759",
        "other" = "#76b7b2"
    ), aesthetics = c("fill", "color"))
}
