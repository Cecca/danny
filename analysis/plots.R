# Common functionality for plots
source("packages.R")

theme_paper <- function() {
    theme_bw() +
        theme(
            legend.position = "bottom",
            text = element_text(size = 8),
            strip.background = element_blank()
        )
}

scale_color_algorithm <- function() {
    scale_color_manual(values = c(
        "LocalLSH" = "#4e79a7",
        "TwoLevelLSH" = "#f28e2c",
        "OneLevelLSH" = "#e15759",
        "Cartesian" = "#76b7b2"
    ), aesthetics = c("fill", "color"))
}
