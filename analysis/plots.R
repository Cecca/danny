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
    # local_work <- colorspace::sequential_hcl(4, "Reds")
    local_work <- rev(c(
        "#A00017","#C93931","#E86853","#FB9A7E"
    ))
    # state <- colorspace::sequential_hcl(2, "Blues")
    state <- rev(c(
        "#173F8C",
        "#3C74F2"
    ))
    # timely <- colorspace::sequential_hcl(4, "Purples")
    timely <- rev(c(
        "#9500A1",
        "#BC6AAE"
    ))
    values <- c(
        # Local work
        "local join (self)" = local_work[1],
        "sketch" = local_work[2],
        "verify" = local_work[3],
        "deduplicate" = local_work[4],
        # State management
        "hashmap" = state[1],
        "extend vector" = state[2],
        # Timely
        "mutex lock/unlock (self)" = timely[1],
        "communication (self)" = timely[2],
        # "worker step (self)" = timely[3],
        # "timely progress (self)" = timely[4],
        "other" = "gray90"
    )
    scale_color_manual(values = values, aesthetics = c("fill", "color"))
}
