source("packages.R")
source("tables.R")
source("plots.R")

profile <- table_profile() %>%
    filter(name != "timely progress (self)") %>%
    mutate(name = factor(name,
        levels = rev(c(
            # Local work
            "local join (self)",
            "sketch",
            "verify",
            "deduplicate",
            # State management
            "hashmap",
            "extend vector",
            # Timely
            "mutex lock/unlock (self)",
            "communication (self)",
            # "worker step (self)",
            "timely progress (self)",
            "other"
        )),
        ordered = T
    ))

scale_color_profile <- function() {
    local_work <- rev(c(
        "#A00017","#C93931","#E86853","#FB9A7E"
    ))
    infra <- rev(c(
        "#093378",
        "#2E50A8",
        "#546FD3",
        "#7D90FA",
        "#bbc4f8"
    ))
    values <- c(
        # Local work
        "local join (self)" = local_work[1],
        "sketch" = local_work[2],
        "verify" = local_work[3],
        "deduplicate" = local_work[4],
        # State management
        "hashmap" = infra[1],
        "extend vector" = infra[2],
        # Timely
        "mutex lock/unlock (self)" = infra[3],
        "communication (self)" = infra[4],
        "timely progress (self)" = infra[5],
        # "worker step (self)" = timely[3],
        "other" = "gray90"
    )
    scale_color_manual(values = values, aesthetics = c("fill", "color"))
}

detail <- profile %>%
    filter(dataset == "Glove", threshold == 0.5) %>%
    group_by(id, hostname, name, algorithm) %>%
    summarise(frame_count = sum(frame_count)) %>%
    ungroup()

ggplot(
    detail,
    aes(
        # x = str_c(str_sub(hostname, 1, 5), "-", str_sub(thread, -1)),
        x = hostname,
        y = frame_count,
        fill = name
    )
) +
    geom_col(position = "stack", color = "white", size = 0.1) +
    facet_wrap(vars(algorithm)) +
    scale_color_profile() +
    labs(x = "", title = "Frame counts by worker", subtitle = "Glove dataset") +
    coord_flip() +
    theme_paper() +
    theme(
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank(),
        panel.grid = element_blank()
    )

ggsave("imgs/profile_glove_detail.png", width = 8, height = 8)


profile %>%
    group_by(algorithm, dataset, name) %>%
    summarise(frame_count = sum(frame_count)) %>%
    ggplot(
        aes(
            x = algorithm,
            y = frame_count,
            fill = name
        )
    ) +
    geom_col(position = "stack", color = "white", size = 0.1) +
    scale_color_profile() +
    facet_wrap(vars(dataset), ncol = 4, scales = "free_y") +
    theme_paper() +
    theme(
        axis.text.x = element_text(angle = 90),
        panel.grid = element_blank()
    )

ggsave("imgs/profile.png", width = 8, height = 3)

# The following plot focuses instead on how many frames we spend per output
# pair, which should be on the same order of magnitude for all three algorithms.
normalized_profile <- table_normalized_profile() %>%
    select(-ends_with("input"), -sketch, -verify, -deduplicate) %>%
    pivot_longer(ends_with("ppf"), names_to = "component", values_to = "ppf") %>%
    mutate(
        component = str_remove(component, "_ppf"),
        component = if_else(component == "dedup", "deduplicate", component),
        component = factor(component,
            levels = c("sketch", "verify", "deduplicate"),
            ordered = T
        )
    )

ggplot(
    normalized_profile,
    aes(
        x = algorithm,
        y = ppf,
        fill = component
    )
) +
    geom_col(position = position_dodge()) +
    scale_y_log10(labels = scales::number_format()) +
    facet_wrap(vars(dataset), ncol = 4) +
    scale_color_profile() +
    labs(x = "algorithm", y = "Pairs per Frame") +
    theme_paper() +
    theme(axis.text.x = element_text(angle=90))

ggsave("imgs/profile_normalized.png", width = 8, height = 3)
