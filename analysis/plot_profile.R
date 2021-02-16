source("packages.R")
source("tables.R")
source("plots.R")

profile <- table_profile() %>%
    group_by(dataset, algorithm) %>%
    mutate(
        x = row_number(frame_total)
    ) %>%
    ungroup() %>%
    pivot_longer(
        deduplicate:other,
        names_to = "name",
        values_to = "frame_count"
    ) %>%
    mutate(name = factor(name,
        levels = rev(c("other", "sketch", "verify", "deduplicate")),
        ordered = T
    ))

detail <- profile %>%
    filter(dataset == "Glove", threshold == 0.5)

ggplot(
    detail,
    aes(
        x = x,
        y = frame_count,
        fill = name
    )
) +
    geom_col(position = "stack", color = "white", size=0.1) +
    facet_wrap(vars(algorithm)) +
    scale_color_profile() +
    theme_paper() +
    labs(x="", title="Frame counts by worker", subtitle="Glove dataset") +
    theme(
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank(),
        panel.grid = element_blank()
    )

ggsave("imgs/profile_glove_detail.png", width = 8, height = 4)


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
    geom_col(position = "fill", color = "white", size=0.1) +
    scale_color_profile() +
    facet_wrap(vars(dataset), ncol = 4) +
    theme_paper() +
    theme(
        axis.text.x = element_text(angle = 90),
        panel.grid = element_blank()
    )

ggsave("imgs/profile.png", width = 8, height = 4)
