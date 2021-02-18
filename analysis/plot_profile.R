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
            # "timely progress (self)",
            "other"
        )),
        ordered = T
    ))

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
    geom_col(position = "stack", color = "white", size=0.1) +
    facet_wrap(vars(algorithm)) +
    scale_color_profile() +
    labs(x="", title="Frame counts by worker", subtitle="Glove dataset") +
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
    geom_col(position = "stack", color = "white", size=0.1) +
    scale_color_profile() +
    facet_wrap(vars(dataset), ncol = 4, scales='free_y') +
    theme_paper() +
    theme(
        axis.text.x = element_text(angle = 90),
        panel.grid = element_blank()
    )

ggsave("imgs/profile.png", width = 8, height = 4)