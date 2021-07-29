source("tables.R")
source("plots.R")

plotdata <- table_sysmonitor() %>% 
    filter(id %in% c(2719, 2720, 2721)) %>% 
    mutate(time = time / 60) # put time in minutes

tot_net <- 1024 * 1024

ggplot(plotdata, aes(x=time)) +
    # CPU
    geom_line(aes(y=cpu_user), color="darkorange") +
    geom_line(aes(y=mem_used / mem_total), color="darkgreen") +
    geom_line(aes(y=net_tx / tot_net), color="blue") +
    geom_line(aes(y=net_rx / tot_net), color="steelblue1") +
    geom_label(
        aes(y=cpu_user),
        label = "CPU",
        color = "darkorange",
        fill="white",
        label.size=NA,
        size=3,
        data=function (d) { group_by(d, id) %>% sample_n(1) }
    ) +
    # Memory
    geom_label(
        aes(y=mem_used / mem_total),
        label = "Memory",
        color = "darkgreen",
        fill="white",
        label.size=NA,
        size=3,
        data=function (d) { group_by(d, id) %>% sample_n(1) }
    ) +
    # Network
    geom_label(
        aes(y=net_tx / tot_net),
        label = "Net (tx)",
        color = "blue",
        fill="white",
        label.size=NA,
        size=3,
        data=function (d) { group_by(d, id) %>% sample_n(1) }
    ) +
    # Network
    geom_label(
        aes(y=net_rx / tot_net),
        label = "Net (rx)",
        color = "steelblue1",
        fill="white",
        label.size=NA,
        size=3,
        data=function (d) { group_by(d, id) %>% sample_n(1) }
    ) +
    facet_wrap(vars(id), ncol=1) +
    scale_y_continuous(labels=scales::percent_format(), expand=expansion(mult=0.1)) +
    labs(
        x = "time (minutes)",
        y = "usage"
    ) +
    theme_paper()

 
