source("tables.R")
source("plots.R")

plotdata <- table_sysmonitor() %>% 
    ungroup() %>%
    filter(hostname == "desktop2") %>%
    mutate(time = time / 60) # put time in minutes

tot_net <- plotdata %>% summarise(max(net_tx)) %>% pull()

endtimes <- plotdata %>%
    group_by(algorithm) %>%
    summarise(end_local = max(time))

plotdata %>% 
  group_by(algorithm) %>% 
  mutate(net_frac = net_tx / max(net_tx)) %>% 
  filter(net_frac > 0.1) %>% 
  summarise(start_comm = min(time), end_comm=max(time), comm_span = 60*(end_comm - start_comm))  %>%
  inner_join(endtimes) %>%
  mutate(span_local = 60*(end_local - end_comm), span_init = 60*start_comm)

annotations <- tribble(
    ~algorithm, ~from, ~to, ~text, ~y, ~ytext,
    "TwoLevelLSH", 0.5, 1.4, "58 s", 0.25, 0.12,
    "TwoLevelLSH", 1.5, 6.5, "307 s", 0.15, 0.25,
    "TwoLevelLSH", 0, 0.45, "29 s", 0.75, 0.55,
    "OneLevelLSH", 0.12, 1.2, "68 s", 0.25, 0.12,
    "OneLevelLSH", 1.22, 6.1, " 293 s", 0.75, 0.55,
    "LocalLSH", 4, 4, "Initialization: 2 s", NA, 0.75,
    "LocalLSH", 4, 4, "Communication: 16 s", NA, 0.5,
    "LocalLSH", 4, 4, "Local computation: 44 s", NA, 0.25,
)

percent_idx <- function(p) {
    as.integer(quantile(1:n(), p))
}

ggplot(plotdata, aes(x=time, group=hostname)) +
    # CPU
    geom_line(aes(y=net_tx / tot_net), color="blue") +
    geom_line(aes(y=net_rx / tot_net), color="steelblue1") +
    geom_line(aes(y=cpu_user), color="darkorange") +
    geom_ribbon(aes(
            ymax=(mem_used / mem_total) + 0.02,
            ymin=(mem_used / mem_total) - 0.02
        ), 
        fill="white"
    ) +
    geom_line(aes(y=mem_used / mem_total), color="darkgreen") +
    geom_label(
        aes(y=cpu_user, hjust=0),
        label = "CPU",
        color = "darkorange",
        fill="white",
        alpha=0.0,
        label.size=NA,
        size=2.5,
        data=function (d) { group_by(d, id) %>% slice(percent_idx(.99)) }
    ) +
    # Memory
    geom_label(
        aes(y=mem_used / mem_total, hjust=0),
        label = "Memory",
        color = "darkgreen",
        fill="white",
        alpha=0.0,
        label.size=NA,
        size=2.5,
        data=function (d) { group_by(d, id) %>% slice(percent_idx(0.99)) }
    ) +
    # Network
    geom_label(
        aes(y=net_tx / tot_net, hjust=0),
        label = "Network",
        color = "blue",
        fill="white",
        alpha=0.0,
        label.size=NA,
        size=2.5,
        data=function (d) { group_by(d, id) %>% slice(percent_idx(0.99)) }
    ) +
    geom_segment(
        aes(y=y, yend=y, x=from, xend=to),
        data=annotations,
        inherit.aes=F,
        linetype="dotted",
        size=.4
    ) +
    geom_text(
        aes(y=ytext, x=(from+to)/2, label=text),
        data=annotations,
        inherit.aes=F,
        size=2.5
    ) +
    facet_wrap(vars(algorithm), ncol=1) +
    scale_y_continuous(labels=scales::percent_format(), expand=expansion(mult=0.1)) +
    scale_x_continuous(expand=expansion(add=c(0.3, 1))) +
    labs(
        x = "time (minutes)",
        y = "usage"
    ) +
    theme_paper() +
    theme(
        panel.border = element_rect(size=0.5),
        panel.grid = element_blank()
    )

ggsave("imgs/system.png", width = 4, height = 3)
 
