library(tidyverse)
source("experiment.R")

data <- load.table("results.json","result") %>%
  filter(algorithm == 'fixed-lsh') %>%
  # mutate(sketch_bits = ifelse(is.na(sketch_bits), "no-sketch", sketch_bits)) %>%
  mutate(input = basename(left_path)) %>%
  mutate(pairs_per_second = output_size / (total_time_ms / 1000))

# Plot recall against k
data %>%
  ggplot(aes(x=factor(k), y=recall)) +
  geom_boxplot() +
  facet_grid(~input, scales = 'free_x') +
  theme_bw()

ggsave("recall.png", width = 7, height = 4)

# Plot time versus recall 
datasets <- data %>% select(input) %>% distinct() %>% pull()
for (dataset in datasets) {
  print(dataset)
  data %>%
    filter(input == dataset) %>%
    ggplot(aes(x=recall, y=pairs_per_second, 
               color=factor(k))) +
    geom_point(stat = 'summary') +
    scale_y_log10() +
    xlim(0,1) +
    ggtitle(paste('Throughput vs. recall', dataset)) +
    theme_bw()
  ggsave(paste("recall-throughput-", dataset, ".png"), width=6, height=4)
}
for (dataset in datasets) {
  print(dataset)
  data %>%
    filter(input == dataset) %>%
    ggplot(aes(x=recall, y=time, 
               color=factor(k))) +
    geom_point(stat = 'summary') +
    scale_y_log10() +
    xlim(0,1) +
    ggtitle(paste('Time vs. recall', dataset)) +
    theme_bw()
  ggsave(paste("recall-Time-", dataset, ".png"), width=6, height=4)
}
