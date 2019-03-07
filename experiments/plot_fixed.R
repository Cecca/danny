library(tidyverse)
source("experiment.R")

data <- load.table("../results.json","result") %>%
  filter(algorithm == 'fixed-lsh') %>%
  mutate(sketch_bits = ifelse(is.na(sketch_bits), "no-sketch", sketch_bits)) %>%
  mutate(input = basename(left_path)) %>%
  mutate(pairs_per_second = output_size / (total_time_ms / 1000))

  
# Plot recall against k
data %>%
  ggplot(aes(x=factor(k), y=recall, color=factor(sketch_bits))) +
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
               color=factor(k), shape=factor(sketch_bits))) +
    geom_point(stat = 'summary') +
    scale_y_log10() +
    ggtitle(paste('Time vs. recall', dataset)) +
    theme_bw()
  ggsave(paste("recall-time-", dataset, ".png"), width=6, height=4)
}