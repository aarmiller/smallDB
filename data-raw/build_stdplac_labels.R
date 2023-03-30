

library(tidyverse)
stdplac_labels <- read_csv("~/Data/MarketScan/labels/stdplac_labels.csv") %>% 
  rename(label = stdplac_label)

usethis::use_data(stdplac_labels, name = stdplac_labels, overwrite = TRUE)
