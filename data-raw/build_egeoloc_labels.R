

library(tidyverse)
egeoloc_labels <- read_csv("~/Data/MarketScan/labels/egeoloc.csv")


usethis::use_data(egeoloc_labels, name = egeoloc_labels, overwrite = TRUE)


