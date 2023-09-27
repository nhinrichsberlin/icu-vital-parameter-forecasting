source("R/setup.R")

t1_mlife_eicu() %>%
  readr::write_delim("evaluation/tables/table1.csv",
                     delim = ";")