source("R/setup.R")

# read the configuration file
config <- yaml::read_yaml('config/config.yaml')
  
# load data from csv (produced by R/compute_metrics.R)
file <- "evaluation/tables/error_metrics.csv"
error_metrics <- readr::read_delim(file,
                                   delim = ";",
                                   show_col_types = FALSE)

file <- "evaluation/tables/mape_across_all.csv"
mape_across_all <- readr::read_delim(file,
                                     delim = ";",
                                     show_col_types = FALSE)

file <- "evaluation/tables/mape_across_all_by_hours_since_admission.csv"
mape_across_all_by_hours_since_admission <- readr::read_delim(file,
                                                              delim = ";",
                                                              show_col_types = FALSE)

# do the plotting
for (d in c("test", "eicu")) {
  
  .plot_metrics_all_models(errors_df = error_metrics, 
                           eicu_or_test = d)
  
  .plot_mape_across_all(mape_df = mape_across_all, 
                        eicu_or_test = d)
  
  .plot_mape_across_all_by_hour_since_admission(mape_df_by_hour = mape_across_all_by_hours_since_admission,
                                                eicu_or_test = d) 
  
}
