source("R/setup.R")

# read the configuration file
config <- yaml::read_yaml('config/config.yaml')
  
# load data from csv (produced by R/compute_metrics.R)
file <- "evaluation/tables/error_metrics.csv"
error_metrics <- readr::read_delim(file,
                                   delim = ";",
                                   show_col_types = FALSE)

# do the plotting
for (d in c("test", "eicu")) {
  
  .plot_metrics_all_models(errors_df = error_metrics, 
                           eicu_or_test = d)
  
  .plot_metric_vs_naive_metric(errors_df = error_metrics,
                               eicu_or_test = d)
  
}
