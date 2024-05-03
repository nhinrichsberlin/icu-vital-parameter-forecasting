source("R/setup.R")

# read the configuration file
config <- yaml::read_yaml('config/config.yaml')

# check that all forecasts exist as expected
.check_expected_vs_forecasted(datasets = c("test", "eicu"))

# store error metrics without grouping by hours_since_admission
.store_error_metrics(targets = config$targets,
                     models = c(config$ts_models, 
                                "blend",
                                config$ml_models),
                     by_hours_since_admission = FALSE)

# store error metrics with grouping by hours_since_admission
.store_error_metrics(targets = config$targets,
                     models = c(config$ts_models, 
                                "blend",
                                config$ml_models),
                     by_hours_since_admission = TRUE)
