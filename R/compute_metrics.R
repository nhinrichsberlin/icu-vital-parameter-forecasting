source("R/setup.R")

# read the configuration file
config <- yaml::read_yaml('config/config.yaml')

# check that all forecasts exist as expected
.check_expected_vs_forecasted(datasets = c("test", "eicu"))

# store error metrics
.store_error_metrics(targets = config$targets,
                     models = c(config$ts_models, "blend", "gru", "transformer"))
