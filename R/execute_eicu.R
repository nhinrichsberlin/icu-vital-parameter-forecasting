source("R/setup.R")

# load configurations
config <- yaml::read_yaml('config/config.yaml')

# execute the forecast pipeline on the external EICU dataset
forecast_results <- run_forecast_pipeline(dataset = "eicu",
                                          config = config,
                                          re_do = c())
