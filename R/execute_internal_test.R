source("R/setup.R")

# load configurations
config <- yaml::read_yaml('config/config.yaml')

# execute the forecast pipeline on the internal test set
forecast_results <- run_forecast_pipeline(dataset = "test",
                                          config=config,
                                          re_do = c())
