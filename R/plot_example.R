source("R/setup.R")
source("R/shinyforecasts/plots.R")

config <- yaml::read_yaml("config/config.yaml")
dataset <- "test"
case_id <- "1111d613bcca4678b40f0d8043e10a01e7705cf5dd4df180397b44d09228dbae"
case_pattern <- paste0("case_", case_id, ".parquet$")

univ_folder <- paste0("data/predictions/", dataset, "_predictions_univariate/")
ml_folder <- paste0("data/predictions/", dataset, "_predictions_ml/")

case_files <- c(
  paste0(univ_folder, list.files(univ_folder) %>% stringr::str_subset(case_pattern)),
  paste0(ml_folder, list.files(ml_folder) %>% stringr::str_subset(case_pattern))
  
)

case_data <-
  case_files %>% 
  purrr::map(~arrow::read_parquet(.)) %>% 
  dplyr::bind_rows()

models_to_plot <- c("naive", "arima_auto", "gru", "transformer")

# times as they really are
datetime_forecast <- lubridate::as_datetime("2019-11-09 05:12:00", tz = "Europe/Berlin")
start <- lubridate::as_datetime("2019-11-08 22:12:00", tz = "Europe/Berlin")
end <- lubridate::as_datetime("2019-11-09 07:12:00", tz = "Europe/Berlin")

# convert times for anonymity
shift <- lubridate::minutes(48)
datetime_forecast <- datetime_forecast + shift
start <- start + shift
end <- end + shift
case_data <-
  case_data %>% 
  dplyr::mutate(datetime_at_forecast = datetime_at_forecast + shift,
                datetime = datetime + shift)

case_data <-
  case_data %>% 
  dplyr::filter(datetime >= start,
                datetime <= end) %>% 
  dplyr::filter(model_name %in% models_to_plot)

case_data_true <-
  case_data %>% 
  dplyr::group_by(datetime, target_type) %>% 
  dplyr::summarise(target_value = median(target_value), .groups = "keep")

case_data_forecasts <-
  case_data %>% 
  dplyr::filter(datetime_at_forecast == datetime_forecast)

ylims <-
  case_data %>% 
  dplyr::group_by(target_type) %>% 
  dplyr::summarise(min_true = min(target_value),
                   max_true = max(target_value),
                   min_forecast = min(prediction),
                   max_forecast = max(prediction),
                   .groups = "keep")

col_map <- 
  config 
  purrr::pluck("ts_model_colours") %>% 
  unlist()

model_name_map <-
  config %>% 
  purrr::pluck("model_name_labels") %>% 
  unlist()


.vitals_plots(data_true = case_data_true,
              targets = c("blood_pressure_systolic",
                          "blood_pressure_diastolic",
                          "blood_pressure_mean",
                          "central_venous_pressure",
                          "oxygen_saturation",
                          "heart_rate"),
              data_forecasts = case_data_forecasts,
              forecast_time = datetime_forecast,
              ylims_per_target_type = ylims,
              legend_position = c(1, 1.1),
              legend_direction = "horizontal",
              output_file = "evaluation/plots/performance/example.png",
              linewidth = 0.5,
              pointsize = 0.75,
              nrow = 3,
              ncol = 2)
