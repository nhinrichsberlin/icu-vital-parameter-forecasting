#' Checks whether any targets contain missing data
#' @param data A dataframe
.check_for_missing_targets <- function(data) {
  # check for missing values among targets
  missing <- purrr::map(data, ~sum(is.na(.)))
  
  if (any(missing[config$targets] > 0)) {
    stop("Targets contain missing values.")
  }

}


#' Reads a folder containing parquet files
#' @param data_directory The path to the parquet files
.read_data_from_parquet <- function(data_directory) {
  
  data <-
    data_directory %>%
    arrow::open_dataset() %>%
    arrow::Scanner$create() 
  
  data <- 
    data$ToTable() %>% 
    dplyr::as_tibble() %>% 
    dplyr::arrange(case_id, datetime)
  
  # Set the timezone to CET
  data <- 
    data %>% 
    dplyr::mutate(datetime = lubridate::as_datetime(datetime, tz = "CET"))
  
  # If needed, drop the dask_index
  if ("__null_dask_index__" %in% names(data)) {
    
    data <- 
      data %>%
      dplyr::select(-"__null_dask_index__")
    
  }
  
  return(data)
  
}

#' Flips the dataframe from wide to long format
#' @param data A dataframe
#' @param targets A list of targets to consider
#' @return A dataframe in the long format
.wide_to_long <- function(data, targets) {
  data_long <- 
    data %>% 
    tidyr::pivot_longer(cols = dplyr::all_of(targets),
                        names_to = "target_type",
                        values_to = "target_value") %>% 
    dplyr::select(case_id, datetime, minutes_elapsed, target_type, target_value) %>% 
    dplyr::arrange(case_id, datetime)
  
  imputation <- 
    data %>% 
    tidyr::pivot_longer(cols = dplyr::all_of(paste0(targets, "__NA")),
                        names_to = "target_type",
                        values_to = "target_imputed") %>% 
    dplyr::select(case_id, datetime, minutes_elapsed, target_type, target_imputed) %>% 
    dplyr::mutate(target_type = gsub("__NA", "", target_type)) %>% 
    dplyr::arrange(case_id, datetime)
  
  # add info on imputed targets to the data
  data_long <- 
    data_long %>% 
    dplyr::left_join(imputation, by = c("case_id", "datetime", "target_type", "minutes_elapsed"))
    

  # check for missing values among targets
  missing <- purrr::map(data_long, ~sum(is.na(.)))
  
  if (missing$target_value > 0) {
    stop("Targets contain missing values.")
  }
  
  return(data_long)
  
}


#' Adds a column indicating the model_names to be used in forecasting
#' @param data A dataframe
#' @param model_names A list of models to use
#' @return A dataframe
.add_model_name_col <- function(data, model_names) {
  
  models <- dplyr::tibble(model_name = model_names)
  
  data <- 
    data %>% 
    dplyr::left_join(models, by = character())
  
}


#' Nests the data for forecasting
#' @param data A dataframe
#' @param forecast_freq The frequency at which to produce forecasts
#' @param nested_data_name The name of the column containing the past data 
#' @return A nested dataframe
.nest_time_series <- function(data,
                              forecast_freq,
                              nested_data_name = "target_ts") {
  
  # per id, get the largest minutes elapsed
  min_elapsed_max <-
    data %>% 
    dplyr::select(case_id, minutes_elapsed) %>% 
    dplyr::group_by(case_id) %>% 
    dplyr::summarise(minutes_elapsed_max = max(minutes_elapsed),
                     .groups = "keep")
  
  # define a grid of points in time per case and 
  # target for which we need a forecast
  grid <- 
    data %>% 
    dplyr::select(case_id, target_type, minutes_elapsed, datetime) %>% 
    # join the min_elapsed_max for filtering
    dplyr::left_join(min_elapsed_max, by = c("case_id")) %>% 
    # filter by forecast frequency
    dplyr::filter(minutes_elapsed %% forecast_freq == 0) %>%
    # filter out "early" minutes elapsed where we have too few obs. to forecast
    dplyr::filter(minutes_elapsed > 20) %>% 
    # filter out the last minutes elapsed where a forecast makes no sense
    # because we won't have any true values to compare
    dplyr::filter(minutes_elapsed < minutes_elapsed_max) %>% 
    # some cosmetic changes
    dplyr::rename(datetime_at_forecast = datetime) %>% 
    dplyr::select(-c(minutes_elapsed, minutes_elapsed_max))
  
  # left join this grid onto the data
  data_nested <-
    data %>% 
    dplyr::nest_by(case_id, target_type, .key = nested_data_name) %>% 
    dplyr::left_join(grid, by = c("case_id", "target_type"))
  
  # filter such that per grid-point, we nest the 
  # available training data at this point
  data_nested <-
    data_nested %>% 
    tidyr::unnest(cols = all_of(nested_data_name)) %>% 
    dplyr::ungroup() %>% 
    dplyr::filter(datetime <= datetime_at_forecast) %>% 
    dplyr::group_by(case_id, target_type, datetime_at_forecast) %>% 
    dplyr::summarise(target_ts = list(target_value), 
                     timestamps = list(datetime), 
                     .groups = "keep") %>% 
    # add the target value at the time of the forecast
    dplyr::mutate(target_value_at_forecast = purrr::map_dbl(target_ts, ~dplyr::last(.))) %>% 
    dplyr::ungroup()

}


#' Loads data from parquet files to tibble
#' @param path_to_file Path to the parquet file
#' @param targets The desired targets to be included
#' @return A dataframe
.load_long_data <- function(path_to_file,
                            targets) {
  
  columns_to_keep <- c(c("patient_id",
                         "case_id",
                         "datetime",
                         "minutes_elapsed"),
                       targets,
                       paste0(targets, "__NA"))
  
  data <- 
    path_to_file %>% 
    arrow::read_parquet(dplyr::all_of(columns_to_keep))
  
  .check_for_missing_targets(data)
  
  data <- 
    data %>% 
    .wide_to_long(targets)
  
  case_id <- 
    data %>% 
    purrr::pluck("case_id") %>% 
    unique()
  
  message("\t", Sys.time(), " -- Loaded case ", case_id)
  
  return(data)
  
}


#' Nests the forecast data, to produce row per
#' case_id, datetime_at_forecast, model_name 
#' @param data_long A dataframe containing past data in the long format
#' @param forecast_freq The desired frequency at which to forecast
#' @param ts_model_names A list of models to use for prediction
#' @return A nested dataframe
.nest_long_data <- function(data_long,
                            forecast_freq,
                            ts_model_names) {

  data <- 
    data_long %>% 
    .nest_time_series(forecast_freq) %>% 
    .add_model_name_col(model_names = ts_model_names) %>% 
    dplyr::arrange(case_id, target_type, datetime_at_forecast, model_name) %>% 
    # Put columns in nicer order
    dplyr::select(case_id, target_type, datetime_at_forecast,
                  model_name, target_ts, timestamps, target_value_at_forecast) 
  
}


#' Turns a dataframe in the long format containing past data
#' into a nested dataframe on which forecast models can be applied per row
#' @param data_long A dataframe in the long format
#' @param forecast_freq The desired frequency at which to forecast
#' @param ts_model_names A list of time series models to use
#' @param bounds A dataframe indicating per target type the 
#' min/max allowed forecasted value
.load_nested_data <- function(data_long,
                              forecast_freq,
                              ts_model_names,
                              bounds) {
  
  data_nested <- 
    data_long %>% 
    .nest_long_data(forecast_freq = forecast_freq,
                    ts_model_names = ts_model_names) %>% 
    # add the upper and lower bounds
    dplyr::left_join(bounds, by = "target_type") %>% 
    # add the time of the first observation
    dplyr::left_join(data_long %>% 
                       dplyr::group_by(case_id) %>% 
                       dplyr::summarise(datetime_first_obs = min(datetime),
                                        .groups = "keep"),
                     by = c("case_id")) %>% 
    # add some noise to constant time-series for convenience
    dplyr::mutate(target_ts = purrr::map(target_ts,
                                         .add_noise_to_constant_ts)) %>% 
    # add the in-sample random walk forecast MAE for scaling
    dplyr::mutate(in_sample_rw_mae = purrr::map(target_ts,
                                                .in_sample_rw_mae) %>%
                                      as.double())
  
  
    if(!data_nested$in_sample_rw_mae %>% min() > 0) {
      stop("Something wrong with the in-sample random walk MAE!")
    }
  
  return(data_nested)
  
}
