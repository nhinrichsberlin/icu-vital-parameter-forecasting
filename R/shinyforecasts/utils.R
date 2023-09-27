#' Loads predictions from univariate and ML models
#' from directories containing parquet files
#' and adds some useful columns
#' @param cases_per_dataset The number of cases per dataset (internal/eicu) to display
#' @return A beautiful dataframe containing forecasts from different models.
.load_predictions <- function(cases_per_dataset = NULL){
  
  message("Loading predictions.")
  
  # read univariate TS model forecasts and ML forecasts
  # from the internal test set
  data_internal <- dplyr::bind_rows(
    .read_data_from_parquet("data/predictions/test_predictions_univariate"),
    .read_data_from_parquet("data/predictions/test_predictions_ml")
  )
  message("Internal test done.")
  
  # if needed, downsample to keep only a sub-set of all cases
  # TODO: This could be done earlier to avoid loading too many cases
  if (!is.null(cases_per_dataset)) {
    data_internal <-
      data_internal %>% 
      dplyr::filter(case_id %in% sample(unique(data_internal$case_id), cases_per_dataset))
  }
  
  # from the external EICU dataset
  data_eicu <- dplyr::bind_rows(
    .read_data_from_parquet("data/predictions/eicu_predictions_univariate"),
    .read_data_from_parquet("data/predictions/eicu_predictions_ml")
  )
  message("EICU done.")
  
  # if needed, downsample to keep only a sub-set of all cases
  if (!is.null(cases_per_dataset)) {
    data_eicu <-
      data_eicu %>% 
      dplyr::filter(case_id %in% sample(unique(data_eicu$case_id), cases_per_dataset))
  }
  
  # add indicators
  data_internal <-
    data_internal %>% 
    dplyr::mutate(dataset_name = 'internal') %>% 
    dplyr::mutate(case_id = paste0('internal__', case_id))
  
  data_eicu <- 
    data_eicu %>% 
    dplyr::mutate(dataset_name = 'eicu') %>% 
    dplyr::mutate(case_id = paste0('eicu__', case_id))
  
  # combine the internal and external data 
  data <- 
    dplyr::bind_rows(data_internal, data_eicu)
  message("EICU and internal test set combined.")
  
  # add a column indicating how far the forecast refers to the future
  data <- 
    data %>% 
    dplyr::mutate(minutes_ahead = (datetime - datetime_at_forecast) %>% 
                                    as.double(unit = "mins"))
  
  # add a column indicating the available training time in full hours
  data <- 
    data %>% 
    dplyr::mutate(full_hours_since_admission = 
                    (datetime_at_forecast - datetime_first_obs) %>% 
                    as.double(unit = "hours") %>% 
                    floor())
  message("Loading complete.")
  
  return(data)

}


#' Adds desired forecast accuracy metrics to a dataframe 
#' containing true values and predictions
#' @param forecast_data A dataframe containing true values and predictions
#' @return The same dataframe with additional columns
.add_metrics_to_df <- function(forecast_data) {
  
  forecast_data %>% 
    dplyr::summarise(
      mae = (target_value - prediction) %>% abs() %>% mean(),
      mape = ((target_value - prediction) / max(target_value, 1)) %>% abs() %>% mean(),
      rmse = (target_value - prediction) ** 2 %>% mean() %>% sqrt(),
      .groups = "keep"
    )
  
}


#' Calculates error metrics per 
#' forecast horizon, full hours since admission, target_type, and model
#' from a dataframe containing forecasts and true values
#' @param forecast_data A dataframe containing forecasts and true values
#' @return A dataframe of forecast error metrics
.calculate_error_metrics <- function(forecast_data,
                                     groupby = c("dataset_name",
                                                 "model_name",
                                                 "target_type",
                                                 "minutes_ahead",
                                                 "full_hours_since_admission")) {
  
  metrics <-
    forecast_data %>% 
    dplyr::group_by(!!!rlang::syms(groupby)) %>% 
    .add_metrics_to_df()

}

#' Returns per target type the largest error metrics
#' @param data_metrics A dataframe containing accuracy metrics
#' per model, forecast horizon, full hours since admission
#' @return A dataframe indicating the max metric per target type
.max_error_metrics_per_target <- function(data_metrics) {
  
  ymax_metrics_per_target_type <- 
    data_metrics %>% 
    dplyr::group_by(target_type) %>% 
    dplyr::mutate(max_mae = max(mae),
                  max_mape = max(mape),
                  max_rmse = max(rmse))
  
}


#' Returns a proper name for each target
#' @param target A variable name string
#' @return A nice target name..
.assign_target_name <- function(target) {
  
  names <- list(
    blood_pressure_systolic = "Systolic BP",
    blood_pressure_diastolic = "Diastolic BP",
    blood_pressure_mean = "Mean BP",
    oxygen_saturation = "Oxygen saturation",
    central_venous_pressure = "Central venous pressure",
    heart_rate = "Heart rate",
    pulse = "Pulse",
    body_temperature = "Body temperature"
  )
  
  names[[target]]
  
}


#' Returns a checkboxGroupInput for selecting time series models
#' @param input_id checkboxGroupInput inputId
#' @param ts_models A list of models to appear in the checkbox
#' @return A checkboxGroupInput
.ts_models_checkbox <- function(input_id,
                                ts_models) {
  
  model_colours <- yaml::read_yaml("config/config.yaml")$ts_model_colours
  colour_map <- 
    ts_models %>% 
    lapply(function(model) {
      shiny::tags$span(model, style = paste0("color: ", model_colours[[model]], ";
                                      font-weight: bold;"))
    })
  
  shiny::checkboxGroupInput(inputId = input_id,
                            label = "TS Models",
                            choiceValues = ts_models,
                            choiceNames = colour_map,
                            selected = "naive")
  
}
