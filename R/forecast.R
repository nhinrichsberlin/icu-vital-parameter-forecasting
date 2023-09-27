#' Produces forecasts from fitted forecast objects
#' @param forecast_obj A fitted forecast object from the forecast package
#' @param lambda The applied box-cox transformation parameter
#' @param bias_correction Whether or not to use bias correction when doing
#' box-cox re-transformation 
#' @param forecast_produced_at A timestamp indicating the forecast time
#' @param lower_bound The lowest allowed forecast value
#' @param upper_bound The largest allowed forecast value
#' @return A two-column dataframe: predicted values, their respective timestamps
.format_forecast <- function(forecast_obj,
                             lambda,
                             bias_correction,
                             forecast_produced_at,
                             lower_bound,
                             upper_bound) {

  # extract the point forecasts from the forecast object
  y_hat <- 
    forecast_obj %>% 
    purrr::pluck("mean") %>% 
    as.double()
  
  if (! lambda %>% is.null()) {
    
    # we need an estimate of forecast variance for bias correction.
    # can either be obtained from confidence intervals
    # or by estimating the variance of the residuals
    if (all(c("level", "lower", "upper") %in% names(forecast_obj))) {
      fvar <- forecast_obj
    } else if ("residuals" %in% names(forecast_obj)) {
      fvar <- var(forecast_obj$residuals, na.rm = TRUE)
    } else {
      # if neither is available, proceed without biasadjustments
      fvar <- NULL
      bias_correction <- FALSE
    }
    
    y_hat <- InvBoxCox(y_hat, lambda, biasadj = bias_correction, fvar = fvar)
    
  }
  
  # handle missing values
  if (y_hat %>% is.na() %>% any()| y_hat %>% identical(numeric(0))) {
    
    if (y_hat %>% is.na() %>% all() | y_hat %>% identical(numeric(0))) {
      stop("All forecasts are NA.")
    }
    
    y_hat <-
      y_hat %>% 
      .fill_na_forecasts()
  }
  
  # clip the forecast
  y_hat <- pmax(lower_bound, pmin(upper_bound, y_hat))
  
  
  # just to be sure...
  if (y_hat %>% anyNA()) {
    stop("Forecast still contains NAs.")
  }
  
  # create a DF with two rows: prediction and corresponding datetime
  predictions <- 
    y_hat %>% 
    .make_prediction_tibble(forecast_produced_at)
  
  return(predictions)
  
}


#' A wrapper for the .format_forecast function
.safely_format_forecast <- purrr::safely(.format_forecast)


#' Produces forecasts from fitted forecast objects and returns nice
#' forecast dataframes of predictions and timestamps
#' @param forecast_obj A fitted forecast object from the forecast package
#' @param lambda The applied box-cox transformation parameter
#' @param bias_correction Whether or not to use bias correction when doing
#' box-cox re-transformation 
#' @param forecast_produced_at A timestamp indicating the forecast time
#' @param lower_bound The lowest allowed forecast value
#' @param upper_bound The largest allowed forecast value
#' @return A two-column dataframe: predicted values, their respective timestamps
.transform_forecast <- function(forecast_obj,
                                lambda,
                                bias_correction,
                                forecast_produced_at,
                                lower_bound,
                                upper_bound) {
  
  
  forecast <- .safely_format_forecast(forecast_obj,
                                      lambda,
                                      bias_correction,
                                      forecast_produced_at,
                                      lower_bound,
                                      upper_bound)
  
  # use the naive model as default if something goes wrong
  if (forecast$error %>% is.null()) {
    result <- list(y_hat = forecast$result,
                   success = TRUE,
                   error = NA)
    
  } else {
    
    last_obs <- 
      forecast_obj %>% 
      purrr::pluck("x") %>% 
      tail(1)
    
    horizon <-
      forecast_obj %>% 
      purrr::pluck("mean") %>% 
      length()
   
   y_hat <- .make_prediction_tibble(rep(last_obs, horizon), forecast_produced_at)
   
    result <- list(y_hat = y_hat,
                   success = FALSE,
                   error = forecast$error)
    
  }
  
  return(result)
}


#' A wrapper for multiple forecast models
#' @param vitals_ts A time series
#' @param timestamps The respective time stamps
#' @param model_name The name of the forecast model
#' @param horizon The number of steps to forecast
.fit_forecast_model <- function(vitals_ts, timestamps, model_name, horizon) {
  
  if(model_name == "naive") {
    
    model <- forecast::naive(vitals_ts, h = horizon)
    
  } else if (model_name == "arima_auto") {
    
    model <- forecast::auto.arima(y = vitals_ts,
                                  lambda = NULL,
                                  D = 0,
                                  max.d = 1,
                                  max.p = 3,
                                  max.q = 3,
                                  max.order = 4,
                                  seasonal = FALSE,
                                  stepwise = FALSE,
                                  method = "ML")

  } else if(model_name == "nnet_ar") {
    
    # build exponentially rising weights
    # to put more emphasis on recent observations
    p <- min(length(vitals_ts) - 1, 6)
    weights <- exp(seq(-3, 0, length.out = length(vitals_ts) - p))
    
    model <- forecast::nnetar(y = vitals_ts,
                              p = p,
                              size = 18,
                              weights = weights,
                              repeats = 5,
                              decay = 2,
                              lambda = NULL,
                              P = 0)

  } else if(model_name == "ses") {
    
    model <- forecast::ets(y = vitals_ts,
                           model = "ANN")
    
  } else if(model_name == "holt") {
    
    model <- forecast::ets(y = vitals_ts,
                           model = "AAN",
                           damped = FALSE)
    
  } else if(model_name == "holt_damped") {
    
    model <- forecast::ets(y = vitals_ts,
                           model = "AAN",
                           damped = TRUE)
    
  } else if(model_name == "theta") {
    
    model <- forecast::thetaf(y = vitals_ts,
                              h = horizon)
    
  } else if(model_name == "theta_unseasonal") {
    
    model <- .theta_unseasonal(y = vitals_ts,
                               h = horizon)
    
  } else if(model_name == "es_auto") {
    
    model <- forecast::ets(y = vitals_ts,
                           model = "ZZN",
                           lambda = NULL,
                           damped = NULL,
                           allow.multiplicative.trend = TRUE)
    
  } else {
    
    stop(paste0("Invalid model_name: ", model_name))
    
 }
  
  return(model)
  
}


#' Fits a forecast model with a time constraint
#' @param vitals_ts A time series
#' @param timestamps The respective time stamps
#' @param model_name The name of the forecast model
#' @param horizon The number of steps to forecast
#' @param timeout_seconds The max allowed time per forecast
.fit_forecast_model_with_timeout <- function(vitals_ts,
                                             timestamps,
                                             model_name,
                                             horizon,
                                             timeout_seconds) {
 
  model <- R.utils::withTimeout(.fit_forecast_model(vitals_ts, 
                                                    timestamps,
                                                    model_name,
                                                    horizon),
                                 timeout = timeout_seconds,
                                 onTimeout = "error")
  
  return(model)
   
}


#' A wrapper of the .fit_forecast_model-with_timeout function
.safely_fit_forecast_model <- purrr::safely(.fit_forecast_model_with_timeout)


#' Fits a forecast model
#' @param vitals_ts A time series
#' @param timestamps The respective time stamps
#' @param model_name The name of the forecast model
#' @param horizon The number of steps to forecast
#' @param timeout_seconds The max allowed time per forecast
.fit_model <- function(vitals_ts,
                       timestamps,
                       model_name, 
                       horizon,
                       timeout_seconds) {
  
  time_start <- Sys.time()
  
  model <- .safely_fit_forecast_model(vitals_ts,
                                      timestamps,
                                      model_name,
                                      horizon,
                                      timeout_seconds)

  seconds_elapsed <- difftime(Sys.time(),
                              time_start,
                              units = 'secs')[[1]]

  # use the naive model as default if something goes wrong
  if (model$error %>% is.null()) {
    
    result <- list(model = model$result,
                   success = TRUE,
                   error = NA,
                   seconds_to_fit = seconds_elapsed)
    
  } else {
    
    result <- list(model = .fit_forecast_model(vitals_ts,
                                               timestamps,
                                               "naive",
                                               horizon),
                   success = FALSE,
                   error = model$error,
                   seconds_to_fit = seconds_elapsed)

  }
  
  return(result)
  
}


#' Makes predictions given a time series of past data
#' @param vitals_ts A time series
#' @param timestamps The respective time stamps
#' @param model_name The name of the forecast model
#' @param current_time The time of the forecast
#' @param lower_bound The smallest allowed forecast value
#' @param upper_bound The largest allowed forecast value
#' @param horizon The number of steps to forecast
#' @param timeout_seconds The max allowed time per forecast
#' @param bias_correction Bias correction upon box-cox re-transformation?
.make_predictions <- function(vitals_ts,
                              timestamps,
                              model_name,
                              current_time,
                              lower_bound,
                              upper_bound,
                              horizon,
                              timeout_seconds = 10,
                              bias_correction = FALSE) {
  
  # Box-Cox transform the vitals_ts, if appropriate
  lambda <- .lambdaBoxCox(vitals_ts)
  
  if (! lambda %>% is.null()) {
    
    vitals_ts <- BoxCox(vitals_ts, lambda)
    
  }
  
  # fit a forecast model
  model_fit <- .fit_model(vitals_ts, 
                          timestamps,
                          model_name,
                          horizon,
                          timeout_seconds)
  
  # extract the model object
  model <- model_fit$model
  
  if (model %>% smooth::is.smooth()) {
    
    model <- greybox::forecast(model, h = horizon)
    
    
  } else if (model_name %>% startsWith("fbprophet") & 
             (! model %>% forecast::is.forecast())) {
    
    model <- .standardize_prophet_model(model, current_time, horizon)

  } else if (! model %>% forecast::is.forecast()) {
    
    model <- forecast::forecast(model, h = horizon)
  
  }
  
  # produce forecasts
  forecast <- model %>% .transform_forecast(lambda, 
                                            bias_correction,
                                            current_time, 
                                            lower_bound, 
                                            upper_bound)
  
  result <- list(predictions = forecast$y_hat,
                 lambda = lambda,
                 model_fit_successful = model_fit$success,
                 model_fit_error = model_fit$error,
                 seconds_to_fit = model_fit$seconds_to_fit,
                 model_prediction_successful = forecast$success,
                 model_prediction_error = forecast$error,
                 done_at = Sys.time())
  
  return(result)
  
}



#' Produces a forecast per time series
#' @param data_nested A nested dataframe
#' @param horizon The forecast horizon (time steps ahead)
#' @param cluster A multidplyr forecast cluster
#' @return A dataframe containing the forecasts
.run_forecasts <- function(data_nested,
                           horizon,
                           cluster = NULL) {
  
  # activate parallel computing, if needed
  if (!is.null(cluster)) {
    message("\t", Sys.time(), " -- Partitioning the data for multiprocessing.")
    data_nested <- 
      data_nested %>% 
      dplyr::group_by(case_id, target_type, datetime_at_forecast) %>% 
      multidplyr::partition(cluster = cluster)
    
  }
  
  message("\t", Sys.time(), " -- ", nrow(data_nested), " forecasts to go!")
  
  # run forecasts
  data_nested <- 
    data_nested %>% 
    dplyr::mutate(horizon = horizon) %>% 
    dplyr::mutate(prediction_results = purrr::pmap(list(target_ts,
                                                        timestamps,
                                                        model_name,
                                                        datetime_at_forecast,
                                                        lower_bound,
                                                        upper_bound,
                                                        horizon),
                                                   .make_predictions)) %>% 
    dplyr::collect() %>% 
    dplyr::ungroup() %>% 
    tidyr::unnest_wider(prediction_results) 
  
  message("\t", Sys.time(), " -- All models done.")
  
  return(data_nested)
  
}


.ts_models_to_use <- function(all_models, existing_models, re_do) {
  # return models specifically told to re-do
  # plus models defined in all_models that are not already there
  missing_models <- setdiff(all_models, existing_models)
  models_to_compute <- c(re_do, missing_models)
  return(unique(missing_models))
}


.handle_existing_forecasts <- function(input_folder, output_folder, re_do, all_models) {
  
  # list all files contained in the input folder
  input_files <- list.files(input_folder, pattern = ".parquet")
  
  # list already existing output files, cases and models
  output_files <- list.files(output_folder, pattern = ".parquet")
  existing_models <- purrr::map_chr(output_files, ~stringr::str_split_fixed(., "__", 2)[,1])
  existing_cases <- purrr::map_chr(output_files, ~stringr::str_split_fixed(., "__", 2)[,2])
   
  # in the end, the cases in the input folder and output folder need to match
  # drop output_files referring to cases not present in input_files 
  # (can happen if train/val/test data changes)
  cases_to_remove <- setdiff(existing_cases, input_files)
  message("Removing ", length(cases_to_remove), " cases in the output_folder not present in the input_folder.")
  files_to_remove <- output_files[
    purrr::map_lgl(output_files, ~str_contains(., pattern = cases_to_remove, logic = "OR"))
  ]
  # drop files
  purrr::map(paste0(output_folder, "/", files_to_remove), unlink)
  
  # drop forecasts from models in re_do
  # make sure to also always remove the blend
  if (!is.null(re_do)) {
    re_do <- c(re_do, "blend")
  }
  files_to_remove <- output_files[
    purrr::map_lgl(output_files, ~str_contains(., pattern = re_do, logic = "OR"))
  ]
  message("Removing ", length(files_to_remove),
          " files containing the following models because of re-do: ",
          paste0(re_do, collapse = ", "))
  purrr::map(paste0(output_folder, "/", files_to_remove), unlink)
  
  # drop forecasts from cases not listed in all_models
  models_not_required <- setdiff(existing_models, c(all_models, "blend"))
  files_to_remove <- output_files[
    purrr::map_lgl(output_files, ~str_contains(., pattern = models_not_required, logic = "OR"))
  ]
  message("Removing ", length(files_to_remove),
          " files containing the following models because not in list of ts_models: ",
          paste0(models_not_required, collapse = ", "))
  purrr::map(paste0(output_folder, "/", files_to_remove), unlink)

}


#' Executes the entire forecasting pipeline
#' @param dataset A string indicating for which data set
#' forecasts are required (train, validate, or test)
#' @param config A named list containing configurations
#' @param n_cores The number of CPU cores to use for forecasting
#' @param re_do List of forecast models to re-do
run_forecast_pipeline <- function(dataset,
                                  config,
                                  n_cores = NULL,
                                  re_do = NULL) {
  
  if (!all(re_do %in% config$ts_models)) {
    stop("re_do must be a sub-set of the ts_models!")
  }
  
  # define the folder containing the input data
  input_folder <- 
    paste0("data/processed/vitals_",
           dataset)
  
  # define the output folder
  output_folder <- 
    paste0("data/predictions/",
           dataset,
           "_predictions_univariate")
  
  # take care of previously produced forecasts
  .handle_existing_forecasts(input_folder, output_folder, re_do, config$ts_models)
  
  # load the cutoff-values defined through the training data
  bounds <- .get_forecast_bounds(config)
  
  # define the forecast horizon
  horizon <- config$forecast_horizon
  
  # define models to use in the blended forecast
  models_to_blend <- config$ts_models[
    !(config$ts_models %in% config$ts_models_exclude_from_blend)
  ]
  
  # decide on the number of cpus to use
  if (n_cores %>% is.null()) {
    n_cores <- parallel::detectCores() - 4
  }
  message(paste0("Using ", n_cores, " CPUs for forecasting."))
  
  # create a forecast cluster for parallel processing
  forecast_cluster <- .build_forecast_cluster(n_cores)
  
  i <- 1
  # run the forecast pipeline per file
  input_files <- list.files(input_folder, pattern = ".parquet")
  for (file in input_files) {

    start_time <- Sys.time()
    
    message("\n", Sys.time(), " -- ", dataset, " case ", i, " / ", length(input_files))
    
    # check which forecasts already exist for this case
    existing_models <-
      list.files(output_folder, pattern = file) %>% 
      purrr::map_chr(~stringr::str_split_fixed(., pattern = "__", 2)[, 1]) %>% 
      unique()
    
    ts_models <- .ts_models_to_use(config$ts_models, existing_models, re_do)
    message("\t", Sys.time(), " -- Forecast models: ", paste0(ts_models, collapse = ", "))
    
    # skip this case if all forecasts are already there as needed
    if (length(ts_models) == 0 & "blend" %in% existing_models) {
      
      message("\t", Sys.time(), " -- Already complete, skipping!")
      i <- i + 1
      
      next
    }
    
    # run needed ts_models
    if (length(ts_models) > 0) {
    
      # rebuild the cluster after 50 cases (don't know why, but I have noticed
      # it prevents the execution from getting stuck)
      if (i %% 50 == 0) {
       forecast_cluster <- .build_forecast_cluster(n_cores)
      }
      
      # load the data in the long format
      data_long <- .load_long_data(path_to_file = paste0(input_folder, "/", file),
                                   targets = config$targets)
      
       # nest the data
      data_nested <- 
        data_long %>% 
        .load_nested_data(forecast_freq = config$forecast_frequency,
                          ts_model_names = dplyr::all_of(ts_models),
                          bounds = bounds)
      
      # run the different forecast models
      data_nested <-
        data_nested %>% 
        .run_forecasts(horizon = horizon, 
                       cluster = forecast_cluster)
      
      # check whether all models ran successfully
      model_success <-
        data_nested %>% 
        .get_model_success_rates()
      
      # report if there are any problems
      if (mean(model_success$success_rate_model_fit) < 1 | 
          mean(model_success$success_rate_model_predictions) < 1) {
        
        message("\t", Sys.time(), " -- Not all forecasts successful for case", unique(data_nested$case_id))
        print(model_success)
        
      }
      
      # build a prediction data-frame containing forecasts and predictions per model
      predictions <-
        data_nested %>% 
        .unnested_prediction_df(data_long = data_long)
      
      # write the predictions to a parquet files, one per model
      predictions %>% 
        dplyr::select(!!!rlang::syms(config$needed_output_columns)) %>% 
        .case_to_parquet(output_folder)
      message("\t", Sys.time(), " -- Model outputs written to parquet.")
    
    }
    
    # blend the desired models
    # we load them from parquet, because we might blend
    # some models that were already there and not produced in this run
    predictions_blended <-
      paste0(output_folder, "/", list.files(output_folder, pattern = file)) %>% 
      purrr::map(~arrow::read_parquet(.)) %>% 
      dplyr::bind_rows() %>% 
      .blend_forecasts(blend_name = "blend",
                       model_names = models_to_blend)
    
    predictions_blended %>% 
      dplyr::select(!!!rlang::syms(config$needed_output_columns)) %>% 
      .case_to_parquet(output_folder)
      message("\t", Sys.time(), " -- Blend written to parquet.")
    
    message(
      paste0("\tTime: ",
             round(difftime(Sys.time(), start_time, units = "mins"), 3),
             " minutes \n")
    )
    
    i <- i + 1
    
    # some cleanup, otherwise parallelisation does not work properly
    rm(predictions, predictions_blended, data_nested, data_long)
    
  }
  
}
