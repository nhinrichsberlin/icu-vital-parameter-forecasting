#' The theta method as in the forecast package
#' but with seasonality forced to FALSE
#' @param y The time-series
#' @param h The forecast horizon
#' @param level Confidence levels for prediction intervals
#' @param fan If TRUE, level is set to seq(51,99,by=3). This is suitable for fan plots.
#' @param x Deprecated. Included for backwards compatibility.
#' @return An object of class "forecast"
.theta_unseasonal <- function (y, h, level = c(80, 95), fan = FALSE, x = y) {
  
  if (fan) {
    level <- seq(51, 99, by = 3)
  }
  else {
    if (min(level) > 0 && max(level) < 1) {
      level <- 100 * level
    }
    else if (min(level) < 0 || max(level) > 99.99) {
      stop("Confidence limit out of range")
    }
  }
  
  n <- length(x)
  x <- as.ts(x)
  m <- frequency(x)
  # here comes my adjustment
  seasonal <- FALSE
  origx <- x
  if (seasonal) {
    decomp <- decompose(x, type = "multiplicative")
    if (any(abs(seasonal(decomp)) < 1e-04)) {
      warning("Seasonal indexes close to zero. Using non-seasonal Theta method")
    }
    else {
      x <- seasadj(decomp)
    }
  }
  fcast <- ses(x, h = h)
  tmp2 <- lsfit(0:(n - 1), x)$coefficients[2]/2
  alpha <- pmax(1e-10, fcast$model$par["alpha"])
  fcast$mean <- fcast$mean + tmp2 * (0:(h - 1) + (1 - (1 - 
                                                         alpha)^n)/alpha)
  if (seasonal) {
    fcast$mean <- fcast$mean * rep(tail(decomp$seasonal, 
                                        m), trunc(1 + h/m))[1:h]
    fcast$fitted <- fcast$fitted * decomp$seasonal
  }
  fcast$residuals <- origx - fcast$fitted
  fcast.se <- sqrt(fcast$model$sigma2) * sqrt((0:(h - 1)) * 
                                                alpha^2 + 1)
  nconf <- length(level)
  fcast$lower <- fcast$upper <- ts(matrix(NA, nrow = h, ncol = nconf))
  tsp(fcast$lower) <- tsp(fcast$upper) <- tsp(fcast$mean)
  for (i in 1:nconf) {
    zt <- -qnorm(0.5 - level[i]/200)
    fcast$lower[, i] <- fcast$mean - zt * fcast.se
    fcast$upper[, i] <- fcast$mean + zt * fcast.se
  }
  fcast$x <- origx
  fcast$level <- level
  fcast$method <- "Theta"
  fcast$model <- list(alpha = alpha, drift = tmp2, sigma = fcast$model$sigma2)
  fcast$model$call <- match.call()
  return(fcast)
}


#' Forward-fills NA forecasts
#' @param y_hat A time series of forecasts
#' @return The time series without NAs
.fill_na_forecasts <- function(y_hat) {
  
  dplyr::tibble(y_hat = y_hat) %>% 
    tidyr::fill(y_hat, .direction = "downup") %>% 
    purrr::pluck("y_hat")
  
}


#' A wrapper for the BoxCox.lambda function
#' @param ts A time series
#' @return The ideal lambda transformation parameter
.lambdaBoxCox <- function(ts) {
  
  if (length(ts) <= 24) {
    
    lambda <- NULL
    
  }  else {
    
    lambda <- BoxCox.lambda(ts, lower = 0)
    
  }
  
  return(lambda)
  
}


#' Adds some noise to a constant time series
#' @param ts A time series
#' @return A no longer constant time series
.add_noise_to_constant_ts <- function(ts) {
  
  if(ts %>% forecast::is.constant()) {
    
    ts <- ts + rnorm(length(ts), 0, 0.005)
    
  }
  
  return(ts)
  
}


#' Returns a nice dataframe of forecasts and timestamps
#' @param y_hat A vector of forecasts
#' @param forecast_produced_at The time of the forecast
.make_prediction_tibble <- function(y_hat,
                                    forecast_produced_at) {
  
  # define the datetime index of the forecast
  # use dminutes to correctly handle DST
  start <- forecast_produced_at + lubridate::dminutes(5)
  
  datetime <- seq(start, by = "5 min", length.out = length(y_hat))
  
  # return a tibble
  prediction_tibble <-
    dplyr::tibble(prediction = y_hat,
                  datetime = datetime)
  
  return(prediction_tibble)
  
}


#' Computes the in-sample rolling window MAE for a given time series
#' @param ts A time series
.in_sample_rw_mae <- function(ts) {
  
  mae <- 
    (ts - lag(ts)) %>%
    abs() %>%
    mean(na.rm = TRUE)
  
}


#' Builds a multidplyr forecast cluster
#' @param n_cores The number of cores to use
.build_forecast_cluster <- function(n_cores) {
  
  cluster <- 
    multidplyr::new_cluster(n_cores) %>% 
    multidplyr::cluster_library("dplyr") %>% 
    multidplyr::cluster_library("purrr") %>% 
    multidplyr::cluster_library("forecast") %>% 
    multidplyr::cluster_copy(".make_predictions") %>% 
    multidplyr::cluster_copy(".lambdaBoxCox") %>% 
    multidplyr::cluster_copy(".fit_model") %>% 
    multidplyr::cluster_copy(".format_forecast") %>% 
    multidplyr::cluster_copy(".transform_forecast") %>% 
    multidplyr::cluster_copy(".fit_forecast_model") %>% 
    multidplyr::cluster_copy(".fit_forecast_model_with_timeout") %>% 
    multidplyr::cluster_copy(".fill_na_forecasts") %>% 
    multidplyr::cluster_copy(".theta_unseasonal") %>% 
    multidplyr::cluster_copy(".make_prediction_tibble") %>% 
    multidplyr::cluster_copy(".safely_fit_forecast_model") %>% 
    multidplyr::cluster_copy(".safely_format_forecast")
  
}


#' Selects models performing better than the naive forecast model
#' @param metrics A dataframe of accuracy metrics
#' @param accuracy_metric The accuracy metric used for deciding
#' @return A list of models
.select_successful_models <- function(metrics, accuracy_metric) {
  
  if (!accuracy_metric %in% colnames(metrics)) {
    stop("Invalid accuracy metric.")
  }
  
  naive_accuracy <-
    metrics %>% 
    dplyr::filter(model_name == "naive") %>% 
    purrr::pluck(accuracy_metric)
  
  final_models <-
    metrics %>% 
    dplyr::filter(!!rlang::sym(accuracy_metric) < naive_accuracy) %>% 
    purrr::pluck("model_name")
  
}


#' Selects the k best performing models
#' @param metrics A dataframe of accuracy metrics
#' @param accuracy_metric The accuracy metric used for deciding
#' @param k the desired number of models
#' @return A list of models
.select_k_best_models <- function(metrics, accuracy_metric, k) {
  
  final_models <-
    metrics %>% 
    dplyr::filter(model_name != "naive") %>% 
    dplyr::arrange(!!rlang::sym(accuracy_metric)) %>% 
    dplyr::mutate(i = 1:n()) %>% 
    dplyr::filter(i <= k) %>% 
    purrr::pluck("model_name")
  
}


#' Blends multiple forecast models into a single forecast
#' @param prediction_df A dataframe containing forecasts
#' @param model_names A list of models to blend
#' @param agg_funct The aggregation function to use for blending.
#' @param blend_name The name of the resulting blended forecast
.blend_forecasts <- function(prediction_df,
                             model_names = NULL,
                             agg_func = "mean",
                             blend_name = NULL) {
  
  if (model_names  %>% is.null()) {
    
    model_names_all <- 
      prediction_df %>%
      purrr::pluck("model_name") %>% 
      unique()
    
    model_names <- model_names_all[model_names_all != "naive"]
    
  }
  
  if (blend_name %>% is.null()) {
    
    blend_name <- paste("blend", agg_func, sep = "_")
    
  }
  
  predictions_grouped <-
    prediction_df %>% 
    dplyr::filter(model_name %in% model_names) %>% 
    dplyr::group_by(case_id,
                    target_type,
                    target_value_at_forecast,
                    datetime_at_forecast,
                    datetime_first_obs,
                    datetime)
  
  
  if (agg_func == "median") {
    
    predictions_blended <-
      predictions_grouped %>% 
      dplyr::summarise(prediction = median(prediction),
                       # just to make sure these columns appear in the output
                       target_value = mean(target_value), .groups = "keep") %>% 
      dplyr::mutate(model_name = blend_name)
    
  } else if (agg_func == "mean") {
    
    predictions_blended <-
      predictions_grouped %>% 
      dplyr::summarise(prediction = mean(prediction),
                       # just to make sure these columns appear in the output
                       target_value = mean(target_value),
                       target_imputed = mean(target_imputed),
                       .groups = "keep") %>% 
      dplyr::mutate(model_name = blend_name)
    
  } else {
    
    stop("Invalid aggregation function.")
    
  }
  
  return(predictions_blended)
  
}


#' Calculates the rate of success per model
#' @param data_nested_with_forecasts A nested dataframe with forecasts
#' @return A dataframe indicating the success rate per model
.get_model_success_rates <- function(data_nested_with_forecasts) {
  
  model_success <-
    data_nested_with_forecasts %>% 
    dplyr::group_by(model_name) %>% 
    dplyr::summarise(
      success_rate_model_fit = mean(model_fit_successful),
      success_rate_model_predictions = mean(model_prediction_successful),
      avg_fit_time = mean(seconds_to_fit),
      max_fit_time = max(seconds_to_fit),
      .groups = "keep"
    )
  
}

#' Merges a nested dataframe containing forecasts with the original
#' data containing the true values
#' @param data_nested_with_predictions A nested dataframe with forecasts
#' @param data_long An un-nested dataframe of true values
#' @return A combined dataframe
.unnested_prediction_df <- function(data_nested_with_predictions,
                                    data_long) {
  
  # build a prediction data-frame containing forecasts and predictions per model
  prediction_df <-
    data_nested_with_predictions %>% 
    dplyr::select(-c(target_ts, upper_bound, lower_bound)) %>% 
    tidyr::unnest(cols = predictions) %>% 
    # add the true values
    dplyr::left_join(data_long %>% 
                       dplyr::select(case_id,
                                     datetime, 
                                     target_type,
                                     target_value,
                                     target_imputed),
                     by = c("case_id", 
                            "datetime",
                            "target_type")) %>% 
    # drop forecasts that go beyond the observed time-frame
    tidyr::drop_na(target_value) %>% 
    # add some time-info
    dplyr::mutate(minutes_prior = difftime(datetime, datetime_at_forecast, units = "mins") %>% as.numeric(),
                  minutes_since_first_obs = difftime(datetime_at_forecast, datetime_first_obs, unit = "mins") %>% as.numeric()) %>%
    # sort to make things nicer
    dplyr::arrange(case_id, datetime_at_forecast, datetime)
  
}


#' Writes a dataframe to a parquet file
#' @param df A dataframe containing a single case_id
#' @param output_folder The path to the desired parquet files
.case_to_parquet <- function(df,
                             output_folder) {
  
  # get the case number
  case <- 
    df %>% 
    purrr::pluck("case_id") %>% 
    min() %>% 
    # avoid scientific notation like 9e+5 for 900000
    format(scientific = FALSE)
  
  # get the ts models 
  models <-
    df %>% 
    purrr::pluck("model_name") %>% 
    unique()
  
  for (m in models) {
    
    file <- paste0(output_folder, "/", m, "__case_", case, ".parquet")
    df %>%
      dplyr::filter(model_name == m) %>% 
      arrow::write_parquet(file)
    
  }

}


#' Writes a dataframe to parquet files grouped by the case_id
#' @param df A dataframe
#' @param output_folder The path to the desired parquet files
.to_parquet_per_case <- function(df,
                                 output_folder) {
  
  # always overwrite
  if (output_folder %in% list.dirs("data/predictions")) {
    unlink(output_folder, recursive = TRUE)
  }
  dir.create(output_folder)
  
  list_of_tibbles <- df %>% 
    dplyr::group_by(case_id) %>% 
    dplyr::group_split()
  
  t <- lapply(list_of_tibbles, .case_to_parquet, output_folder)
  
}


#' Per target type, returns the min and max
#' @param config The configuration dictionary
#' @return A dataframe giving min and max per target
.get_forecast_bounds <- function(config, bound_type = c("extreme_value_limits", "danger_zone_limits")) {
  
  bound_type <- match.arg(bound_type)
  
  bounds <- 
    config %>% 
    purrr::pluck(bound_type) %>% 
    dplyr::as_tibble() %>% 
    dplyr::select(config$targets) %>% 
    sjmisc::rotate_df() %>% 
    dplyr::rename(lower_bound = V1,
                  upper_bound = V2) %>% 
    tibble::rownames_to_column() %>% 
    dplyr::rename(target_type = rowname)
  
}
