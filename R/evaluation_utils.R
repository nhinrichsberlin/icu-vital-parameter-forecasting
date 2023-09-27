#' Function that creates (if needed) a new directory in which to store plots
#' @param path_to_directory Path to the desired directory in which to store plots
#' @return NULL
.create_plot_directory <- function(path_to_directory) {
  
  if (dir.exists(path_to_directory)) {
    return(NULL)
  }
  
  dir.create(path_to_directory)
  file.create(paste0(path_to_directory, "/.gitignore"))
  
  return(NULL)
  
}


#' Checks that all forecasts were produced as expected
#' @param datasets Whether to check for test data, eicu data or both
#' @return Nothing
.check_expected_vs_forecasted <- function(datasets = c("test", "eicu")) {
  
  # check that all model/case combinations match
  for (dataset in datasets) {
    
    expected_cases <-
      list.files(paste0("data/processed/vitals_", dataset),
                 pattern = ".parquet")
    
    univariate_model_cases <- 
      list.files(paste0("data/predictions/", dataset, "_predictions_univariate/"),
                 pattern = ".parquet") %>% 
      purrr::map_chr(~purrr::pluck(stringr::str_split_fixed(., "__", 2), 2)) %>% 
      unique()
    
    ml_model_cases <- 
      list.files(paste0("data/predictions/", dataset, "_predictions_ml/"),
                 pattern = ".parquet") %>% 
      purrr::map_chr(~purrr::pluck(stringr::str_split_fixed(., "__", 2), 2)) %>% 
      unique()
    
    unviv_matches_expected <- identical(expected_cases, univariate_model_cases)
    ml_matches_expected <- identical(expected_cases, ml_model_cases)
    
    if (!(unviv_matches_expected & ml_matches_expected)) {
      
      stop("Cases do not match between univariate and ml models.")
      
    }
    
    message("\t", Sys.time(), " -- Cases match for ", dataset, " data.")
    
  }
  
}


.store_error_metrics <- function(targets,
                                 models) {
  
  error_metrics <- 
    .load_error_metrics(datasets = c("test", "eicu"),
                        model_families = c("univariate", "ml"),
                        target_types = targets,
                        models = models,
                        mins_ahead = seq(5, 120, 5))
  
  folder <- "evaluation/tables/"
  file <- "error_metrics.csv"
  
  error_metrics %>% 
    readr::write_delim(paste0(folder, file),
                       delim = ";")
  

}



#' Loads forecasts of a single case and calculates errors
#' @param folder The folder in which the forecasts are stored
#' @param case_file The case for which we need the errors (e.g. case_123.parquet)
#' @param eicu_or_test Whether forecasts are from the EICU or the test dataset
#' @param target_types The desired targets for which to compute errors
#' @param models The desired models for which to compute errors
#' @param mins_ahead The desired forecast horizons
#' @return A dataframe with summed up errors for the cast in question
.error_sums_single_case <- function(folder,
                                    case_file,
                                    eicu_or_test,
                                    target_types,
                                    models,
                                    mins_ahead) {

  # list all files in the folder
  files_in_folder <- list.files(folder, pattern = ".parquet")
  
  # each case is described via multiple files
  # (e.g. transformer__case_123.parquet, gru__case_123.parquet)
  relevant_files <- 
    files_in_folder[
      files_in_folder %>% purrr::map_lgl(~base::endsWith(., case_file))
    ]
  
  # define the path to the desired files
  relevant_files <- paste0(folder, "/", relevant_files)
  
  # read the file(s) to a tibble
  case <- 
    relevant_files %>% 
    purrr::map(~arrow::read_parquet(.)) %>% 
    dplyr::bind_rows()
  
  case <- 
    
    case %>% 
    
    # make sure the case_id column is a chr (for compatibility)
    dplyr::mutate(case_id = as.character(case_id)) %>% 
    
    # add minutes_ahead column
    dplyr::mutate(minutes_ahead = (datetime - datetime_at_forecast) %>% as.double(unit = "mins")) %>% 
    
    # drop cases where the target was imputed
    dplyr::filter(target_imputed == 0) %>% 
    
    # make sure only to include desired target types, horizons, models
    dplyr::filter(target_type %in% target_types) %>% 
    dplyr::filter(minutes_ahead %in% mins_ahead) %>%
    dplyr::filter(model_name %in% models) %>% 
    
    # add a hack to make sure model names remain in proper order when plotting
    dplyr::mutate(model_name = factor(model_name, levels = models))
  
  case <-
    case %>% 
    
    # calculate errors
    dplyr::mutate(error = prediction - target_value,
                  error_abs = abs(prediction - target_value),
                  error_sqr = (prediction - target_value)^2,
                  error_abs_rel = abs(prediction - target_value) / max(1, target_value)) %>% 
    
    # calculate trends
    dplyr::mutate(# upward
      upward_trend = target_value %>=% target_value_at_forecast,
      upward_trend_forecast = prediction %>=% target_value_at_forecast,
      # downward
      downward_trend = target_value %<<% target_value_at_forecast,
      downward_trend_forecast = prediction %<<% target_value_at_forecast) %>% 
    
    # calculate trend forecast accuracy %>% 
    dplyr::mutate(trend_identified = (upward_trend & upward_trend_forecast) | (downward_trend & downward_trend_forecast)) %>% 

    # summarise errors by summing them up
    dplyr::group_by(case_id,
                    target_type,
                    model_name,
                    minutes_ahead) %>% 
    
    dplyr::summarise(dataset_name = eicu_or_test,
                     
                     # counts
                     n_obs_case = n(),
                     n_movements_case = sum(upward_trend | downward_trend),
                     
                     # errors metrics
                     error_sum_case = sum(error),
                     error_abs_sum_case = sum(error_abs),
                     error_sqr_sum_case = sum(error_sqr),
                     error_abs_rel_sum_case = sum(error_abs_rel),
                     
                     # trend identification
                     trend_identified_sum_case = sum(trend_identified),
                     
                     .groups = "keep")
  
  return(case)
  
} 



#' Computes error metrics from forecasts
#' @param datasets Which of eicu and/or test set to use
#' @param model_families Which of univariate and/or ml to use
#' @param models The models of interest
#' @param mins_ahead The forecast horizons of interest
#' @return A dataframe of forecast errors across all cases
.load_error_metrics <- function(datasets,
                                model_families,
                                target_types,
                                models,
                                mins_ahead) {
  
  message("\t", Sys.time(), " -- Computing error metrics.")
  
  # define empty tibble to be filled step by step
  error_sums <- dplyr::tibble()
  
  # loop over datasets
  for (d in datasets) {
    
    message("\t", Sys.time(), " -- Beginning with ", d, " data.")
    
    # define the list of case files for which we have forecasts
    cases <- list.files(paste0("data/processed/vitals_", d), pattern = ".parquet")

    # loop over model families
    for (mf in model_families) {
      
      # create a cluster for multi-processing
      cluster <- parallel::makeCluster(parallel::detectCores() - 4)
      doParallel::registerDoParallel(cluster)
      
      message("\t\t", Sys.time(), " -- ", mf, " models.")
      
      # define the folder containing predictions
      folder <- paste0("data/predictions/", d, "_predictions_", mf)
      
      # loop over cases
      error_sums_current <- foreach::foreach(i = 1:length(cases), .combine = "rbind") %dopar% {
        
        # functions and packages need to be re-loaded inside the parallel foreach loop
        source("R/evaluation_utils.R")
        library(dplyr)
        library(fpCompare)
        
        .error_sums_single_case(folder = folder,
                                case_file = cases[i],
                                eicu_or_test = d,
                                target_types = target_types,
                                models = models,
                                mins_ahead = mins_ahead)
        
      }
      
      # append the recent results to the overall results
      error_sums <- rbind(error_sums, error_sums_current)
      
      # cleanup
      parallel::stopCluster(cluster)
      rm(cluster)
      
    }
  }
  
  # display number of unique cases
  message("Loaded prediction errors for ",
          error_sums$case_id %>% unique() %>% length(),
          " cases.")
  
  # compute aggregate statistics by weighting with the corresponding n_..._case
  error_means <-
    
    error_sums %>% 
    
    dplyr::group_by(dataset_name,
                    target_type,
                    model_name,
                    minutes_ahead) %>% 
    
    # get the total sum of observations across all cases
    dplyr::mutate(n = sum(n_obs_case)) %>% 
    
    # get the total sum of movements
    dplyr::mutate(n_movements = sum(n_movements_case)) %>% 
    
    # compute mean errors by weighting error sums by n_obs_case / n
    dplyr::summarise(bias = sum(error_sum_case / n),
                     mae = sum(error_abs_sum_case / n),
                     rmse = sqrt(sum(error_sqr_sum_case / n)),
                     mape = 100 * sum(error_abs_rel_sum_case / n),
                     trend_identification = 100 * sum(trend_identified_sum_case / n_movements),
                     .groups = "keep")
}
