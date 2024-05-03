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
                                 models,
                                 by_hours_since_admission) {
  
  metrics <- 
    .load_error_metrics(datasets = c("eicu", "test"),
                        model_families = c("univariate", "ml"),
                        target_types = targets,
                        models = models,
                        mins_ahead = seq(5, 120, 5),
                        by_hours_since_admission = by_hours_since_admission)
  
  # extract the errors grouped by dataset, model, target_type, minutes ahead
  error_metrics <- metrics$error_means
  # extract the MAPE across dataset and model
  mape_across_all <- metrics$mape_across_all 
  
  folder <- "evaluation/tables/"
  
  if (!by_hours_since_admission) {
    
    error_metrics %>% 
      readr::write_delim(paste0(folder, "error_metrics.csv"),
                         delim = ";")
    
    mape_across_all %>% 
      readr::write_delim(paste0(folder, "mape_across_all.csv"),
                         delim = ";")
    
  } else {
    
    mape_across_all %>% 
      readr::write_delim(paste0(folder, "mape_across_all_by_hours_since_admission.csv"),
                         delim = ";")
  }
}



#' computes the RMSE from an error metrics dataframe
#' @param case_ids A vector of case_ids in the dataframe
#' @param i A vector of bootstrapped case_ids
#' @errors_per_case A tibble
#' @return A double: the RMSE
.rmse <- function(case_ids, i, errors_per_case) {
  
  # per case id in the bootstrap sample, calculate how often the case
  # is represented
  n_per_case <- 
    dplyr::tibble(case_id = case_ids[i]) %>% 
    dplyr::group_by(case_id) %>% 
    dplyr::summarise(n_boots = n())
  
  # match this info to the errors_per_case
  errors_per_case <-
    errors_per_case %>% 
    dplyr::left_join(n_per_case, by = "case_id") %>% 
    dplyr::mutate(n_boots = dplyr::coalesce(n_boots, 0)) %>% 
    # explode the tibble
    tidyr::uncount(n_boots)
  
  n <- sum(errors_per_case$n_obs_case)
  mse <- sum(errors_per_case$error_sqr_sum_case / n)
  rmse <- sqrt(mse)
  
  return(rmse)
  
}


#' A wrapper around boot::boot
#' @param dataset Either EICU or Test
#' @param model The forecast model
#' @param target The target type
#' @param minutes The forecast horizon in minutes
#' @param errors_per_case A tibble containing errors per case
#' @return A call to boot::boot
.bootstrap_rmse_error <- function(errors_per_case) {
  
  cases <- unique(errors_per_case$case_id)
  
  b <- boot::boot(cases, 
                  statistic = .rmse, 
                  R = 250,
                  errors_per_case = errors_per_case,
                  ncpus = 4,
                  parallel = "multicore")
  
  return(b)
  
}


.filter_error_sums_rmse <- function(dataset,
                                    model,
                                    target,
                                    minutes,
                                    error_sums) {
  
  error_sums %>% 
    dplyr::filter(dataset_name == dataset,
                  model_name == model,
                  target_type == target,
                  minutes_ahead == minutes)
  
}


#' A function performing bootstrapping for the RMSE
#' @param error_sums_per_case A tibble containing errors per case_id
#' @return A tibble with an extra column: the call to boot::boot
.bootstrap_rmse <- function(error_sums_per_case) {
  
  multicore_cluster <- 
    multidplyr::new_cluster(6) %>% 
    multidplyr::cluster_library("dplyr") %>% 
    multidplyr::cluster_copy(".rmse") %>% 
    multidplyr::cluster_copy(".bootstrap_rmse_error")
  
  bootstrap <-
    error_sums_per_case %>% 
    # subset to variables for which to bootstrap
    dplyr::select(dataset_name,
                  model_name,
                  target_type,
                  minutes_ahead) %>% 
    # keep only distinct combinations
    dplyr::distinct() %>% 
    # create a new column containing only the relevant error sums
    dplyr::mutate(error_sums_relevant = purrr::pmap(.l = list(dataset_name,
                                                              model_name,
                                                              target_type,
                                                              minutes_ahead),
                                                    .f = .filter_error_sums_rmse,
                                                    error_sums = error_sums_per_case))
  
  # clean up to avoid memory issues
  rm(error_sums_per_case)
  
  bootstrap <-
    bootstrap %>% 
    # apply multiprocessing
    multidplyr::partition(multicore_cluster) %>% 
    # run the bootstrap
    dplyr::mutate(bootstrap = purrr::map(.x = error_sums_relevant,
                                         .f = .bootstrap_rmse_error)) %>% 
    dplyr::collect() %>% 
    dplyr::ungroup()
  
  # cleanup
  rm(multicore_cluster)
  
  return(bootstrap)

}


#' computes the MAPE from an error metrics dataframe
#' @param case_ids A vector of case_ids in the dataframe
#' @param i A vector of bootstrapped case_ids
#' @errors_per_case A tibble
#' @return A double: the MAPE
.mape <- function(case_ids, i, errors_per_case) {
  
  # per case id in the bootstrap sample, calculate how often the case
  # is represented
  n_per_case <- 
    dplyr::tibble(case_id = case_ids[i]) %>% 
    dplyr::group_by(case_id) %>% 
    dplyr::summarise(n_boots = n())
  
  # match this info to the errors_per_case
  errors_per_case <-
    errors_per_case %>% 
    dplyr::left_join(n_per_case, by = "case_id") %>% 
    dplyr::mutate(n_boots = dplyr::coalesce(n_boots, 0)) %>% 
    # explode the tibble
    tidyr::uncount(n_boots)
  
  n <- sum(errors_per_case$n_obs_case)
  mape <- 100 * sum(errors_per_case$error_abs_rel_sum_case / n)
  
  return(mape)
  
}


#' A wrapper around boot::boot
#' @param dataset Either EICU or Test
#' @param model The forecast model
#' @param errors_per_case A tibble containing errors per case
#' @return A call to boot::boot
.bootstrap_mape_error <- function(errors_per_case) {
  
  cases <- unique(errors_per_case$case_id)
  
  b <- boot::boot(cases, 
                  statistic = .mape, 
                  R = 250, 
                  errors_per_case = errors_per_case,
                  ncpus = 6,
                  parallel = "multicore")
  
  return(b)
  
}


.filter_error_sums_mape <- function(dataset,
                                    model,
                                    error_sums) {
  
  error_sums %>% 
    dplyr::filter(dataset_name == dataset,
                  model_name == model)
  
}


#' A function performing bootstrapping for the RMSE
#' @param error_sums_per_case A tibble containing errors per case_id
#' @return A tibble with an extra column: the call to boot::boot
.bootstrap_mape_across_targets_and_horizons <- function(error_sums_per_case) {
  
  bootstrap <-
    error_sums_per_case %>% 
    # ungroup to get make sure no other variable is kept
    dplyr::ungroup() %>% 
    # subset to variables for which to bootstrap
    dplyr::select(dataset_name,
                  model_name) %>% 
    # keep only distinct combinations
    dplyr::distinct() %>%
    # create a new column containing only the relevant error sums
    dplyr::mutate(error_sums_relevant = purrr::pmap(.l = list(dataset_name,
                                                              model_name),
                                                    .f = .filter_error_sums_mape,
                                                    error_sums = error_sums_per_case))
  
  # cleanup to avoid memory issues
  rm(error_sums_per_case)
  
  bootstrap <-
    bootstrap %>% 
    dplyr::mutate(bootstrap = purrr::map(.x = error_sums_relevant,
                                         .f = .bootstrap_mape_error))
  
  return(bootstrap)
  
}


#' A function that reads a parquet file
#' and does necessary mutations to make sure
#' multiple cases can be appended
#' @param filepath Path to parquet file
#' @returns A tibble
.read_case_from_parquet <- function(filepath) {
  
  arrow::read_parquet(filepath) %>% 
    # make sure case_id is a character vector
    dplyr::mutate(case_id = as.character(case_id))
  
} 


#' A function that loads all predictions made for a case
#' @param folder A folder path to predictions files
#' @param case Either a file-name case_id.parquet or just the case_id
.load_case_predictions <- function(folder,
                                   case) {
  
  # make sure we can pass case as parquet file or as case_id
  if (!endsWith(case, '.parquet')) {
    case <- paste0(case, '.parquet')
  }
  
  # list all files in the folder
  files_in_folder <- list.files(folder, pattern = ".parquet")
  
  # each case is described via multiple files
  # (e.g. transformer__case_123.parquet, gru__case_123.parquet)
  relevant_files <- 
    files_in_folder[
      files_in_folder %>% purrr::map_lgl(~base::endsWith(., case))
    ]
  
  # define the path to the desired files
  relevant_files <- paste0(folder, "/", relevant_files)
  
  # read the file(s) to a tibble
  case <- 
    relevant_files %>% 
    purrr::map(.read_case_from_parquet) %>% 
    dplyr::bind_rows()
  
  return(case)
  
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
                                    mins_ahead,
                                    by_hours_since_admission) {

  # load all predictions stored in the folder made for the case in question
  case <- .load_case_predictions(folder, case_file)
  
  # define by which variables to group
  grouping_vars <- c("case_id",
                     "target_type",
                     "model_name",
                     "minutes_ahead")
  
  grouping_vars <- if (by_hours_since_admission) c(grouping_vars, "hours_since_admission") else grouping_vars
  
  case <- 
    
    case %>% 
    
    # add minutes since admission column 
    # (needed to filter ML models, since for univariate models this is always >= 30 minutes)
    dplyr::mutate(minutes_since_admission = (datetime - datetime_first_obs) %>% as.double(unit = "mins")) %>% 
    
    # add minutes_ahead column
    dplyr::mutate(minutes_ahead = (datetime - datetime_at_forecast) %>% as.double(unit = "mins")) %>% 
    
    # add hours_since_admission column
    dplyr::mutate(hours_since_admission = (datetime - datetime_first_obs) %>%
                                             as.double(unit='hours') %>% 
                                             as.integer()) %>% 
  
    # if we group by hours since admission, keep only minutes_ahead <= 30
    {if (by_hours_since_admission) dplyr::filter(., minutes_ahead <= 30) else .} %>% 
    
    # drop the first 25 minutes after admission
    dplyr::filter(minutes_since_admission >= 30) %>% 
  
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
                  error_abs_rel = abs(prediction - target_value) / max(1, abs(target_value))) %>% 

    # summarise errors by summing them up
    dplyr::group_by(!!!rlang::syms(grouping_vars)) %>% 
    
    dplyr::summarise(dataset_name = eicu_or_test,
                     
                     # counts
                     n_obs_case = n(),
                     
                     # errors metrics
                     error_sum_case = sum(error),
                     error_abs_sum_case = sum(error_abs),
                     error_sqr_sum_case = sum(error_sqr),
                     error_abs_rel_sum_case = sum(error_abs_rel),
                     
                     .groups = "keep")
  
  return(list(case))
  
}


.compute_error_sums_per_case <- function(datasets,
                                         model_families,
                                         target_types,
                                         models,
                                         mins_ahead,
                                         by_hours_since_admission) {
  
  message("\t",
          Sys.time(),
          " -- Computing error metrics",
          if (by_hours_since_admission) " - accounting for hours since admission." else ".")
  
  # name of output files
  output_name <- if(!by_hours_since_admission) "error_sums" else "error_sums__by_hour"
  
  # keep a list of files containing error sums
  files_error_sums <- c()
  
  # loop over datasets
  for (d in datasets) {
    
    # get a list of files containing forecasts
    folder_dataset <- paste0("data/processed/vitals_", d)
    cases_dataset <- list.files(folder_dataset, pattern = ".parquet")
    
    # if we have lots of cases, split them to avoid memory issues
    n_splits <- if (length(cases_dataset) < 500) 2 else as.integer(length(cases_dataset) / 500)
    splits <- parallel::splitIndices(length(cases_dataset), n_splits)
    
    # loop over model families
    for (mf in model_families) {
      
      # define the folder containing predictions
      folder_model_family <- paste0("data/predictions/", d, "_predictions_", mf)
      
      for (j in 1:n_splits) {
        
        message("\t\t",
                Sys.time(),
                " -- ",
                d,
                " -- ",
                mf,
                " -- ",
                "Case split ",
                j,
                " of ",
                n_splits)
        
        cases_subset <- cases_dataset[splits[[j]]]
        
        # create a cluster for multi-processing
        cluster <- parallel::makeCluster(parallel::detectCores() - 1)
        doParallel::registerDoParallel(cluster)
        
        # loop over cases and compute error sums
        error_sums_current <- foreach::foreach(i = 1:length(cases_subset),
                                               .combine = "append",
                                               .packages = c("dplyr")) %dopar% {
                                                 
          # functions and packages need to be re-loaded inside the parallel foreach loop
          source("R/evaluation_utils.R")
          
          .error_sums_single_case(folder = folder_model_family,
                                  case_file = cases_subset[i],
                                  eicu_or_test = d,
                                  target_types = target_types,
                                  models = models,
                                  mins_ahead = mins_ahead,
                                  by_hours_since_admission = by_hours_since_admission)
                                                 
        }
        
        # cleanup
        parallel::stopCluster(cluster)
  
        # concatenate individual tibbles into a single tibble
        error_sums_current <- 
          error_sums_current %>% 
          dplyr::bind_rows()
        
        message("\t\t",
                Sys.time(),
                " -- ",
                d,
                " -- ",
                mf,
                " -- ",
                "Case split ",
                j,
                " of ",
                n_splits,
                " done.")
        
        # store error sums per case split by model and dataset
        # to avoid huge CSV files
        for (model in unique(error_sums_current$model_name)) {
          
          file_error_sums <- 
            paste0("evaluation/tables/error_sums_per_case/",
                   output_name,
                   "__",
                   model,
                   "__",
                   d,
                   ".csv")
          
          append <- (j > 1)
          
          message("\t\t",
                  Sys.time(),
                  if (append) " -- Appending file " else " -- Creating new file ",
                  file_error_sums)
          
          error_sums_current %>% 
            readr::write_delim(file_error_sums, 
                               delim = ";",
                               append = append,
                               col_names = !append)
          
          # track new file
          file_is_new <- !(file_error_sums %in% files_error_sums)
          files_error_sums <- if (file_is_new) c(file_error_sums, files_error_sums) else files_error_sums
          
        }
      }
    }
  }
  
  return(files_error_sums)
  
}


.read_error_sum_file <- function(f) {
  
  result <- 
    f %>% 
    readr::read_delim(delim = ";", show_col_types = FALSE) %>% 
    dplyr::mutate(case_id = as.character(case_id))
  
  return(result)
}


.load_error_sums_from_files <- function(files_error_sums,
                                        by_hours_since_admission) {
  
  message(Sys.time(),
          " -- Loading error sums from ",
          length(files_error_sums),
          " CSV-files.")
  
  error_sums <- 
    files_error_sums %>% 
    purrr::map(.f = ~.read_error_sum_file(.x)) %>% 
    dplyr::bind_rows()
  
  message(Sys.time(),
          " -- Error sums successfully loaded.")
  
  return(error_sums)
  
}


.rmse_bootstrap <- function(error_sums) {
  
  # sub-set to desired minutes to be included in plots
  error_sums <-
    error_sums %>% 
    dplyr::filter(minutes_ahead %in% config$minutes_ahead_to_plot) %>% 
    # make sure it is un-grouped
    ungroup()
  
  out_file <- "evaluation/tables/rmse_error_bars.csv"
  
  bootstrap_rmse <- dplyr::tibble()
  
  # split to avoid memory issues
  for (dataset in unique(error_sums$dataset_name)) {
    for (model in (unique(error_sums$model_name))) {
      for (minutes in unique(error_sums$minutes_ahead)){
        
        message(Sys.time(),
                " -- Bootstrapping RMSE CIs -- ",
                dataset,
                " -- ",
                model,
                " -- ",
                minutes,
                " min horizon.")
        
        # sub-set error sums
        error_sums_subset <-
          error_sums %>% 
          dplyr::filter(dataset_name == dataset,
                        model_name == model,
                        minutes_ahead == minutes)
        
        # run bootstrapping
        bootstrap_rmse_current <- 
          .bootstrap_rmse(error_sums_subset) %>% 
          dplyr::mutate(rmse_point_estimate = purrr::map_dbl(bootstrap, ~(.x$t0))) %>% 
          dplyr::mutate(rmse_std_error = purrr::map_dbl(bootstrap, ~(sd(.x$t)))) %>% 
          dplyr::mutate(rmse_plus_one_std = rmse_point_estimate + rmse_std_error,
                        rmse_minus_one_std = rmse_point_estimate - rmse_std_error) %>% 
          dplyr::mutate(rmse_ci_lower = purrr::map_dbl(bootstrap, ~quantile(.x$t, 0.05))) %>% 
          dplyr::mutate(rmse_ci_upper = purrr::map_dbl(bootstrap, ~quantile(.x$t, 0.95)))
        
        # append the results
        bootstrap_rmse <-
          bootstrap_rmse %>% 
          dplyr::bind_rows(bootstrap_rmse_current)
        
      }
    }
    
    # drop no longer needed rows from error sums to save memory
    error_sums <-
      error_sums %>% 
      dplyr::filter(dataset_name != dataset)
    
  }
  
  # store results for later use in plots
  bootstrap_rmse %>% 
    dplyr::select(dataset_name,
                  target_type,
                  model_name,
                  minutes_ahead,
                  rmse_point_estimate,
                  rmse_std_error,
                  rmse_plus_one_std,
                  rmse_minus_one_std,
                  rmse_ci_lower,
                  rmse_ci_upper) %>%
    readr::write_delim(out_file, delim = ";")
  
  message(Sys.time(),
          " --  Results stored here: ",
          out_file)
  
}


.mape_bootstrap <- function(error_sums) {
  
  out_file <- "evaluation/tables/mape_error_bars.csv"
  
  bootstrap_mape <- dplyr::tibble()
  
  for (dataset in unique(error_sums$dataset_name)) {
    for (model in unique(error_sums$model_name)) {
      
      message(Sys.time(),
              " --  Bootstrapping MAPE CIs ",
              " for dataset ",
              dataset,
              " and model ",
              model)
      
      # sub-set error_sums
      error_sums_subset <-
        error_sums %>% 
        dplyr::filter(dataset_name == dataset,
                      model_name == model)
      
      bootstrap_mape_current <-
        .bootstrap_mape_across_targets_and_horizons(error_sums_subset) %>% 
        dplyr::mutate(mape_point_estimate = purrr::map_dbl(bootstrap, ~(.x$t0))) %>% 
        dplyr::mutate(mape_std_error = purrr::map_dbl(bootstrap, ~(sd(.x$t)))) %>% 
        dplyr::mutate(mape_plus_one_std = mape_point_estimate + mape_std_error,
                      mape_minus_one_std = mape_point_estimate - mape_std_error) %>% 
        dplyr::mutate(mape_ci_lower = purrr::map_dbl(bootstrap, ~quantile(.x$t, 0.05))) %>% 
        dplyr::mutate(mape_ci_upper = purrr::map_dbl(bootstrap, ~quantile(.x$t, 0.95)))
      
      bootstrap_mape <-
        bootstrap_mape %>% 
        dplyr::bind_rows(bootstrap_mape_current)
    }
    
    # drop no longer needed rows from error_sums to save memory
    error_sums <-
      error_sums %>% 
      dplyr::filter(dataset_name != dataset)
    
  }
  
  bootstrap_mape %>% 
    dplyr::select(dataset_name,
                  model_name,
                  mape_point_estimate,
                  mape_std_error,
                  mape_plus_one_std,
                  mape_minus_one_std,
                  mape_ci_lower,
                  mape_ci_upper) %>% 
    readr::write_delim(out_file, delim = ";")
  
  message(Sys.time(),
          " --  Results stored here: ",
          out_file)
  
}


.summarise_error_sums <- function(error_sums) {
  
  # define by which variables to group the output
  grouping_vars <- c("dataset_name",
                     "target_type",
                     "model_name",
                     "minutes_ahead")
  
  # compute aggregate statistics by weighting with the corresponding n_..._case
  error_means <-
    error_sums %>% 
    dplyr::group_by(!!!rlang::syms(grouping_vars)) %>% 
    # get the total sum of observations across all cases
    dplyr::mutate(n = sum(n_obs_case)) %>% 
    # compute mean errors by weighting error sums by n_obs_case / n
    dplyr::summarise(bias = sum(error_sum_case / n),
                     mae = sum(error_abs_sum_case / n),
                     rmse = sqrt(sum(error_sqr_sum_case / n)),
                     mape = 100 * sum(error_abs_rel_sum_case / n),
                     .groups = "keep")
  
}


.compute_mape <- function(error_sums,
                          by_hours_since_admission) {
  
  # compute the MAPE across all target types and forecast horizons
  grouping_vars <- c("dataset_name",
                     "model_name")
  
  grouping_vars <- if (by_hours_since_admission) c(grouping_vars, "minutes_ahead", "hours_since_admission") else grouping_vars
  
  error_sums %>% 
    dplyr::group_by(!!!rlang::syms(grouping_vars)) %>% 
    # get the total sum of observations
    dplyr::mutate(n = sum(n_obs_case)) %>% 
    # compute the MAPE
    dplyr::summarise(mape = 100 * sum(error_abs_rel_sum_case / n),
                     .groups = "keep")
  
}


.compute_mape_by_hours_since_admission <- function(files_error_sums) {
  
  message(Sys.time(),
          " --  Computing MAPE, accounting for hours since admission.")
  
  mape_by_hours <- 
    files_error_sums %>% 
    purrr::map(.f = ~.compute_mape(.read_error_sum_file(.), TRUE)) %>% 
    dplyr::bind_rows()
  
  return(mape_by_hours)
  
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
                                mins_ahead,
                                by_hours_since_admission) {
  
  # compute error sums and receive a list of files where they are stored
  files_error_sums <- .compute_error_sums_per_case(datasets,
                                                   model_families,
                                                   target_types,
                                                   models,
                                                   mins_ahead,
                                                   by_hours_since_admission)
  
  # folder <- "evaluation/tables/error_sums_per_case/"
  # files_error_sums <- paste0(folder, list.files(folder, pattern = ".csv"))
  # files_error_sums <- files_error_sums[!grepl("__by_hour__", files_error_sums)]
  # files_error_sums <- files_error_sums[grepl("__by_hour__", files_error_sums)]
  
  if (by_hours_since_admission) {
    

    result <- list(error_means = NULL,
                   mape_across_all = .compute_mape_by_hours_since_admission(files_error_sums))
    
    return(result)
    
  } else {
    
    # load error sums from files
    error_sums <- .load_error_sums_from_files(files_error_sums,
                                              by_hours_since_admission)
    
    # compute confidence intervals using bootstrapping
    .rmse_bootstrap(error_sums)
    .mape_bootstrap(error_sums)
    
    result <- list(error_means = .summarise_error_sums(error_sums),
                   mape_across_all = .compute_mape(error_sums,
                                                   by_hours_since_admission))
    
    return(result)
  
  }
}
