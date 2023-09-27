source("R/setup.R")

metrics <- readr::read_delim("evaluation/tables/error_metrics.csv", delim = ";")

round_tidy = function(x, digits){
  sprintf.arg = paste0("%.", digits, "f")
  x.out = do.call(sprintf, list(sprintf.arg, x)) # keep trailing zeros
  return(x.out)
}

metrics <-
  metrics %>% 
  dplyr::select(dataset_name, target_type, model_name, minutes_ahead, rmse, mae, mape) %>%
  dplyr::filter(model_name %in% c("naive", "theta_unseasonal", "es_auto", "arima_auto", "nnet_ar", "gru", "transformer")) %>% 
  dplyr::mutate(dataset_name = dplyr::case_when(dataset_name == "eicu" ~ "External test set (eICU)",
                                                dataset_name == "test" ~ "Internal test set")) %>% 
  dplyr::mutate(model_name = dplyr::case_when(model_name == "naive" ~ "Naive",
                                              model_name == "theta_unseasonal" ~ "Theta",
                                              model_name == "es_auto" ~ "ETS",
                                              model_name == "arima_auto" ~ "ARIMA",
                                              model_name == "nnet_ar" ~ "AR NNet",
                                              model_name == "gru" ~ "GRU",
                                              model_name == "transformer" ~ "Transformer")) %>% 
  dplyr::mutate(target_type = dplyr::case_when(target_type == "blood_pressure_systolic" ~ "Systolic BP",
                                               target_type == "blood_pressure_diastolic" ~ "Diastolic BP",
                                               target_type == "blood_pressure_mean" ~ "Mean BP",
                                               target_type == "central_venous_pressure" ~ "Central venous pressure",
                                               target_type == "heart_rate" ~ "Heart rate",
                                               target_type == "oxygen_saturation" ~ "SpO2"))

m <- 
  metrics %>% 
  dplyr::arrange(dataset_name,
                 target_type,
                 minutes_ahead,
                 model_name) %>% 
  dplyr::group_by(dataset_name, 
                  target_type,
                  minutes_ahead) %>% 
  dplyr::mutate(rmse_rank = rank(rmse)) %>% 
  dplyr::group_by(dataset_name,
                  model_name,
                  rmse_rank) %>% 
  dplyr::summarise(n = n()) %>% 
  dplyr::arrange(dataset_name,
                 model_name,
                 rmse_rank)
  
  
# save as csv
metrics %>% 
  dplyr::mutate_at(c("rmse", "mae", "mape"), ~round_tidy(., 4)) %>% 
  dplyr::mutate_at(c("rmse", "mae", "mape"), ~as.character(.)) %>% 
  # HACK to avoid problems when importing table into MS Word
  dplyr::mutate_at(c("rmse", "mae", "mape"), ~paste0("xxx", .)) %>% 
  dplyr::arrange(dataset_name, target_type, model_name) %>% 
  dplyr::rename(RMSE = rmse,
                MAE = mae,
                MAPE = mape,
                "Vital parameter" = target_type,
                "Dataset" = "dataset_name",
                "Minutes ahead" = minutes_ahead,
                "Model" = model_name) %>% 
  readr::write_excel_csv("evaluation/tables/error_metrics_appendix.csv", delim = ";")
