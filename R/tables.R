#' Loads and combines training and evaluation data
#' @param var_labels Optional labels for the variables in question
#' @return A tibble containing the data
.load_mlife_vs_eicu <- function(var_labels = NULL) {
  
  vars_to_read <- c(
    "case_id",
    "age",
    "sex_male",
    "blood_pressure_systolic",
    "blood_pressure_mean",
    "blood_pressure_diastolic",
    "central_venous_pressure",
    "heart_rate",
    "oxygen_saturation"
  )
  
  # read MLife data
  mlife_train <- 
    .read_data_from_parquet("data/processed/vitals_train/") %>% 
    dplyr::select(dplyr::all_of(vars_to_read)) %>% 
    dplyr::mutate(datatype = "Training data")
    
  mlife_validate <- 
    .read_data_from_parquet("data/processed/vitals_validate/") %>% 
    dplyr::select(dplyr::all_of(vars_to_read)) %>% 
  dplyr::mutate(datatype = "Validation data") 
  
  mlife_test <- 
    .read_data_from_parquet("data/processed/vitals_test/") %>% 
    dplyr::select(dplyr::all_of(vars_to_read)) %>% 
  dplyr::mutate(datatype = "Internal test data")
  
  # read EICU data
  eicu <- 
    .read_data_from_parquet("data/processed/vitals_eicu/") %>% 
    dplyr::mutate(case_id = as.character(case_id)) %>% 
    dplyr::select(dplyr::all_of(vars_to_read)) %>% 
  dplyr::mutate(datatype = "External test data (EICU)")
  
  # combine all
  data <- 
    dplyr::bind_rows(mlife_train, mlife_validate, mlife_test, eicu)
  
  if (!is.null(var_labels)) {
   
    # update labels
    data <-
      data %>% 
      Hmisc::upData(labels = var_labels, print = FALSE)
  
  }
  
  return(data)
  
}


#' Produces a nice table one
mlife_vs_eicu <- function(all_vars, 
                          cont_vars,
                          cat_vars,
                          var_labels) {
  
  data <- .load_mlife_vs_eicu(var_labels)
  
  # create a nice table
  table <- 
    tableone::CreateTableOne(data = data,
                             vars = all_vars,
                             factorVars = cat_vars,
                             strata = "datatype",
                             testNonNormal = wilcox.test)
  
  # convert to chr
  table_chr <- 
    table %>% 
    print(nonnormal = cont_vars,
          smd = FALSE,
          test = FALSE,
          showAllLevels = FALSE,
          varLabels = if (is.null(var_labels)) FALSE else TRUE,
          dropEqual = TRUE,
          explain = FALSE)
  
  # convert to tibble
  rownames <- dimnames(table_chr)[[1]]
  table_tibble <-
    table_chr %>% 
    dplyr::as_tibble() %>% 
    dplyr::mutate(" " = rownames) %>% 
    dplyr::select(c(" ",
                    "Training data",
                    "Validation data",
                    "Internal test data",
                    "External test data (EICU)"))
  
  # add number of cases
  n_cases <- 
    data %>%
    dplyr::group_by(datatype) %>% 
    dplyr::summarise(n_cases = length(unique(case_id)))
  
  table_tibble <-
    table_tibble %>% 
    # Add number of patients
    dplyr::add_row(" " = "Nr. of patients",
                   "Training data" = n_cases %>% 
                                       dplyr::filter(datatype == "Training data") %>% 
                                       purrr::pluck("n_cases") %>% 
                                       as.character(),
                   "Validation data" = n_cases %>% 
                                         dplyr::filter(datatype == "Validation data") %>% 
                                         purrr::pluck("n_cases") %>% 
                                         as.character(),
                   "Internal test data" = n_cases %>% 
                                            dplyr::filter(datatype == "Internal test data") %>% 
                                            purrr::pluck("n_cases") %>% 
                                            as.character(),
                   "External test data (EICU)" = n_cases %>% 
                                                  dplyr::filter(datatype == "External test data (EICU)") %>% 
                                                  purrr::pluck("n_cases") %>% 
                                                  as.character(),
                   .before = 2)
  
  # rename n to Nr. of observations
  table_tibble[1, 1] <- "Nr. of observations"
  
  # drop leading/trailing whitespace
  table_tibble <-
    table_tibble %>% 
    dplyr::mutate_all(~trimws(.))
  
  return(table_tibble)

}


t1_mlife_eicu <- function() {
  
  # read the config file
  config <- yaml::read_yaml("config/config.yaml")
  
  # define variables to display
  vars <- c("age", "sex_male", config$targets)
  binary <- c("sex_male")
  continuous <- dplyr::setdiff(vars, binary)
  
  # create the table
  t <- mlife_vs_eicu(all_vars = vars,
                     cont_vars = continuous,
                     cat_vars = binary,
                     var_labels = config$var_labels)
  
}
