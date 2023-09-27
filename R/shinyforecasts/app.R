# This shiny app displays forecasts produced by the code in this repo
# Work in progress, especially the data loading part is not very efficient

library(shiny)
library(ggplot2)
library(cowplot)

# Set the working directory to the root directory of the project
setwd("../..")
source("R/setup.R")
source("R/shinyforecasts/plots.R")
source("R/shinyforecasts/utils.R")

# Load data and make global definitions ----
# TODO make more efficient by loading only the desired number of cases
data <- .load_predictions(cases_per_dataset = 5)

# case_id
case_ids <- unique(data$case_id)
default_id <- min(case_ids)

# datetime at forecast
default_forecast_times <- 
  data %>% 
  dplyr::filter(case_id == default_id) %>% 
  purrr::pluck("datetime_at_forecast")

default_min_forecast_time <- min(default_forecast_times)
default_max_forecast_time <- max(default_forecast_times)
default_value_forecast_time <- min(default_forecast_times)
default_step <- 5 * 60

# ts models
ts_models <- unique(data$model_name)
default_ts_model <- "naive"

# y-limits per case (to keep the axis constant)
ylims_all_cases <- 
  data %>% 
  dplyr::group_by(case_id, target_type) %>% 
  dplyr::summarise(min_true = min(target_value),
                   max_true = max(target_value),
                   min_forecast = quantile(prediction, 0.015),
                   max_forecast = quantile(prediction, 0.985),
                   .groups = "keep")


################################################################################
# Define the UI function
################################################################################
ui <- shiny::fluidPage(
  
  shiny::tabsetPanel(
    
    ############################################################################
    # PANEL 1: Plots individual forecasts vs. true values
    ############################################################################
    shiny::tabPanel(
      
      title = "Individual Forecasts",
      fluid = TRUE,
      
      # Top-bar layout with input and output definitions
      shiny::fluidRow(
        
        shiny::column(
          
          # Input: The desired case_id ----
          width = 2,
          selectInput(inputId = "case_id",
                      label = "Case ID",
                      choices = case_ids,
                      selected = default_id)
          
        ),
        
        shiny::column(
          
          # Input: The desired TS models
          width = 1,
          .ts_models_checkbox(input_id = "ts_models",
                              ts_models = ts_models)
          
        ),

        shiny::column(
          
          # Input: The amount of past data (in hours) to display
          width = 3,      
          offset = 1,
          sliderInput(inputId = "past_hours",
                      label = "Displayed past (h)",
                      min = 0,
                      max = 12,
                      value = 5,
                      step = 1)
          
        ),
        shiny::column(
          
          # Input: The time the forecast was produced
          width = 5,
          div(style = "display: inline-block;vertical-align:text-bottom;",
              actionButton(inputId = "forecast_time__previous",
                           label = "-")),
          div(style = "display: inline-block;vertical-align:center;",
              sliderInput(inputId = "forecast_time",
                          label = "Forecast time",
                          min = default_min_forecast_time,
                          max = default_max_forecast_time,
                          value = default_value_forecast_time,
                          step = default_step)),
          div(style = "display: inline-block;vertical-align:text-bottom;",
              actionButton(inputId = "forecast_time__next",
                           label = "+"))
        ),
        
        # Main panel of figures ----
        plotOutput(outputId = "forecastPlots",
                   width = "95%",
                   height = "800px")
      )
    )
  )
)


################################################################################
# Define the server function
################################################################################
server <- function(input, output, session) {
  
  # Set the working directory to the root directory of the project
  setwd("../..")
  
  ##############################################################################
  # PANEL 1: Plots individual forecasts vs. true values
  ##############################################################################
  # Function to sub-set the data to the desired case_id and forecast time
  data_case_filtered <- shiny::reactive({
    
    case <- input$case_id
    past_hours <- input$past_hours
    datetime_start <- input$forecast_time - lubridate::hours(past_hours)
    datetime_end <- input$forecast_time + lubridate::hours(2)
    
    data_case <- 
      data %>% 
      dplyr::filter(case_id == case &
                    datetime >= datetime_start &
                    datetime <= datetime_end)

  })
  
  # Function returning the available forecast times for a given case_id
  forecast_time_choices <- shiny::reactive({
    
    data %>%
      dplyr::filter(case_id == input$case_id) %>%
      purrr::pluck("datetime_at_forecast") %>%
      unique()
    
  })
  
  # Function returning the y-lims per target for a given case_id
  ylims_per_target_type <- shiny::reactive({
    
    ylims_all_cases %>% 
      dplyr::filter(case_id == input$case_id)
    
  })
  
  
  # Plotting of individual forecasts
  output$forecastPlots <- shiny::renderPlot({
    
    data_case <- data_case_filtered()
    
    data_true <- 
      data_case %>%
      dplyr::group_by(case_id, datetime, target_type) %>% 
      dplyr::summarise(target_value = median(target_value), .groups = "keep")

    data_forecasts <- 
      data_case %>% 
      dplyr::filter(datetime_at_forecast == input$forecast_time & 
                    datetime > input$forecast_time) %>% 
      dplyr::filter(model_name %in% input$ts_models)
    
    .vitals_plots(data_true = data_true,
                  data_forecasts = data_forecasts,
                  forecast_time = input$forecast_time,
                  ylims_per_target_type = ylims_per_target_type())
  
  })
  
  # Function to adjust the forecast time slider to the desired case_id
  shiny::observe({

    forecast_times <- forecast_time_choices()
    default <- forecast_times[[
      floor(length(forecast_times) / 2)
    ]]
    updateSliderInput(session = session,
                      inputId = "forecast_time",
                      min = min(forecast_times),
                      max = max(forecast_times),
                      value = default)

  })
  
  # Adjust the forecast time slider to the action buttons
  shiny::observeEvent(input$forecast_time__next, {
    updateSliderInput(session = session,
                      inputId = "forecast_time", 
                      value = input$forecast_time + lubridate::minutes(5))
  })
  shiny::observeEvent(input$forecast_time__previous, {
    updateSliderInput(session = session,
                      inputId = "forecast_time", 
                      value = input$forecast_time - lubridate::minutes(5))
  })
}


################################################################################
# Run the shiny app
################################################################################
shiny::shinyApp(ui = ui, server = server)
