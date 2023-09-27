#' Assigns a y-label for a plot based on the target type and metric to plot
.y_label <- function(target, metric) {
  
  # default: no label
  ylab <- ""
  
  # for specific targets (indicated by position in the plot):
  # assign a label based on the metric to plot
  if (target %in% c("blood_pressure_systolic",
                    "blood_pressure_mean",
                    "oxygen_saturation")) {
    
    if (metric == "mape") {
      ylab <- "Mean Absolute Percentage Error" 
    }
    
    if (metric == "rmse") {
      ylab <- "Root Mean Squared Error" 
    }
    
    if (metric == "mae") {
      ylab <- "Mean Absolute Error" 
    }
    
    if (metric == "bias") {
      ylab <- "Mean Error" 
    }
    
    if (metric == "trend_identification") {
      ylab <- "Correctly identified trend (%)" 
    }
    
    if (metric == "rmse_rel_diff") {
      
      ylab <- "Improvement on the Naive RMSE (%)"
      
    }
    
  }
  
  return(ylab)

}


#' Assigns a name to a target type
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


#' @param error_df A DataFrame containing the error metric per target_type, model_name, and minutes_ahead
#' @param target The desired target type
#' @param ylab The desired ylabel
#' @return A beautiful plot
.barplot_single_target <- function(error_df,
                                   target,
                                   ylab,
                                   y_min = 0,
                                   legend_position = "none",
                                   model_labels = NULL) {
  
  n_models_to_plot <- length(model_labels)
  
  # make sure to keep same colors between absolute plots including naive model
  # and plots of performance relative to the naive model
  model_colors <- heat.colors(n_models_to_plot)
  
  if (!"naive" %in% names(model_labels)) {
    
    model_colors <- tail(model_colors, n_models_to_plot)
    
  }

  error_df <-
    error_df %>% 
    dplyr::filter(target_type == target) %>% 
    dplyr::mutate(model_name_label = purrr::map(model_name, ~purrr::pluck(model_labels, .)))
  
  error_df %>% 
    dplyr::mutate(minutes_ahead = as.factor(minutes_ahead)) %>% 
    ggplot2::ggplot(aes(x = minutes_ahead,
                        y = metric,
                        fill = model_name)) +
    ggplot2::geom_bar(stat = "identity",
                      position = "dodge2") +
    # add star on top of best model
    ggplot2::geom_text(aes(label = label,
                           group = model_name),
                       position = position_dodge(width = .9),
                       vjust = -0.75,
                       size = 4,
                       fontface = "bold") +
    # write metric into the plot
    ggplot2::geom_text(aes(label = round(metric, 1),
                           group = model_name,
                           vjust = dplyr::if_else(metric >= 0, -0.1, 1.1)),
                       position = position_dodge(width = .9),
                       size = 1.15,
                       # vjust = -0.1,
                       fontface = "bold") +
    # write model name into the plot (if needed)
    ggplot2::geom_text(aes(y = 0.025 * min(error_df$metric),
                           label = if (legend_position == "none") model_name_label else "",
                           group = model_name),
                       position = position_dodge(width = .9),
                       size = 1.75,
                       hjust = "left",
                       angle = 90,
                       fontface = "bold") +
    ggplot2::xlab("Minutes ahead") + 
    ggplot2::ylab(ylab) +
    ggplot2::scale_y_continuous(limits = c(dplyr::if_else(is.null(y_min), 1.025 * min(error_df$metric), y_min),
                                           1.05 * max(error_df$metric)),
                                n.breaks = 10) +
    ggplot2::scale_fill_manual("",
                               labels = model_labels,
                               values = model_colors) +
    ggplot2::labs(title=.assign_target_name(target)) +
    ggplot2::theme_light() +
    ggplot2::labs(title=.assign_target_name(target)) +
    ggplot2::theme(legend.position = legend_position,
                   legend.key.size = unit(0.3, "cm"),
                   legend.direction = "horizontal",
                   legend.margin = margin(0, 0, 0, 0),
                   legend.text=element_text(size=6.5),
                   plot.title = element_text(face = "bold",
                                             size = 10, 
                                             hjust = 0.5),
                   axis.text.x = element_text(face = "bold",
                                              size = 9),
                   axis.title.y = element_text(face = "bold",
                                               size = 9)) +
    ggplot2::guides(fill = ggplot2::guide_legend(nrow = 1))
  
}


#' @param error_df A DataFrame containing the MAPE per target_type, model_name, and minutes_ahead
#' @param target All target types to plot
#' @param metric The error metric
#' @return A list of beautiful plots
.barplot_all_targets <- function(error_df, 
                                 target_types,
                                 metric,
                                 model_labels,
                                 legend,
                                 y_min = 0) {
  
  # create an empty list to be filled with plots for each target type
  plot_list <- list()
  
  for (t in target_types) {
    
    # define the y-axis label
    ylab <- .y_label(target = t,
                     metric = metric)
    
    
    # define the legend position
    legend_position <- if (legend) c(0.5, 0.025) else "none"
    
    # append the plot list
    plot_list[[t]] <- .barplot_single_target(error_df = error_df,
                                             target = t,
                                             ylab = ylab,
                                             legend_position = legend_position,
                                             model_labels = model_labels,
                                             y_min = y_min)
      
    
  }
  
  return(plot_list)
  
}


#' A wrapper for cowplot's plot_grid function.
#' @param target_plots A list of plots, one for each target
#' @param nrow The desired number of rows
#' @param ncol The desired number of columns
#' @return A plot grid
.plot_all_targets <- function(target_plots,
                              nrow = 2,
                              ncol = 3,
                              output_file = NULL){
  
  plot <- cowplot::plot_grid(plotlist = target_plots,
                             nrow = nrow,
                             ncol = ncol)
  
  if (!is.null(output_file)) {
    ggplot2::ggsave(output_file,
                    dpi = 1000,
                    width = 10,
                    height = 14.1)
  }
  
  return(plot)
  
}



#' Produces a bar plot per target type
#' for the desired dataset (internal or eicu)
#' @param errors_df A dataframe of forecast metrics
#' @param eicu_or_test Whether to plot EICU or test data
#' @param file_name_suffix A suffix for the filename
#' @return Nothing, but the plot gets saved
.plot_metrics_all_models <- function(errors_df, eicu_or_test, file_name_suffix = "") {
  
  message("Plotting error metrics for ", eicu_or_test, " data.")
  
  # sub-set to the desired errors
  errors <- 
    errors_df %>% 
    # filter for eicu or internal test set
    dplyr::filter(dataset_name == eicu_or_test) %>% 
    # filter for desired forecast horizons
    dplyr::filter(minutes_ahead %in% config$minutes_ahead_to_plot) %>% 
    # filter for desired ts models
    dplyr::filter(model_name %in% config$ts_models_to_plot) %>% 
    # add a hack to make sure model names remain in proper order when plotting
    dplyr::mutate(model_name = factor(model_name, levels = config$ts_models_to_plot))
  
  
  # loop over the desired error metrics
  for (m in c("rmse", "trend_identification")) {

    message("  Plotting ", m)
    
    errors <- 
      errors %>% 
      # rename the metric to "metric"
      dplyr::mutate(metric = !!rlang::sym(m)) %>% 
      # indicate the best model with a *
      dplyr::group_by(target_type, minutes_ahead) %>% 
      dplyr::mutate(min_metric = min(metric),
                    max_metric = max(metric)) %>% 
      dplyr::mutate(best_metric = dplyr::case_when(endsWith(m, "_identification")~max_metric,
                                                   TRUE~min_metric)) %>% 
      dplyr::mutate(label = dplyr::case_when(metric == best_metric~"*", TRUE~""))
    
    # create a list of plots per target type
    target_plots <- .barplot_all_targets(error_df = errors,
                                         target_types = config$targets,
                                         metric = m,
                                         legend = FALSE,
                                         model_labels = config$model_name_labels[config$ts_models_to_plot])
    
    # plot them all
    .plot_all_targets(target_plots = target_plots,
                      nrow = 3,
                      ncol = 2)
    
    # save the plot
    directory <- paste0("evaluation/plots/performance/", Sys.Date())
    .create_plot_directory(directory)
    
    ggplot2::ggsave(paste0(directory,
                           "/",
                           m,
                           "_",
                           eicu_or_test,
                           file_name_suffix,
                           ".png"),
                    dpi = 1000,
                    width = 10,
                    height = 14.1)
    
  }
  
}


#' Produces a bar plot per target type
#' for the desired dataset (internal or eicu)
#' @param errors_df A dataframe of forecast metrics
#' @param eicu_or_test Whether to plot EICU or test data
#' @return Nothing, but the plot gets saved
.plot_metric_vs_naive_metric <- function(errors_df, eicu_or_test, file_name_suffix = "") {
  
  message("Plotting comparison to naive model for ", eicu_or_test, " data.")
  
  # sub-set to the desired errors
  errors <- 
    errors_df %>% 
    # filter for eicu or internal test set
    dplyr::filter(dataset_name == eicu_or_test) %>% 
    # filter for desired forecast horizons
    dplyr::filter(minutes_ahead %in% config$minutes_ahead_to_plot) %>% 
    # filter for desired ts models
    dplyr::filter(model_name %in% config$ts_models_to_plot) %>% 
    # add a hack to make sure model names remain in proper order when plotting
    dplyr::mutate(model_name = factor(model_name, levels = config$ts_models_to_plot))
  
  # add the naive performance
  errors_naive <-
    errors %>% 
    dplyr::filter(model_name == "naive") %>% 
    dplyr::rename(rmse_naive = rmse) %>% 
    dplyr::select(dataset_name,
                  target_type,
                  minutes_ahead,
                  rmse_naive)
  
  errors <-
    errors %>% 
    dplyr::filter(model_name != "naive") %>% 
    dplyr::left_join(errors_naive, by = c("dataset_name",
                                          "target_type",
                                          "minutes_ahead")) %>% 
    dplyr::select(dataset_name, 
                  target_type,
                  minutes_ahead,
                  model_name,
                  rmse,
                  rmse_naive) %>% 
    # compute relative and absolute differences to the naive performance
    dplyr::mutate(rmse_diff = rmse_naive - rmse,
                  rmse_rel_diff = 100 * (rmse_naive - rmse) / rmse_naive)
  
  
  # loop over the desired error metrics
  for (m in c("rmse_rel_diff")) {
    
    message("  Plotting ", m)
    
    errors <- 
      errors %>% 
      # rename the metric to "metric"
      dplyr::mutate(metric = !!rlang::sym(m)) %>% 
      # indicate the best model with a *
      dplyr::group_by(target_type, minutes_ahead) %>% 
      dplyr::mutate(min_metric = min(metric),
                    max_metric = max(metric)) %>% 
      dplyr::mutate(best_metric = max_metric) %>% 
      dplyr::mutate(label = dplyr::case_when(metric == best_metric~"*", TRUE~""))
    
    # create a list of plots per target type
    target_plots <- .barplot_all_targets(error_df = errors,
                                         target_types = config$targets,
                                         metric = m,
                                         model_labels = config$model_name_labels[setdiff(config$ts_models_to_plot, "naive")],
                                         legend = TRUE,
                                         y_min = NULL)
    
    # plot them all
    .plot_all_targets(target_plots = target_plots,
                      nrow = 3,
                      ncol = 2)
    
    # save the plot
    directory <- paste0("evaluation/plots/performance/", Sys.Date())
    .create_plot_directory(directory)
    
    ggplot2::ggsave(paste0(directory,
                           "/",
                           m,
                           "_",
                           eicu_or_test,
                           file_name_suffix,
                           ".png"),
                    dpi = 1000,
                    width = 10,
                    height = 14.1)
    
  }
  
}
