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
      ylab <- "Mean Absolute Percentage Error (%)" 
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


.model_color_scheme <- function(n_models) {
  
  viridis::plasma(n_models, begin = 0.2, end = 1, direction = -1)
  
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
  
  # load information on error bars 
  error_bars <- 
    readr::read_delim("evaluation/tables/rmse_error_bars.csv",
                      delim = ";",
                      show_col_types = FALSE)
  
  # merge error bar information
  error_df <-
    error_df %>% 
    dplyr::left_join(error_bars, by = c("dataset_name",
                                        "model_name",
                                        "target_type",
                                        "minutes_ahead"))
  
  n_models_to_plot <- length(model_labels)
  
  # make sure to keep same colors between absolute plots including naive model
  # and plots of performance relative to the naive model
  model_colors <- .model_color_scheme(n_models_to_plot)
  
  if (!"naive" %in% names(model_labels)) {
    
    model_colors <- tail(model_colors, n_models_to_plot)
    
  }

  error_df <-
    error_df %>% 
    dplyr::filter(target_type == target) %>% 
    dplyr::mutate(model_name = factor(model_name, levels = config$ts_models_to_plot)) %>% 
    dplyr::mutate(model_name_label = purrr::map(model_name, ~purrr::pluck(model_labels, .)))
  
  error_df %>% 
    dplyr::mutate(minutes_ahead = as.factor(minutes_ahead)) %>% 
    ggplot2::ggplot(aes(x = minutes_ahead,
                        y = metric,
                        fill = model_name)) +
    ggplot2::geom_bar(stat = "identity",
                      position = position_dodge2()) +
    # add error bars (CI)
    ggplot2::geom_errorbar(aes(x = minutes_ahead, 
                               ymin = rmse_ci_lower,
                               ymax = rmse_ci_upper),
                           size = 0.06,
                           position = ggplot2::position_dodge2(padding = 0.5)) +
    # add star on top of best model
    ggplot2::geom_text(aes(label = label,
                           group = model_name),
                       position = position_dodge(width = .9),
                       vjust = -0.6,
                       size = 1.25,
                       fontface = "bold") +
    # write metric into the plot
    ggplot2::geom_text(aes(label = sprintf('%.2f', metric),
                           group = model_name,
                           vjust = 0.5,
                           hjust = 2.3),
                       position = position_dodge(width = .9),
                       size = 0.6,
                       angle = 90) +
    # write model name into the plot (if needed)
    ggplot2::geom_text(aes(y = 0.025 * min(error_df$metric),
                           label = if (legend_position == "none") model_name_label else "",
                           group = model_name),
                       position = position_dodge(width = .9),
                       size = 0.75,
                       hjust = "left",
                       angle = 90,
                       fontface = "bold") +
    ggplot2::xlab("Minutes ahead") + 
    ggplot2::ylab(ylab) +
    ggplot2::scale_y_continuous(limits = c(dplyr::if_else(is.null(y_min), 1.025 * min(error_df$metric), y_min),
                                           1.05 * max(error_df$rmse_ci_upper)),
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
                   plot.margin = ggplot2::margin(t = -0.0825,
                                                 r = 0.01,
                                                 b = -0.07,
                                                 l = -0.0825,
                                                 unit = "cm"),
                   legend.margin = margin(0, 0, 0, 0, unit = "cm"),
                   legend.text=element_text(size=6.5),
                   plot.title = element_text(face = "bold",
                                             size = 5, 
                                             hjust = 0.5,
                                             vjust = -3),
                   axis.text.y = element_text(size = 4),
                   axis.text.x = element_text(size = 4),
                   axis.title.y = element_text(face = "bold",
                                               size = 5,
                                               vjust = -1.5),
                   axis.title.x = element_text(size = 4,
                                               vjust = 3)) +
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
                             ncol = ncol,
                             scale = 1)
    
    
  
  if (!is.null(output_file)) {
    ggplot2::ggsave(output_file,
                    dpi = 750,
                    width = 7,
                    height = 12)
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
  for (m in c("rmse")) {

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
    
    file <- paste0(directory,
                   "/",
                   m,
                   "_",
                   eicu_or_test,
                   file_name_suffix,
                   ".tiff")
    
    ggplot2::ggsave(file,
                    dpi = 1000,
                    width = 9,
                    height = 12,
                    units = "cm",
                    compression = "lzw",
                    type = "cairo")
    
  }
  
}


.plot_mape_across_all <- function(mape_df, eicu_or_test, file_name_suffix = "") {
  
  message("Plotting MAPE across all targets and horizons for ", eicu_or_test, " data.")
  
  model_labels <- config$model_name_labels[config$ts_models_to_plot]
  n_models_to_plot <- length(model_labels)
  model_colors <- .model_color_scheme(n_models_to_plot)
  
  # load error bar information
  mape_error_bars <-
    readr::read_delim("evaluation/tables/mape_error_bars.csv",
                      delim = ";",
                      show_col_types = FALSE)
  
  mape_df <-
    mape_df %>% 
    dplyr::left_join(mape_error_bars, by = c("dataset_name", "model_name"))
  
  # sub-set to the desired errors
  errors <- 
    mape_df %>% 
    # filter for eicu or internal test set
    dplyr::filter(dataset_name == eicu_or_test) %>% 
    # filter for desired ts models
    dplyr::filter(model_name %in% config$ts_models_to_plot) %>% 
    # add a hack to make sure model names remain in proper order when plotting
    dplyr::mutate(model_name = factor(model_name, levels = config$ts_models_to_plot)) %>% 
    dplyr::mutate(model_name_label = purrr::map(model_name, ~purrr::pluck(model_labels, .)))
  
  errors <- 
    errors %>% 
    # label the best model
    dplyr::mutate(label = dplyr::case_when(mape == min(errors$mape) ~ "*",
                                           TRUE ~ ""))
  
  # produce a barplot
  errors %>% 
    ggplot2::ggplot(aes(x = model_name,
                        y = mape,
                        fill = model_name)) +
    ggplot2::geom_bar(stat = "identity",
                      position = "dodge2") +
    ggplot2::geom_errorbar(aes(x = model_name,
                               ymin = mape_ci_lower,
                               ymax = mape_ci_upper),
                           size = 0.2,
                           width = 0.5,
                           position = position_dodge2()) +
    ggplot2::scale_y_continuous(n.breaks = 10) +
    ggplot2::scale_fill_manual("",
                               labels = model_labels,
                               values = model_colors) + 
    ggplot2::theme_light() +
    ggplot2::theme(legend.position = "none",
                   axis.text.y = element_text(size = 3),
                   axis.text.x = element_blank(),
                   axis.title.y = element_text(face = "bold",
                                               size = 5.5),
                   plot.margin = margin(1,1,-12,1)) +
    # write value into plot
    ggplot2::geom_text(aes(label = sprintf('%.2f', mape),
                           group = model_name,
                           vjust = 2.0,
                           hjust = 0.5),
                       position = position_dodge(width = .9),
                       size = 1.5) +
    # write model name into the plot (if needed)
    ggplot2::geom_text(aes(y = 0.075,
                           label = model_name_label),
                       position = position_dodge(width = .9),
                       size = 1.75,
                       hjust = "left",
                       angle = 90,
                       fontface = "bold") +
    # add star on top of best model
    ggplot2::geom_text(aes(label = label),
                       position = position_dodge(width = .9),
                       vjust = 0.15,
                       size = 4,
                       fontface = "bold") +
    ggplot2::ylab(.y_label("blood_pressure_systolic", "mape")) +
    ggplot2::xlab("")
  
  # save the plot
  directory <- paste0("evaluation/plots/performance/", Sys.Date())
  .create_plot_directory(directory)
  
  ggplot2::ggsave(paste0(directory,
                         "/mape_across_all_",
                         eicu_or_test,
                         file_name_suffix,
                         ".tiff"),
                  dpi = 1000,
                  units = "cm",
                  width = 7,
                  height = 7,
                  compression = "lzw",
                  type = "cairo")
  
}


.plot_mape_by_hour <- function(mape_df_by_hour,
                               eicu_or_test,
                               mins_ahead) {
  
  mape_df_by_hour <-
    mape_df_by_hour %>% 
    dplyr::filter(dataset_name == eicu_or_test,
                  minutes_ahead == mins_ahead,
                  hours_since_admission <= 23) %>% 
    dplyr::mutate(label_hours = purrr::map_chr(hours_since_admission, ~paste0('[', .x, ' - ', .x + 1, ')')))
  
  # define which models to plot and how to color them 
  model_labels <- config$model_name_labels[config$ts_models_to_plot]
  n_models_to_plot <- length(model_labels)
  model_colors <- .model_color_scheme(n_models_to_plot)
  
  # define x-ticks and their labels
  break_rhythm <- c(TRUE, FALSE)
  x_ticks <- seq(0, 23, 1)[break_rhythm]
  x_labels <- unique(mape_df_by_hour$label_hours)[break_rhythm]
  
  mape_df_by_hour %>% 
    dplyr::filter(model_name %in% config$ts_models_to_plot) %>% 
    dplyr::mutate(model_name = factor(model_name, levels=config$ts_models_to_plot)) %>% 
    ggplot2::ggplot(aes(x = hours_since_admission, y = mape, color = model_name)) +
    ggplot2::geom_line(size = 0.2) +
    ggplot2::geom_point(size = 0.05) +
    ggplot2::labs(x = "Hours since admission",
                  y = if (mins_ahead %in% c(5, 15, 25)) "Mean Absolute Percentage Error (%)" else "",
                  title = paste0(mins_ahead, " minutes ahead")) +
    ggplot2::scale_color_manual(values = model_colors, labels = model_labels) +
    ggplot2::scale_y_continuous(limits = c(0, 6.25), n.breaks = 8) + 
    ggplot2::scale_x_continuous(breaks = x_ticks, label = x_labels) +
    ggplot2::theme_light() +
    ggplot2::theme(legend.title = ggplot2::element_blank(),
                   axis.title.x = ggplot2::element_text(size = 4,
                                                        vjust = 4.5),
                   axis.title.y = ggplot2::element_text(face = "bold",
                                                        size = 4,
                                                        vjust = -2),
                   axis.text.x = ggplot2::element_text(vjust = 1.25,
                                                       hjust = 0.6,
                                                       size = 2.5,
                                                       angle = 30),
                   axis.text.y = ggplot2::element_text(size = 2.5),
                   plot.title = element_text(face = "bold",
                                             size = 4, 
                                             hjust = 0.5,
                                             vjust = -5),
                   legend.text = ggplot2::element_text(size = 2.5),
                   legend.position = c(0.5, 0.75),
                   # legend.position = "none",
                   legend.spacing.x = unit(0, "mm"),
                   legend.spacing.y = unit(0, "mm"),
                   legend.key.size = unit(3.5, "mm"),
                   legend.margin = margin(0, 0, 0, 0, unit = "mm"),
                   plot.margin = ggplot2::margin(t = -0.0825,
                                                 r = 0.01,
                                                 b = -0.07,
                                                 l = -0.0825,
                                                 unit = "cm")) +
    ggplot2::guides(color = ggplot2::guide_legend(nrow = 3,
                                                  override.aes = list(size = 0.15)))
  
}


.plot_mape_across_all_by_hour_since_admission <- function(mape_df_by_hour,
                                                          eicu_or_test,
                                                          mins_ahead,
                                                          file_name_suffix = "") {
  
  message("Plotting MAPE across all targets and horizons for "
          , eicu_or_test,
          " data grouped by hours since admission")
  
  list_of_plots <- 
    seq(5, 30, 5) %>% 
    purrr::map(~.plot_mape_by_hour(mape_df_by_hour = mape_df_by_hour, 
                                   eicu_or_test = eicu_or_test,
                                   mins_ahead = .))
  
  cowplot::plot_grid(plotlist = list_of_plots,
                     nrow = 3,
                     ncol = 2,
                     scale = 1) 
  
  # save the plot
  directory <- paste0("evaluation/plots/performance/", Sys.Date())
  .create_plot_directory(directory)
  file <- paste0(directory,
                 "/mape_across_all__by_hour__",
                 eicu_or_test,
                 file_name_suffix,
                 ".tiff")
  
  ggplot2::ggsave(file,
                  dpi = 1000,
                  width = 9,
                  height = 12,
                  units = "cm",
                  compression = "lzw",
                  type = "cairo")
  
  
}



