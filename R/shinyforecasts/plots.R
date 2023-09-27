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


#' Plots past true data and future true data vs. forecasts
#' for a single target type.
#' @param data_true A dataframe containing true data
#' @param data_forecasts A dataframe containing forecasts
#' @param target The desired target type to plot
#' @param forecast_time The time the forecast was produced
#' @param ylim A two-element vector specifying ymin, ymax
#' @param legend_position The desired legend position
#' @return A beautiful ggplot
.vitals_plot_single_target <- function(data_true,
                                       data_forecasts,
                                       target,
                                       forecast_time,
                                       ylim,
                                       legend_position, 
                                       legend_direction = "horizontal",
                                       col_map,
                                       model_names_map,
                                       linewidth = 0.5,
                                       pointsize = 0.75) {
  
  
  # Sub-set the true data to the desired target
  data_true_target <- 
    data_true %>% 
    dplyr::filter(target_type == target)
  
  # Sub-set the forecasts to the desired targets
  data_forecasts_target <- 
    data_forecasts %>% 
    dplyr::filter(target_type == target)
  
  # Specify whether to show every other or every fourth datetime on the x-axis
  break_rhythm <- c(TRUE, rep(FALSE, 1 + 2 * (nrow(data_true_target) > 72)))
  
  data_true_target %>% 
    # Plot the true data
    ggplot2::ggplot(aes(x = datetime,
                        y = target_value)) +
    ggplot2::theme_light() +
    ggplot2::geom_point(size = pointsize) +
    ggplot2::geom_line(size = linewidth) +
    # Indicate the time the forecast was produced with a vertical line
    {if(data_forecasts %>% nrow() > 0)
    ggplot2::geom_vline(xintercept = forecast_time,
                        color = "black",
                        alpha = 0.5)
    } +
    ggplot2::ylab("") + 
    ggplot2::xlab("") + 
    # Define the x-ticks
    ggplot2::scale_x_datetime(
      breaks = data_true_target$datetime[break_rhythm], 
      minor_breaks = data_true_target$datetime[break_rhythm],
      date_labels = "%H:%M"
    ) + 
    # Define the number of y-ticks
    ggplot2::scale_y_continuous(limits = ylim, n.breaks = 10) +
    # Define the orientation of x-ticks and legend position
    ggplot2::theme(axis.text.x = element_text(angle = 90,
                                              hjust = 0.5,
                                              vjust = 0.5),
                   legend.position = legend_position,
                   legend.direction = legend_direction,
                   legend.title = element_blank(),
                   legend.key.size = unit(0.5, "cm")) + 
    # Add forecasts to the plot
    ggplot2::geom_line(data = data_forecasts_target, 
                       aes(x = datetime,
                           y = prediction,
                           group = model_name,
                           color = model_name),
                       size = linewidth) + 
    ggplot2::geom_point(data = data_forecasts_target, 
                        aes(x = datetime,
                            y = prediction,
                            group = model_name,
                            color = model_name),
                        size = pointsize) +
    ggplot2::scale_color_manual(values = col_map, labels = model_names_map) +
    # Define the plot title
    ggplot2::labs(title=.assign_target_name(target)) +
    ggplot2::theme(plot.title = element_text(face="bold",
                                             size = 12,
                                             hjust = 0.5))
}


#' Produces a plot comparing true values and forecasts for each target type.
#' @param data_true A dataframe containing true data
#' @param data_forecasts A dataframe containing forecasts
#' @param forecast_time The time the forecast was produced
#' @param ylims_per_target_type A dataframe indicating the max for each target
.vitals_plots <- function(data_true,
                          data_forecasts,
                          forecast_time,
                          ylims_per_target_type,
                          targets = NULL,
                          nrow = 2,
                          ncol = 3,
                          legend_position = c(0.5, 0.075),
                          legend_direction = "horizontal",
                          output_file = NULL,
                          linewidth = 0.5,
                          pointsize = 0.75) {
  
  # Get a list of targets to plot
  if (is.null(targets)) {
    targets <- unique(data_true$target_type)
  }
  
  # Define a list containing all plots to be produced
  target_plots <- list()
  
  # Define colors for different ts models
  col_map <- 
    yaml::read_yaml("config/config.yaml") %>% 
    purrr::pluck("ts_model_colours") %>% 
    unlist()
  
  # Define labels for different ts models
  model_names_map <-
    yaml::read_yaml("config/config.yaml") %>% 
    purrr::pluck("model_name_labels") %>% 
    unlist()
  
  # sub-set to the forecast models requested
  col_map <- col_map[unique(data_forecasts$model_name)]
  
  # Loop over all targets
  for(target in targets) {
    
    # Define the y-axis scale
    ylim <- 
      ylims_per_target_type %>%
      dplyr::filter(target_type == target)
    
    ylim <- c(0.90 * min(ylim$min_true, ylim$min_forecast),
              1.05 * max(ylim$max_true, ylim$max_forecast))
    
    # define legend position
    lp <- "none"
    if (target %in% c("blood_pressure_mean", "oxygen_saturation")) {
      lp <- legend_position
    }
    
    # Assign to the list of target plots a plot for the current target
    target_plots[[target]] <- 
      .vitals_plot_single_target(data_true = data_true,
                                 data_forecasts = data_forecasts,
                                 target = target,
                                 forecast_time = forecast_time,
                                 ylim = ylim,
                                 legend_position = lp,
                                 legend_direction = legend_direction,
                                 col_map = col_map,
                                 model_names_map = model_names_map,
                                 linewidth = linewidth,
                                 pointsize = pointsize)
    
  }
  
  # Produce a plot of all targets
  .plot_all_targets(target_plots, nrow, ncol, output_file)

}


#' Produces a plot for a desired target type: 
#' An accuracy metric vs. the forecast horizon in minutes
#' @param data A dataframe containing forecasts and true values
#' @param target The target type in question
#' @param ymax The ymax of the plot
#' @param legend_position The legend position
#' @return A beautiful ggplot
.metrics_per_minutes_ahead_single_target <- function(data,
                                                     target,
                                                     ymax,
                                                     ylabel,
                                                     legend_position,
                                                     col_map) {
  
  data %>% 
    # Sub-set to the desired target type
    dplyr::filter(target_type == target) %>% 
    ggplot2::ggplot(aes(x = minutes_ahead,
                        y = metric, 
                        groups = model_name,
                        color = model_name)) +
    ggplot2::geom_line() + 
    ggplot2::scale_color_manual(values = col_map) +
    ggplot2::scale_y_continuous(limits = c(0, ymax), n.breaks = 10) +
    ggplot2::scale_x_continuous(n.breaks = 12) +
    ggplot2::xlab("Forecast minutes ahead") + 
    ggplot2::ylab(ylabel) +
    ggplot2::theme_light() +
    ggplot2::theme(legend.position = legend_position,
                   legend.direction = "vertical",
                   legend.title = element_blank()) + 
    ggplot2::labs(title=.assign_target_name(target)) +
    ggplot2::theme(plot.title = element_text(face="bold",
                                             size = 12,
                                             hjust = 0.5))

}


#' Plots for all target types an accuracy metric vs. the forecast horizon
#' @param data_metrics A dataframe containing accuracy metrics
#' @param ymax_per_target_type A dataframe indicating the ymax per target
#' @return A plot per target type
.metrics_per_minutes_ahead <- function(data_metrics,
                                       ymax_per_target_type) {
  
  # Select all unique target types to plot
  targets <- unique(data_metrics$target_type)
  
  # Create an empty list to fill with a plot per target type
  target_plots <- list()
  
  # Define colors for different ts models
  col_map <- 
    yaml::read_yaml("config/config.yaml") %>% 
    purrr::pluck("ts_model_colours") %>% 
    unlist()
  
  # sub-set to the forecast models requested
  col_map <- col_map[unique(data_metrics$model_name)]
  
  # Loop over all target types
  for(target in targets) {
    
    # Define the legend position
    legend_position <- 
      if (target == dplyr::first(targets)) c(0.125, 0.85) else "none"
    
    # Define the ymax of the plot
    ymax <- 
      ymax_per_target_type %>% 
      dplyr::filter(target_type == target) %>% 
      purrr::pluck("max_metric") %>% 
      max()
    
    # Define the ylabel
    ylabel <- .infer_ylabel(data_metrics)
    
    # Add a plot for the current target to the list of plots
    target_plots[[target]] <- 
      .metrics_per_minutes_ahead_single_target(
        data = data_metrics,
        target = target,
        ymax = ymax,
        ylabel = ylabel,
        legend_position = legend_position,
        col_map = col_map
    )
    
  }
  
  # Wrap all plots into one large figure
  .plot_all_targets(target_plots)
  
}


#' Returns the appropriate xlabel for a desired number of full hours observed
#' @param i An integer
#' @return A string: [i, i + 1)
.hours_observed_xlab <- function(i) {
  
  paste0("[", i, " ,", i + 1, ")")
  
}


#' Produces a plot for a desired target type: 
#' An accuracy metric vs. the number of full hours since admission
#' @param data A dataframe containing forecasts and true values
#' @param target The target type in question
#' @param ymax The ymax of the plot
#' @param legend_position The legend position
#' @return A beautiful ggplot
.metrics_per_hour_since_admission_single_target <- function(data, 
                                                            target,
                                                            ymax,
                                                            ylabel,
                                                            legend_position,
                                                            col_map) {
  
  xbreaks <- seq(0, 23, 2)
  
  data %>% 
    # Sub-set to the desired target type
    dplyr::filter(target_type == target) %>% 
    ggplot2::ggplot(aes(x = full_hours_since_admission,
                        y = metric, 
                        groups = model_name,
                        color = model_name)) +
    ggplot2::scale_color_manual(values = col_map) +
    ggplot2::geom_line() + 
    ggplot2::scale_y_continuous(limits = c(0, ymax), n.breaks = 10) +
    ggplot2::scale_x_continuous(limits = c(0, 23),
                                breaks = xbreaks,
                                labels = sapply(xbreaks,
                                                .hours_observed_xlab)) +
    ggplot2::xlab("Hours since admission") + 
    ggplot2::ylab(ylabel) +
    ggplot2::theme_light() +
    ggplot2::theme(legend.position = legend_position,
                   legend.direction = "vertical",
                   legend.title = element_blank()) + 
    ggplot2::labs(title=.assign_target_name(target)) +
    ggplot2::theme(plot.title = element_text(face="bold",
                                             size = 12,
                                             hjust = 0.5),
                   axis.text.x = element_text(angle = 60,
                                              hjust = 0.5,
                                              vjust = 0.5))
  
}


#' Plots for all target types an accuracy metric vs. 
#' the number of full hours since admission
#' @param data_metrics A dataframe containing accuracy metrics
#' @param ymax_per_target_type A dataframe indicating the ymax per target
#' @return A plot per target type
.metrics_per_hours_since_admission <- function(data_metrics,
                                               ymax_per_target_type) {
  
  # Select all target types to be plotted
  targets <- unique(data_metrics$target_type)
  
  # Create an empty list to be filled with a plot per target
  target_plots <- list()
  
  # Define colors for different ts models
  col_map <- 
    yaml::read_yaml("config/config.yaml") %>% 
    purrr::pluck("ts_model_colours") %>% 
    unlist()
  
  # sub-set to the forecast models requested
  col_map <- col_map[unique(data_metrics$model_name)]
  
  # Loop over all targets
  for(target in targets) {
    
    # Define the legend position
    legend_position <- 
      if (target == dplyr::first(targets)) c(0.125, 0.85) else "none"
    
    # Define the ymax
    ymax <- 
      ymax_per_target_type %>% 
      dplyr::filter(target_type == target) %>% 
      purrr::pluck("max_metric") %>% 
      max()
    
    # Define the ylabel
    ylabel <- .infer_ylabel(data_metrics)
    
    # Add the target plot to the list
    target_plots[[target]] <- 
      .metrics_per_hour_since_admission_single_target(
        data = data_metrics,
        target = target,
        ymax = ymax,
        ylabel = ylabel,
        legend_position = legend_position,
        col_map = col_map
    )
  }
  
  # Wrap all plots into a single figure
  .plot_all_targets(target_plots)

}


#' Infers the correct ylabel from a dataframe containing metrics
#' @param data_metris A dataframe containing accuracy metrics
#' @return A string: the desired label
.infer_ylabel <- function(data_metrics) {
  
  if (all(data_metrics$metric == data_metrics$mae)) {
    
    return("Mean Absolute Error")
    
  } else if (all(data_metrics$metric == data_metrics$rmse)) {
    
    return("Root Mean Squared Error")
    
  } else if (all(data_metrics$metric == data_metrics$mape)) {
    
    return("Mean Absolute Percentage Error")
    
  } else {
    
    stop("Unable to infer the accuracy metric.")
    
  }
  
}


#' Plots MAPE for all models across targets
#' for a desired forecast horizon
#' @param metrics_df A dataframe containing accuracy metrics
#' @param mins_ahead The desired forecast horizon
#' @param ymax The display limit on the MAPE axis
#' @return A bar plot of MAPEs per forecast model
.mape_bar_plot <- function(metrics_df, mins_ahead, ymax, sorted) {
  
  # load the desired colors per model
  col_map <- 
    yaml::read_yaml("config/config.yaml") %>% 
    purrr::pluck("ts_model_colours") %>% 
    unlist()
  
  ymax <- 1.025 * round(1.01 * ymax, 2)
  
  model_order <- 
    metrics_df %>% 
    dplyr::arrange(mape) %>% 
    purrr::pluck("model_name")
  
  model_order <- if (sorted) model_order else names(col_map) 
  col_order <- if (sorted) col_map[model_order] else col_map

  metrics_df %>% 
    # make sure models are in the desired order
    dplyr::mutate(model_name = factor(model_name, levels = model_order)) %>% 
    ggplot2::ggplot(aes(x = model_name, y = mape, fill = model_name)) +
    ggplot2::geom_bar(stat = "identity") + 
    ggplot2::scale_fill_manual(values = col_order) +
    ggplot2::xlab("") +
    ggplot2::ylab("MAPE")  + 
    ggplot2::scale_y_continuous(breaks = seq(0, ymax, 0.01),
                                limits = c(0, ymax)) +
    ggplot2::theme_light() + 
    ggplot2::theme(axis.text.x = element_text(angle = 90,
                                              vjust = 0.5,
                                              size = 12,
                                              face = "bold"),
                   legend.position = "none",
                   plot.title = element_text(paste0("Forecast ",
                                                    mins_ahead,
                                                    " min. ahead")))

}
