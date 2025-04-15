#' Create a map of educational locations
#'
#' This function creates a leaflet map of educational locations from RIO data.
#'
#' @param locations A tibble containing educational location data from the RIO API.
#' @param popup_vars Character vector of variable names to include in popups. Default is NULL.
#' @param color The color of the markers. Default is "blue".
#' @param radius The radius of the markers. Default is 5.
#'
#' @return A leaflet map object.
#'
#' @examples
#' \dontrun{
#' locations <- rio_get_locations(city = "Rotterdam")
#' map <- rio_map_locations(
#'   locations,
#'   popup_vars = c("ONDERWIJSLOCATIENAAM", "PLAATSNAAM")
#' )
#' }
#'
#' @importFrom sf st_as_sf
#' @importFrom leaflet leaflet addTiles addCircleMarkers
#' @export
rio_map_locations <- function(locations, popup_vars = NULL, color = "blue", radius = 5) {
  # Check if required packages are available
  if (!requireNamespace("sf", quietly = TRUE) || !requireNamespace("leaflet", quietly = TRUE)) {
    stop("Packages 'sf' and 'leaflet' must be installed to use this function")
  }

  # Check for required longitude and latitude columns
  if (!all(c("GPS_LONGITUDE", "GPS_LATITUDE") %in% names(locations))) {
    stop("The 'locations' data must include GPS_LONGITUDE and GPS_LATITUDE columns")
  }

  # Remove rows with missing coordinates
  valid_locations <- dplyr::filter(
    locations,
    !is.na(GPS_LONGITUDE) & !is.na(GPS_LATITUDE)
  )

  # Convert to sf object
  locations_sf <- sf::st_as_sf(
    valid_locations,
    coords = c("GPS_LONGITUDE", "GPS_LATITUDE"),
    crs = 4326
  )

  # Create popup content if variables are specified
  if (!is.null(popup_vars)) {
    # Check if all popup_vars exist in the data
    missing_vars <- popup_vars[!popup_vars %in% names(valid_locations)]
    if (length(missing_vars) > 0) {
      warning("The following popup variables were not found in the data: ",
              paste(missing_vars, collapse = ", "))
      popup_vars <- popup_vars[popup_vars %in% names(valid_locations)]
    }

    # Create popup content
    if (length(popup_vars) > 0) {
      popup_content <- apply(valid_locations[, popup_vars, drop = FALSE], 1, function(row) {
        paste0(
          "<div style='max-width: 200px;'>",
          paste0("<b>", popup_vars, ":</b> ", row, collapse = "<br>"),
          "</div>"
        )
      })
    } else {
      popup_content <- NULL
    }
  } else {
    popup_content <- NULL
  }

  # Create the map
  map <- leaflet::leaflet(locations_sf) |>
    leaflet::addTiles() |>
    leaflet::addCircleMarkers(
      radius = radius,
      color = color,
      stroke = FALSE,
      fillOpacity = 0.8,
      popup = popup_content
    )

  return(map)
}

#' Plot timeline of institutions or entities
#'
#' This function creates a timeline plot showing the validity periods of institutions or other entities.
#'
#' @param data A tibble containing data with start and end dates.
#' @param id_col The name of the column containing entity IDs.
#' @param name_col The name of the column containing entity names. Default is NULL.
#' @param start_date_col The name of the column containing start dates. Default is "BEGINDATUM".
#' @param end_date_col The name of the column containing end dates. Default is "EINDDATUM".
#'
#' @return A ggplot object showing the timeline.
#'
#' @examples
#' \dontrun{
#' institutions <- rio_get_data(dataset_id = "institution-resource-id")
#' plot <- rio_timeline(
#'   institutions,
#'   id_col = "INSTELLINGSCODE",
#'   name_col = "INSTELLINGSNAAM"
#' )
#' }
#'
#' @export
rio_visualize_timeline <- function(data, id_col, name_col = NULL,
                         start_date_col = "BEGINDATUM", end_date_col = "EINDDATUM") {
  # Check if required packages are available
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("Package 'ggplot2' must be installed to use this function")
  }

  # Ensure date columns are Date objects
  if (!inherits(data[[start_date_col]], "Date")) {
    data[[start_date_col]] <- purrr::map_vec(data[[start_date_col]], rio_parse_date)
  }

  if (end_date_col %in% names(data) && !inherits(data[[end_date_col]], "Date")) {
    data[[end_date_col]] <- purrr::map_vec(data[[end_date_col]], rio_parse_date)
  }

  # Replace NA end dates with current date
  if (end_date_col %in% names(data)) {
    data[[end_date_col]] <- dplyr::if_else(is.na(data[[end_date_col]]), Sys.Date(), data[[end_date_col]])
  } else {
    # If end date column doesn't exist, use current date
    data[[end_date_col]] <- Sys.Date()
  }

  # Create label column
  if (is.null(name_col)) {
    data$label <- data[[id_col]]
  } else {
    data$label <- paste0(data[[id_col]], ": ", data[[name_col]])
  }

  # Create the plot
  plot <- ggplot2::ggplot(data, ggplot2::aes(x = .data[[start_date_col]],
                                             xend = .data[[end_date_col]],
                                             y = .data$label,
                                             yend = .data$label)) +
    ggplot2::geom_segment(size = 4, alpha = 0.7) +
    ggplot2::labs(
      title = "Timeline",
      x = "Date",
      y = ""
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.title = ggplot2::element_text(hjust = 0.5),
      axis.text.y = ggplot2::element_text(size = 8)
    )

  return(plot)
}
