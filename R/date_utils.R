#' Convert RIO date format to R Date object
#'
#' This function converts dates from the RIO API format to R Date objects.
#'
#' @param date_str A character string containing a date in "YYYY-MM-DD" or "DD-MM-YYYY" format.
#'
#' @return A Date object, or NA if the input is not a valid date.
#'
#' @examples
#' \dontrun{
#' date <- rio_parse_date("2023-01-01")
#' }
#'
#' @export
rio_parse_date <- function(date_str) {
  if (is.na(date_str) || date_str == "") {
    return(NA)
  }

  # Try different date formats
  tryCatch({
    # Try ISO format (YYYY-MM-DD)
    result <- as.Date(date_str, format = "%Y-%m-%d")
    if (!is.na(result)) return(result)

    # Try Dutch format (DD-MM-YYYY)
    result <- as.Date(date_str, format = "%d-%m-%Y")
    if (!is.na(result)) return(result)

    # If all formats fail, return NA
    return(NA)
  }, error = function(e) {
    warning("Failed to parse date: ", date_str)
    return(NA)
  })
}

#' Filter data based on reference date
#'
#' This function filters data from the RIO API based on a reference date,
#' considering start and end dates of records.
#'
#' @param data A tibble containing RIO data.
#' @param reference_date The reference date to filter by. Default is the current date.
#' @param start_date_col The name of the column containing start dates. Default is "BEGINDATUM".
#' @param end_date_col The name of the column containing end dates. Default is "EINDDATUM".
#'
#' @return A tibble containing only records that are valid at the reference date.
#'
#' @examples
#' \dontrun{
#' # Filter data for records valid on January 1, 2023
#' filtered_data <- rio_filter_by_date(
#'   data,
#'   reference_date = as.Date("2023-01-01")
#' )
#' }
#'
#' @export
rio_filter_by_date <- function(data, reference_date = Sys.Date(),
                               start_date_col = "BEGINDATUM", end_date_col = "EINDDATUM") {
  # Check if the required date columns exist in the data
  if (!start_date_col %in% names(data)) {
    warning("Start date column '", start_date_col, "' not found in data")
    return(data)
  }

  # Ensure the reference date is a Date object
  if (!inherits(reference_date, "Date")) {
    reference_date <- as.Date(reference_date)
  }

  # Convert date columns to Date objects if they're not already
  if (!inherits(data[[start_date_col]], "Date")) {
    data[[start_date_col]] <- purrr::map_vec(data[[start_date_col]], rio_parse_date)
  }

  # Filter based on start date
  valid_data <- dplyr::filter(data, is.na(.data[[start_date_col]]) | .data[[start_date_col]] <= reference_date)

  # Filter based on end date if it exists
  if (end_date_col %in% names(data)) {
    if (!inherits(data[[end_date_col]], "Date")) {
      data[[end_date_col]] <- purrr::map_vec(data[[end_date_col]], rio_parse_date)
    }

    valid_data <- dplyr::filter(valid_data,
                                is.na(.data[[end_date_col]]) | .data[[end_date_col]] >= reference_date)
  }

  return(valid_data)
}

#' Get the most recent data version
#'
#' This function retrieves the most recent version of each entity in the data,
#' based on start and end dates.
#'
#' @param data A tibble containing RIO data.
#' @param id_col The name of the column containing entity IDs.
#' @param start_date_col The name of the column containing start dates. Default is "BEGINDATUM".
#' @param end_date_col The name of the column containing end dates. Default is "EINDDATUM".
#'
#' @return A tibble containing only the most recent version of each entity.
#'
#' @examples
#' \dontrun{
#' # Get the most recent version of each institution
#' recent_data <- rio_get_most_recent(
#'   data,
#'   id_col = "INSTELLINGSCODE"
#' )
#' }
#'
#' @export
rio_get_most_recent <- function(data, id_col, start_date_col = "BEGINDATUM", end_date_col = "EINDDATUM") {
  # Check if the required columns exist in the data
  if (!id_col %in% names(data)) {
    stop("ID column '", id_col, "' not found in data")
  }

  if (!start_date_col %in% names(data)) {
    warning("Start date column '", start_date_col, "' not found in data")
    return(data)
  }

  # Convert date columns to Date objects if they're not already
  if (!inherits(data[[start_date_col]], "Date")) {
    data[[start_date_col]] <- purrr::map_vec(data[[start_date_col]], rio_parse_date)
  }

  has_end_date <- end_date_col %in% names(data)

  if (has_end_date && !inherits(data[[end_date_col]], "Date")) {
    data[[end_date_col]] <- purrr::map_vec(data[[end_date_col]], rio_parse_date)
  }

  # Sort by ID and start date (descending)
  sorted_data <- dplyr::arrange(data, .data[[id_col]], dplyr::desc(.data[[start_date_col]]))

  # Group by ID and take the first row (most recent start date)
  most_recent <- dplyr::group_by(sorted_data, .data[[id_col]]) |>
    dplyr::slice(1) |>
    dplyr::ungroup()

  return(most_recent)
}
