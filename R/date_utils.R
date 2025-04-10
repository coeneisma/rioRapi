#' Convert RIO date format to R Date object
#'
#' This function converts dates from the RIO API format to R Date objects.
#'
#' @param date_str A character string containing a date in "YYYY-MM-DD" or "DD-MM-YYYY" format.
#'
#' @return A Date object, or NA if the input is not a valid date.
#'
#' @keywords internal
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
#' @keywords internal
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

