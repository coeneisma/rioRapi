#' Verkrijg de directory voor het opslaan van RIO package data
#'
#' @return Een character string met het pad naar de RIO data directory
#'
#' @keywords internal
get_rio_data_dir <- function() {
  # Use rappdirs if available, otherwise use a subdirectory in the user's home directory
  if (requireNamespace("rappdirs", quietly = TRUE)) {
    dir <- rappdirs::user_data_dir("rioRapi", "R")
  } else {
    dir <- file.path(Sys.getenv("HOME"), ".rioRapi")
  }

  # Create the directory if it doesn't exist
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }

  return(dir)
}

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
