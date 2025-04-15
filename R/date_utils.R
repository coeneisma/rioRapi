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
