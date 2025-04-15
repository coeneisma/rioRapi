
#' Vind datumvelden in een dataframe
#'
#' @param df Een dataframe
#'
#' @return Een character vector met namen van datumvelden
#'
#' @keywords internal
find_date_fields <- function(df) {
  date_field_patterns <- c(
    "^BEGINDATUM", "^EINDDATUM", "^STARTDATUM", "^OPHEFFINGSDATUM",
    "^INGANGSDATUM", "^INBEDRIJFDATUM", "^UITBEDRIJFDATUM",
    "_PERIODE$"
  )

  # Find columns matching the patterns
  date_fields <- character(0)
  for (pattern in date_field_patterns) {
    matching_cols <- grep(pattern, names(df), value = TRUE)
    date_fields <- c(date_fields, matching_cols)
  }

  return(unique(date_fields))
}

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
