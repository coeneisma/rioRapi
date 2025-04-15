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
