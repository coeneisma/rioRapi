#' Combine RIO datasets
#'
#' This function combines multiple RIO datasets based on a common ID field.
#'
#' @param ... Named tibbles to combine. Each argument should be a tibble with data from RIO.
#' @param by Character vector of columns to join by. If NULL, will join by all columns with the same name.
#' @param suffix Suffixes to add to make non-by column names unique across the joined tables.
#'
#' @return A combined tibble with data from all input tibbles.
#'
#' @examples
#' \dontrun{
#' locations <- rio_get_locations(city = "Rotterdam")
#' institutions <- rio_get_data(dataset_id = "institution-resource-id")
#'
#' # Combine datasets
#' combined_data <- rio_combine(
#'   locations = locations,
#'   institutions = institutions,
#'   by = "INSTELLINGSCODE"
#' )
#' }
#'
#' @export
rio_combine <- function(..., by = NULL, suffix = c("_x", "_y")) {
  # Get the list of tibbles
  tibbles <- list(...)

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for combining")
  }

  # Start with the first tibble
  result <- tibbles[[1]]

  # Combine with each of the remaining tibbles
  for (i in 2:length(tibbles)) {
    result <- dplyr::left_join(result, tibbles[[i]], by = by, suffix = suffix)
  }

  return(result)
}

#' Join RIO datasets with vector lookup
#'
#' This function joins data from a RIO dataset based on a vector of IDs.
#'
#' @param ids A vector of IDs to look up.
#' @param dataset_id The ID of the dataset resource to retrieve.
#' @param id_field The name of the ID field in the dataset.
#' @param reference_date Optional date to filter valid records. Default is the current date.
#'
#' @return A tibble with data for the specified IDs.
#'
#' @keywords internal
rio_lookup <- function(ids, dataset_id, id_field, reference_date = NULL) {
  # Create connection
  conn <- rio_api_connection()

  # Fetch data by IDs
  filters <- list()
  filters[[id_field]] <- ids

  data <- rio_get_data(
    dataset_id = dataset_id,
    quiet = TRUE,
    filters = filters
  )

  # Apply date filtering if a reference date is provided
  if (!is.null(reference_date)) {
    data <- rio_filter_by_date(data, reference_date = reference_date)
  }

  return(data)
}

#' Join RIO datasets with dataframe lookup
#'
#' This function joins data from a RIO dataset based on IDs in a dataframe.
#'
#' @param df A dataframe containing IDs to look up.
#' @param id_col The name of the column in the dataframe containing IDs.
#' @param dataset_id The ID of the dataset resource to retrieve.
#' @param rio_id_field The name of the ID field in the RIO dataset. If NULL, uses id_col.
#' @param reference_date Optional date to filter valid records. Default is the current date.
#'
#' @return A tibble with the input dataframe joined with data from the RIO dataset.
#'
#' @examples
#' \dontrun{
#' # Join a dataframe with institution data
#' df <- data.frame(inst_id = c("12345", "67890"), value = c(10, 20))
#' result <- rio_join(
#'   df = df,
#'   id_col = "inst_id",
#'   dataset_id = "institution-resource-id",
#'   rio_id_field = "INSTELLINGSCODE"
#' )
#' }
#'
#' @export
rio_join <- function(df, id_col, dataset_id, rio_id_field = NULL, reference_date = NULL) {
  # If rio_id_field is not specified, use id_col
  if (is.null(rio_id_field)) {
    rio_id_field <- id_col
  }

  # Extract unique IDs from the dataframe
  ids <- unique(df[[id_col]])

  # Look up the data using rio_lookup (internal function)
  rio_data <- rio_lookup(
    ids = ids,
    dataset_id = dataset_id,
    id_field = rio_id_field,
    reference_date = reference_date
  )

  # Join with the input dataframe
  if (id_col != rio_id_field) {
    # Rename rio_id_field to id_col for joining
    rio_data <- rio_data |>
      dplyr::rename(!!id_col := !!rio_id_field)
  }

  result <- dplyr::left_join(df, rio_data, by = id_col)

  return(result)
}
