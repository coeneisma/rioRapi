#' Get metadata for the RIO dataset
#'
#' This function retrieves detailed metadata for the Dutch Register of Institutions
#' and Programs (RIO) dataset.
#'
#' @return A list containing the package metadata.
#'
#' @examples
#' \dontrun{
#' rio_metadata <- rio_get_metadata()
#' }
#'
#' @keywords internal
rio_get_metadata <- function() {
  # Create connection
  conn <- rio_api_connection()

  # Define package_id
  package_id <- "rio_nfo_po_vo_vavo_mbo_ho"

  response <- rio_api_call(
    conn,
    "package_show",
    method = "POST",
    body = list(id = package_id)
  )

  # Return the full result, which includes all package metadata
  if (!is.null(response$result)) {
    return(response$result)
  } else {
    warning("No metadata found for the RIO dataset")
    return(list())
  }
}

#' List available datasets in the RIO package
#'
#' This function retrieves a list of all available datasets (resources) in the RIO package.
#'
#' @return A tibble with information about each dataset, including ID, name, and description.
#'
#' @examples
#' \dontrun{
#' datasets <- rio_list_datasets()
#' }
#'
#' @export
rio_list_datasets <- function() {
  # Create connection
  conn <- rio_api_connection()

  # Define package_id
  package_id <- "rio_nfo_po_vo_vavo_mbo_ho"

  # Get package metadata
  metadata <- rio_get_metadata()

  # Check if resources exist
  if (!is.null(metadata$resources) && length(metadata$resources) > 0) {
    # Resources data frame
    resources_df <- tibble::as_tibble(metadata$resources)

    # Select relevant columns, if they exist
    cols_to_select <- intersect(
      c("id", "name", "description", "format", "last_modified", "url", "created"),
      names(resources_df)
    )

    return(resources_df[, cols_to_select, drop = FALSE])
  } else {
    warning("No resources found in the package: ", package_id)
    return(tibble::tibble())
  }
}

#' Get detailed information about a specific dataset
#'
#' This function retrieves detailed information about a specific dataset (resource) in the RIO package.
#'
#' @param resource_id The ID of the resource to retrieve. Default is NULL.
#' @param resource_name The name of the dataset resource to retrieve. Default is NULL.
#'        Either resource_id or resource_name must be provided.
#'
#' @return A list containing detailed information about the dataset.
#'
#' @keywords internal
rio_get_resource_info <- function(resource_id = NULL, resource_name = NULL) {
  # Create connection
  conn <- rio_api_connection()

  # Define package_id
  package_id <- "rio_nfo_po_vo_vavo_mbo_ho"

  # Check that either resource_id or resource_name is provided
  if (is.null(resource_id) && is.null(resource_name)) {
    stop("Either resource_id or resource_name must be provided")
  }

  # If resource_name is provided and resource_id is not, look up the ID
  if (is.null(resource_id) && !is.null(resource_name)) {
    resource_id <- get_resource_id_from_name(conn, resource_name, package_id)
    if (is.null(resource_id)) {
      return(list())
    }
  }

  # Execute API call to get resource information
  response <- rio_api_call(
    conn,
    "resource_show",
    method = "POST",
    body = list(id = resource_id)
  )

  if (!is.null(response$result)) {
    return(response$result)
  } else {
    warning("Resource information not found for the specified dataset")
    return(list())
  }
}

#' Get information about fields in a dataset
#'
#' This function retrieves information about the available fields (columns) in a dataset,
#' which can be used for filtering in the rio_get_data function.
#'
#' @param resource_id The ID of the dataset resource. Default is NULL.
#' @param resource_name The name of the dataset resource. Default is NULL.
#'        Either resource_id or resource_name must be provided.
#'
#' @return A tibble with information about the fields in the dataset.
#'
#' @examples
#' \dontrun{
#' # Get fields for the "Onderwijslocaties" dataset
#' fields <- rio_get_fields(resource_name = "Onderwijslocaties")
#'
#' # Print the field names that can be used for filtering
#' print(fields$id)
#' }
#'
#' @export
rio_get_fields <- function(resource_id = NULL, resource_name = NULL) {
  # Create connection
  conn <- rio_api_connection()

  # Define package_id
  package_id <- "rio_nfo_po_vo_vavo_mbo_ho"

  # Check that either resource_id or resource_name is provided
  if (is.null(resource_id) && is.null(resource_name)) {
    stop("Either resource_id or resource_name must be provided")
  }

  # If resource_name is provided and resource_id is not, look up the ID
  if (is.null(resource_id) && !is.null(resource_name)) {
    resource_id <- get_resource_id_from_name(conn, resource_name, package_id)
    if (is.null(resource_id)) {
      return(tibble::tibble())
    }
  }

  # Execute API call to get field information
  response <- rio_api_call(
    conn,
    "datastore_search",
    method = "POST",
    body = list(resource_id = resource_id, limit = 0)
  )

  if (!is.null(response$result) && !is.null(response$result$fields)) {
    fields_df <- tibble::as_tibble(response$result$fields)
    return(fields_df)
  } else {
    warning("Field information not found for the specified dataset")
    return(tibble::tibble())
  }
}
