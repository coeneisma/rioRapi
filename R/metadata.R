#' List available groups in the RIO dataset
#'
#' This function retrieves a list of all available groups in the RIO CKAN API.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#'
#' @return A character vector of group names.
#'
#' @examples
#' \dontrun{
#' groups <- rio_list_groups()
#' }
#'
#' @export
rio_list_groups <- function(conn = NULL) {
  response <- rio_api_call(conn, "group_list")
  return(response$result)
}

#' List available packages in the RIO dataset
#'
#' This function retrieves a list of all available packages in the RIO CKAN API.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#'
#' @return A character vector of package names.
#'
#' @examples
#' \dontrun{
#' packages <- rio_list_packages()
#' }
#'
#' @export
rio_list_packages <- function(conn = NULL) {
  response <- rio_api_call(conn, "package_list")
  return(response$result)
}

#' List available tags in the RIO dataset
#'
#' This function retrieves a list of all available tags in the RIO CKAN API.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#'
#' @return A character vector of tag names.
#'
#' @examples
#' \dontrun{
#' tags <- rio_list_tags()
#' }
#'
#' @export
rio_list_tags <- function(conn = NULL) {
  response <- rio_api_call(conn, "tag_list")
  return(response$result)
}

#' Get metadata for a specific package
#'
#' This function retrieves detailed metadata for a specific package in the RIO CKAN API.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param package_id The ID of the package to retrieve. Default is "rio_nfo_po_vo_vavo_mbo_ho".
#'
#' @return A list containing the package metadata.
#'
#' @examples
#' \dontrun{
#' rio_metadata <- rio_get_package_metadata()
#' }
#'
#' @export
rio_get_package_metadata <- function(conn = NULL, package_id = "rio_nfo_po_vo_vavo_mbo_ho") {
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
    warning("No metadata found for package: ", package_id)
    return(list())
  }
}

#' List available datasets in the RIO package
#'
#' This function retrieves a list of all available datasets (resources) in the RIO package.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param package_id The ID of the package to retrieve datasets from. Default is "rio_nfo_po_vo_vavo_mbo_ho".
#'
#' @return A tibble with information about each dataset, including ID, name, and description.
#'
#' @examples
#' \dontrun{
#' datasets <- rio_list_datasets()
#' }
#'
#' @export
rio_list_datasets <- function(conn = NULL, package_id = "rio_nfo_po_vo_vavo_mbo_ho") {
  # Get package metadata
  metadata <- rio_get_package_metadata(conn, package_id)

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
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param resource_id The ID of the resource to retrieve.
#'
#' @return A list containing detailed information about the dataset.
#'
#' @examples
#' \dontrun{
#' datasets <- rio_list_datasets()
#' resource_id <- datasets$id[1]
#' resource_info <- rio_get_resource_info(resource_id)
#' }
#'
#' @export
rio_get_resource_info <- function(conn = NULL, resource_id) {
  response <- rio_api_call(
    conn,
    "resource_show",
    method = "POST",
    body = list(id = resource_id)
  )

  if (!is.null(response$result)) {
    return(response$result)
  } else {
    warning("Resource information not found for ID: ", resource_id)
    return(list())
  }
}

#' Get the field (column) information for a dataset
#'
#' This function retrieves the field information for a specific dataset in the RIO package.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param resource_id The ID of the dataset resource.
#'
#' @return A tibble with information about the fields in the dataset.
#'
#' @examples
#' \dontrun{
#' datasets <- rio_list_datasets()
#' resource_id <- datasets$id[1]
#' fields <- rio_get_fields(resource_id)
#' }
#'
#' @export
rio_get_fields <- function(conn = NULL, resource_id) {
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
    warning("Field information not found for resource ID: ", resource_id)
    return(tibble::tibble())
  }
}
