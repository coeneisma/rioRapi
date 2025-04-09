#' Fetch data by multiple ID values
#'
#' This function retrieves data from a specific dataset based on a list of ID values.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param resource_id The ID of the dataset resource to retrieve.
#' @param id_field The name of the ID field in the dataset.
#' @param id_values Vector of ID values to filter by.
#' @param limit Maximum number of records to return per request. Default is NULL.
#'
#' @return A tibble containing the retrieved data.
#'
#' @examples
#' \dontrun{
#' # Get educational institutions by INSTELLINGSCODE
#' institutions <- rio_fetch_by_ids(
#'   resource_id = "your-resource-id",
#'   id_field = "INSTELLINGSCODE",
#'   id_values = c("12345", "67890")
#' )
#' }
#'
#' @export
rio_fetch_by_ids <- function(conn = NULL, resource_id, id_field, id_values, limit = NULL) {
  # Ensure id_values is a character vector
  id_values <- as.character(id_values)

  # Create a filters list with the ID field
  filters <- list()
  filters[[id_field]] <- id_values

  # Fetch the data
  data <- rio_fetch_data(
    conn = conn,
    resource_id = resource_id,
    filters = filters,
    limit = limit
  )

  return(data)
}

#' Fetch data from multiple resources
#'
#' This function retrieves data from multiple datasets and combines the results.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param resource_ids Vector of resource IDs to fetch data from.
#' @param ... Additional parameters passed to \code{rio_fetch_data()}.
#'
#' @return A list of tibbles, one for each resource ID.
#'
#' @examples
#' \dontrun{
#' # Get data from multiple resources
#' data_list <- rio_fetch_multiple(
#'   resource_ids = c("resource-id-1", "resource-id-2"),
#'   limit = 10
#' )
#' }
#'
#' @export
rio_fetch_multiple <- function(conn = NULL, resource_ids, ...) {
  # Fetch data for each resource ID
  data_list <- lapply(resource_ids, function(id) {
    result <- rio_fetch_data(conn, resource_id = id, ...)
    return(result)
  })

  # Name the list elements with the resource IDs
  names(data_list) <- resource_ids

  return(data_list)
}

#' Get educational locations
#'
#' This function retrieves educational locations from the RIO API.
#'
#'#' Fetch data from a RIO dataset
#'
#' This function retrieves data from a specific dataset in the RIO CKAN API.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param resource_id The ID of the dataset resource to retrieve.
#' @param limit Maximum number of records to return. Default is NULL (all records).
#' @param filters A named list of filter conditions. Default is NULL.
#' @param query A search query string or a named list of field queries. Default is NULL.
#'
#' @return A tibble containing the retrieved data.
#'
#' @examples
#' \dontrun{
#' # Get educational locations in Rotterdam
#' locations <- rio_fetch_data(
#'   resource_id = "a7e3f323-6e46-4dca-a834-369d9d520aa8",
#'   filters = list("PLAATSNAAM" = "Rotterdam")
#' )
#' }
#'
#' @export
rio_fetch_data <- function(conn = NULL, resource_id, limit = NULL, filters = NULL, query = NULL) {
  # Build request body
  body <- list(resource_id = resource_id)

  # Add optional parameters if provided
  if (!is.null(limit)) {
    body$limit <- limit
  }

  if (!is.null(filters)) {
    body$filters <- filters
  }

  if (!is.null(query)) {
    body$q <- query
  }

  # Execute API call
  response <- rio_api_call(
    conn,
    "datastore_search",
    method = "POST",
    body = body
  )

  # Check if records were returned
  if (is.null(response$result) || is.null(response$result$records) || length(response$result$records) == 0) {
    warning("No records found for the specified criteria")
    return(tibble::tibble())
  }

  # Convert to tibble
  data <- tibble::as_tibble(response$result$records)

  return(data)
}

#' Fetch data by multiple ID values
#'
#' This function retrieves data from a specific dataset based on a list of ID values.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#' @param resource_id The ID of the dataset resource to retrieve.
#' @param id_field The name of the ID field in the dataset.
#' @param id_values Vector of ID values to filter by.
#' @param limit Maximum number of records to return per request. Default is NULL.
#'
#' @return A tibble containing the retrieved data.
#'
#' @examples
#' \dontrun{
#' rio_conn <- rio_api_connection()
#' # Get educational institutions by INSTELLINGSCODE
#' institutions <- rio_fetch_by_ids(
#'   rio_conn,
#'   resource_id = "your-resource-id",
#'   id_field = "INSTELLINGSCODE",
#'   id_values = c("12345", "67890")
#' )
#' }
#'
#' @export
rio_fetch_by_ids <- function(conn, resource_id, id_field, id_values, limit = NULL) {
  # Ensure id_values is a character vector
  id_values <- as.character(id_values)

  # Create a filters list with the ID field
  filters <- list()
  filters[[id_field]] <- id_values

  # Fetch the data
  data <- rio_fetch_data(
    conn = conn,
    resource_id = resource_id,
    filters = filters,
    limit = limit
  )

  return(data)
}

#' Fetch data from multiple resources
#'
#' This function retrieves data from multiple datasets and combines the results.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#' @param resource_ids Vector of resource IDs to fetch data from.
#' @param ... Additional parameters passed to \code{rio_fetch_data()}.
#'
#' @return A list of tibbles, one for each resource ID.
#'
#' @examples
#' \dontrun{
#' rio_conn <- rio_api_connection()
#' # Get data from multiple resources
#' data_list <- rio_fetch_multiple(
#'   rio_conn,
#'   resource_ids = c("resource-id-1", "resource-id-2"),
#'   limit = 10
#' )
#' }
#'
#' @export
rio_fetch_multiple <- function(conn, resource_ids, ...) {
  # Fetch data for each resource ID
  data_list <- lapply(resource_ids, function(id) {
    result <- rio_fetch_data(conn, resource_id = id, ...)
    return(result)
  })

  # Name the list elements with the resource IDs
  names(data_list) <- resource_ids

  return(data_list)
}

#' Get educational locations
#'
#' This function retrieves educational locations from the RIO API.
#'
#' @param conn A connection object created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param city Optional city name(s) to filter locations. Default is NULL.
#' @param limit Maximum number of records to return. Default is NULL.
#'
#' @return A tibble containing educational location data.
#'
#' @examples
#' \dontrun{
#' # Get educational locations in Rotterdam
#' locations <- rio_get_locations(city = "Rotterdam")
#' }
#'
#' @export
rio_get_locations <- function(conn = NULL, city = NULL, limit = NULL) {
  # Define the resource ID for educational locations
  resource_id <- "a7e3f323-6e46-4dca-a834-369d9d520aa8"

  # Build filters if city is provided
  filters <- NULL
  if (!is.null(city)) {
    filters <- list("PLAATSNAAM" = city)
  }

  # Fetch the data
  locations <- rio_fetch_data(
    conn = conn,
    resource_id = resource_id,
    filters = filters,
    limit = limit
  )

  return(locations)
}
