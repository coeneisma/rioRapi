#' Create a connection to the RIO CKAN API
#'
#' This function creates a request object to connect to the Dutch Register of
#' Institutions and Programs (RIO) CKAN API.
#'
#' @param base_url The base URL for the RIO CKAN API.
#'   Default is "https://onderwijsdata.duo.nl/api/3/action".
#'
#' @return A request object configured for the RIO CKAN API.
#'
#' @examples
#' \dontrun{
#' rio_conn <- rio_api_connection()
#' }
#'
#' @keywords internal
rio_api_connection <- function(base_url = "https://onderwijsdata.duo.nl/api/3/action") {
  httr2::request(base_url) |>
    httr2::req_headers("Accept" = "application/json")
}

#' Execute an API call to the RIO CKAN API
#'
#' This function executes a prepared request to the RIO CKAN API and returns
#' the parsed JSON response.
#'
#' @param req A request object, typically created with \code{rio_api_connection()}.
#'        If NULL, a new connection will be created.
#' @param endpoint The API endpoint to call.
#' @param method The HTTP method to use. Default is "GET".
#' @param body The request body as a list for POST requests. Default is NULL.
#' @param simplify Whether to simplify the response to R vectors/matrices. Default is TRUE.
#'
#' @return The parsed JSON response from the API.
#'
#' @examples
#' \dontrun{
#' response <- rio_api_call(endpoint = "group_list")
#' }
#'
#' @keywords internal
rio_api_call <- function(endpoint, method = "GET", body = NULL, simplify = TRUE) {
  # Create a new connection
  req <- rio_api_connection()

  # Append endpoint to request path
  req <- req |>
    httr2::req_url_path_append(endpoint)

  # Set method and add body if provided
  if (!is.null(body)) {
    # Voor datastore_search endpoint, zet dataset_id om naar resource_id
    if (endpoint == "datastore_search" && !is.null(body$dataset_id)) {
      body$resource_id <- body$dataset_id
      body$dataset_id <- NULL
    }

    req <- req |>
      httr2::req_method(method) |>
      httr2::req_body_json(body)
  } else if (method != "GET") {
    req <- req |>
      httr2::req_method(method)
  }

  # Add JSON accept header
  req <- req |>
    httr2::req_headers("Accept" = "application/json")

  # Execute request and parse response
  response <- req |>
    httr2::req_perform() |>
    httr2::resp_body_json(simplifyVector = simplify)

  # Return the full response to preserve the response structure
  return(response)
}

#' Check API availability
#'
#' This function checks if the RIO CKAN API is available and responsive.
#'
#' @return TRUE if the API is available, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' is_available <- rio_api_check()
#' }
#'
#' @keywords internal
rio_api_check <- function() {
  # Create a connection
  req <- rio_api_connection()

  tryCatch({
    test_response <- req |>
      httr2::req_url_path_append("site_read") |>
      httr2::req_perform()

    status_code <- httr2::resp_status(test_response)
    return(status_code >= 200 && status_code < 300)
  }, error = function(e) {
    message("Error connecting to RIO API: ", e$message)
    return(FALSE)
  })
}


#' Helper function to get resource ID from name
#'
#' @param conn Connection object
#' @param dataset_name Resource name
#' @param package_id Package ID
#'
#' @return Resource ID or NULL if not found
#'
#' @keywords internal
get_dataset_id_from_name <- function(dataset_name) {
  # Get list of datasets
  datasets <- rio_list_tables()

  # Find dataset with matching name
  match_idx <- which(datasets$name == dataset_name)

  # Return ID if found, otherwise NULL
  if (length(match_idx) > 0) {
    return(datasets$id[match_idx[1]])
  } else {
    warning("Dataset with name '", dataset_name, "' not found")
    return(NULL)
  }
}
