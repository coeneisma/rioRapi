#' Fetch data from a RIO table with date filtering
#'
#' This function retrieves data from a RIO table, optionally filtering by a reference date
#' to get only records valid at that date.
#'
#' @param table_id The ID of the table resource to retrieve. Default is NULL.
#' @param table_name The name of the table resource to retrieve. Default is NULL.
#'        Either table_id or table_name must be provided.
#' @param reference_date Optional date to filter valid records. If provided, only records
#'        valid on this date will be returned. Default is NULL (no date filtering).
#' @param limit Maximum number of records to return. Default is NULL (all records).
#' @param query A search query string for full-text search. Default is NULL.
#' @param all_records Logical indicating whether to fetch all records. Default is TRUE.
#' @param batch_size Number of records to retrieve per API call. Default is 1000.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#' @param ... Additional named parameters used as filters.
#'
#' @return A tibble containing the retrieved data.
#'
#' @examples
#' \dontrun{
#' # Get data from a table by name
#' locaties <- rio_get_data(table_name = "onderwijslocaties")
#'
#' # Get data from a table by ID with a reference date
#' vestigingen <- rio_get_data(table_id = "vestigingserkenningen-id",
#'                            reference_date = as.Date("2023-01-01"))
#'
#' # Get data with a filter
#' rotterdam <- rio_get_data(table_name = "onderwijslocaties",
#'                          PLAATSNAAM = "Rotterdam")
#' }
#'
#' @export
rio_get_data <- function(table_id = NULL, table_name = NULL,
                         reference_date = NULL,
                         limit = NULL, query = NULL,
                         all_records = TRUE, batch_size = 1000,
                         quiet = FALSE,
                         ...) {
  # Check that either table_id or table_name is provided
  if (is.null(table_id) && is.null(table_name)) {
    stop("Either table_id or table_name must be provided")
  }

  # If table_name is provided and table_id is not, look up the ID
  if (is.null(table_id) && !is.null(table_name)) {
    table_name_for_messages <- table_name  # Store for later use in messages
    table_id <- get_dataset_id_from_name(table_name)
    if (is.null(table_id)) {
      return(tibble::tibble())
    }
  } else {
    table_name_for_messages <- table_id  # Use ID for messages if name isn't provided
  }

  # Process additional filter parameters
  dots <- list(...)
  filters <- NULL
  if (length(dots) > 0) {
    filters <- list()
    for (param_name in names(dots)) {
      filters[[param_name]] <- dots[[param_name]]
    }
  }

  # If limit is set or all_records is FALSE, we'll just make a single API call
  if (!is.null(limit) || !all_records) {
    # Build request body
    body <- list(dataset_id = table_id)  # Internally still uses dataset_id

    if (!is.null(limit)) {
      body$limit <- limit
    } else {
      body$limit <- batch_size
    }

    if (!is.null(query)) {
      body$q <- query
    }

    if (!is.null(filters)) {
      body$filters <- filters
    }

    if (!quiet) {
      cli::cli_alert_info("Fetching data from table: {.val {table_name_for_messages}}")
    }

    # Execute API call
    response <- rio_api_call(
      "datastore_search",
      method = "POST",
      body = body
    )

    # Check if records were returned
    if (is.null(response$result) || is.null(response$result$records) || length(response$result$records) == 0) {
      if (!quiet) {
        cli::cli_alert_warning("No records found for the specified criteria")
      }
      return(tibble::tibble())
    }

    # Convert to tibble
    data <- tibble::as_tibble(response$result$records)

    if (!quiet) {
      cli::cli_alert_success("Retrieved {nrow(data)} records")
    }

    return(data)
  } else {
    # We're fetching all records, so we need to make multiple API calls

    # First, get the total number of records
    resource_info <- rio_get_resource_info(dataset_id = table_id)  # Internal function still uses dataset_id

    # Check if we have the preview_rows information
    total_records <- NULL
    if (!is.null(resource_info$preview_rows)) {
      total_records <- as.numeric(resource_info$preview_rows)
    }

    if (is.null(total_records) || is.na(total_records)) {
      # If we don't have total_records from metadata, make an initial query to get total
      if (!quiet) {
        cli::cli_alert_info("Determining total number of records in table: {.val {table_name_for_messages}}")
      }

      initial_query <- rio_api_call(
        "datastore_search",
        method = "POST",
        body = list(
          dataset_id = table_id,  # Internally still uses dataset_id
          limit = 0,
          q = query,
          filters = filters
        )
      )

      if (!is.null(initial_query$result) && !is.null(initial_query$result$total)) {
        total_records <- initial_query$result$total
      } else {
        if (!quiet) {
          cli::cli_alert_warning("Unable to determine total number of records. Will retrieve first batch only.")
        }
        total_records <- batch_size
      }
    }

    if (!quiet) {
      cli::cli_alert_info("Fetching {total_records} records from table: {.val {table_name_for_messages}}")

      # Initialize progress bar with improved format
      cli::cli_progress_bar(
        format = "{cli::pb_spin} Fetching records [{cli::pb_current}/{cli::pb_total}] {cli::pb_percent} | Elapsed: {cli::pb_elapsed} | ETA: {cli::pb_eta}",
        total = total_records,
        clear = FALSE
      )
    }

    # Initialize an empty list to store all batches of data
    all_data <- list()
    records_fetched <- 0
    start_time <- Sys.time()

    # Loop until we've fetched all records
    while (records_fetched < total_records) {
      # Create query for current batch
      body <- list(
        dataset_id = table_id,  # Internally still uses dataset_id
        limit = batch_size,
        offset = records_fetched
      )

      if (!is.null(query)) {
        body$q <- query
      }

      if (!is.null(filters)) {
        body$filters <- filters
      }

      # Execute API call
      batch_response <- rio_api_call(
        "datastore_search",
        method = "POST",
        body = body
      )

      # Check if records were returned
      if (is.null(batch_response$result) || is.null(batch_response$result$records) ||
          length(batch_response$result$records) == 0) {
        # No more records, break out of the loop
        break
      }

      # Convert to tibble and add to our list
      batch_data <- tibble::as_tibble(batch_response$result$records)
      all_data[[length(all_data) + 1]] <- batch_data

      # Update counter
      new_records <- nrow(batch_data)
      records_fetched <- records_fetched + new_records

      # Update progress bar with current speed
      if (!quiet) {
        cli::cli_progress_update(
          set = records_fetched,
          status = sprintf("Speed: %.1f records/sec",
                           records_fetched / as.numeric(difftime(Sys.time(), start_time, units = "secs")))
        )
      }

      # If we got fewer records than requested, we've reached the end
      if (new_records < batch_size) {
        break
      }
    }

    # Finish progress bar
    if (!quiet) {
      cli::cli_progress_done()
    }

    # Combine all batches into a single tibble
    if (length(all_data) == 0) {
      if (!quiet) {
        cli::cli_alert_warning("No records found for the specified criteria")
      }
      return(tibble::tibble())
    } else if (length(all_data) == 1) {
      result <- all_data[[1]]
    } else {
      # Use dplyr::bind_rows to efficiently combine all tibbles
      result <- dplyr::bind_rows(all_data)
    }

    elapsed_time <- difftime(Sys.time(), start_time, units = "secs")

    if (!quiet) {
      speed <- nrow(result) / as.numeric(elapsed_time)
      cli::cli_alert_success("Retrieved {nrow(result)} records in {round(elapsed_time, 1)} seconds ({round(speed, 1)} records/sec)")
    }

    # Apply reference date filtering if specified
    if (!is.null(reference_date)) {
      if (!quiet) {
        cli::cli_alert_info("Filtering data for reference date: {reference_date}")
      }

      # Apply client-side filtering
      result <- rio_filter_by_reference_date(result, reference_date)
    }

    return(result)
  }
}

#' Get educational locations
#'
#' This function retrieves educational locations from the RIO API.
#' It's a convenient wrapper around rio_get_data for the "Onderwijslocaties" dataset.
#' By default, it returns a simple tibble, but it can also convert the data to an sf object
#' for spatial analysis and mapping.
#'
#' @param city Optional city name(s) to filter locations. Default is NULL.
#' @param limit Maximum number of records to return. Default is NULL.
#' @param as_sf Logical indicating whether to return the result as an sf object. Default is FALSE.
#' @param remove_invalid Logical indicating whether to remove rows with invalid or missing coordinates. Default is FALSE
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#' @param ... Additional filter parameters to pass to rio_get_data.
#'
#' @return A tibble containing educational location data, or an sf object if as_sf = TRUE.
#'
#' @examples
#' \dontrun{
#' # Get educational locations in Rotterdam as a tibble
#' locations <- rio_get_locations(city = "Rotterdam")
#'
#' # Get educational locations in Rotterdam as an sf object
#' locations_sf <- rio_get_locations(city = "Rotterdam", as_sf = TRUE)
#'
#' # Plot locations on a map
#' if (requireNamespace("leaflet", quietly = TRUE)) {
#'   leaflet::leaflet(locations_sf) |>
#'     leaflet::addTiles() |>
#'     leaflet::addCircleMarkers()
#' }
#' }
#'
#' @importFrom sf st_as_sf
#' @export
rio_get_locations <- function(city = NULL, limit = NULL,
                              as_sf = FALSE, remove_invalid = FALSE,
                              quiet = FALSE, ...) {
  # Build parameters for rio_get_data
  params <- list(
    dataset_name = "onderwijslocaties",
    limit = limit,
    quiet = quiet,
    ...
  )

  # Add city filter if provided
  if (!is.null(city)) {
    params$PLAATSNAAM <- city
  }

  # Call rio_get_data with the parameters
  locations <- do.call(rio_get_data, params)

  # If no data or not converting to sf, return as is
  if (nrow(locations) == 0 || !as_sf) {
    return(locations)
  }

  # Check if required columns exist
  if (!all(c("GPS_LONGITUDE", "GPS_LATITUDE") %in% names(locations))) {
    if (!quiet) {
      cli::cli_alert_warning("Cannot convert to sf: GPS_LONGITUDE and/or GPS_LATITUDE columns not found")
    }
    return(locations)
  }

  # Check if sf package is available
  if (!requireNamespace("sf", quietly = TRUE)) {
    if (!quiet) {
      cli::cli_alert_warning("Cannot convert to sf: sf package is not installed")
    }
    return(locations)
  }

  # If remove_invalid is TRUE, remove rows with missing or invalid coordinates
  if (remove_invalid) {
    valid_rows <- !is.na(locations$GPS_LONGITUDE) &
      !is.na(locations$GPS_LATITUDE) &
      is.numeric(locations$GPS_LONGITUDE) &
      is.numeric(locations$GPS_LATITUDE)

    if (sum(!valid_rows) > 0 && !quiet) {
      cli::cli_alert_info("Removing {sum(!valid_rows)} rows with invalid or missing coordinates")
    }

    locations <- locations[valid_rows, ]

    # If all rows were invalid, return empty sf object
    if (nrow(locations) == 0) {
      if (!quiet) {
        cli::cli_alert_warning("No valid coordinates found")
      }
      return(sf::st_sf(geometry = sf::st_sfc(), crs = 4326))
    }
  }

  # Convert to sf object
  tryCatch({
    locations_sf <- sf::st_as_sf(
      locations,
      coords = c("GPS_LONGITUDE", "GPS_LATITUDE"),
      crs = 4326
    )

    if (!quiet) {
      cli::cli_alert_success("Converted to sf object with {nrow(locations_sf)} points")
    }

    return(locations_sf)
  }, error = function(e) {
    if (!quiet) {
      cli::cli_alert_danger("Error converting to sf: {e$message}")
    }
    return(locations)
  })
}

#' Filter data based on a reference date
#'
#' This function filters temporal data based on a reference date,
#' keeping only records that are valid at the specified date.
#' It prioritizes EINDDATUM/BEGINDATUM over EINDDATUM_PERIODE/BEGINDATUM_PERIODE.
#'
#' @param data A tibble containing temporal data with date columns.
#' @param reference_date The date for which records should be valid (as Date object).
#' @param start_date_col The name of the column containing start dates.
#'        Default is NULL, which means the function will automatically search for
#'        appropriate columns.
#' @param end_date_col The name of the column containing end dates.
#'        Default is NULL, which means the function will automatically search for
#'        appropriate columns.
#'
#' @return A filtered tibble containing only records that are valid at the reference date,
#'         or the original tibble if no suitable date columns are found.
#'
#' @keywords internal
rio_filter_by_reference_date <- function(data, reference_date,
                                         start_date_col = NULL,
                                         end_date_col = NULL) {
  # Ensure reference_date is a Date object
  if (!inherits(reference_date, "Date")) {
    reference_date <- as.Date(reference_date)
  }

  # Check available columns in the data
  column_names <- names(data)

  # Eerst de filtering op standaard datum velden proberen (hoogste prioriteit)
  standard_start_cols <- c("BEGINDATUM", "STARTDATUM", "INGANGSDATUM", "INBEDRIJFDATUM")
  standard_end_cols <- c("EINDDATUM", "OPHEFFINGSDATUM", "UITBEDRIJFDATUM")

  # Dan de periode datum velden (tweede prioriteit)
  period_start_cols <- grep("_PERIODE$", column_names, value = TRUE)
  period_start_cols <- period_start_cols[grepl("^BEGIN|^START", period_start_cols)]

  period_end_cols <- grep("_PERIODE$", column_names, value = TRUE)
  period_end_cols <- period_end_cols[grepl("^EIND", period_end_cols)]

  # Combineer alle mogelijke datumkolommen in volgorde van prioriteit
  all_start_cols <- c(standard_start_cols, period_start_cols)
  all_end_cols <- c(standard_end_cols, period_end_cols)

  # Vind de eerste beschikbare start datum kolom
  available_start_cols <- all_start_cols[all_start_cols %in% column_names]
  available_end_cols <- all_end_cols[all_end_cols %in% column_names]

  # Gebruik de opgegeven kolommen indien beschikbaar
  if (!is.null(start_date_col)) {
    if (start_date_col %in% column_names) {
      available_start_cols <- c(start_date_col, setdiff(available_start_cols, start_date_col))
    } else {
      message("Specified start date column '", start_date_col, "' not found in data.")
    }
  }

  if (!is.null(end_date_col)) {
    if (end_date_col %in% column_names) {
      available_end_cols <- c(end_date_col, setdiff(available_end_cols, end_date_col))
    } else {
      message("Specified end date column '", end_date_col, "' not found in data.")
    }
  }

  # Als er geen datumkolommen zijn, retourneer ongewijzigde data
  if (length(available_start_cols) == 0 && length(available_end_cols) == 0) {
    message("No suitable date columns found in the data. Returning unfiltered data.")
    return(data)
  }

  # Maak een kopie van de originele data
  filtered_data <- data

  # Verwerk start datum kolommen in volgorde van prioriteit
  if (length(available_start_cols) > 0) {
    for (start_column in available_start_cols) {
      # Converteer naar Date object als het dat nog niet is
      if (!inherits(filtered_data[[start_column]], "Date")) {
        filtered_data[[start_column]] <- purrr::map_vec(filtered_data[[start_column]], rio_parse_date)
      }

      # Filter op start datum
      filtered_data <- dplyr::filter(filtered_data,
                                     is.na(.data[[start_column]]) | .data[[start_column]] <= reference_date)

      message("Filtering using start date column: '", start_column, "'")
    }
  }

  # Verwerk eind datum kolommen in volgorde van prioriteit
  if (length(available_end_cols) > 0) {
    for (end_column in available_end_cols) {
      # Converteer naar Date object als het dat nog niet is
      if (!inherits(filtered_data[[end_column]], "Date")) {
        filtered_data[[end_column]] <- purrr::map_vec(filtered_data[[end_column]], rio_parse_date)
      }

      # Filter op eind datum
      filtered_data <- dplyr::filter(filtered_data,
                                     is.na(.data[[end_column]]) | .data[[end_column]] > reference_date)

      message("Filtering using end date column: '", end_column, "'")
    }
  }

  return(filtered_data)
}
