#' List available tables in the RIO package
#'
#' This function retrieves a list of all available tables (resources) in the RIO package.
#'
#' @return A tibble with information about each table, including ID, name, and description.
#'
#' @examples
#' \dontrun{
#' tables <- rio_list_tables()
#' }
#'
#' @export
rio_list_tables <- function() {
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
  # Define package_id
  package_id <- "rio_nfo_po_vo_vavo_mbo_ho"

  response <- rio_api_call(
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


#' Get information about fields in a dataset
#'
#' This function retrieves information about the available fields (columns) in a dataset,
#' which can be used for filtering in the rio_get_data function.
#'
#' @param dataset_id The ID of the dataset resource. Default is NULL.
#' @param dataset_name The name of the dataset resource. Default is NULL.
#'        Either dataset_id or dataset_name must be provided.
#'
#' @return A tibble with information about the fields in the dataset.
#'
#' @examples
#' \dontrun{
#' # Get fields for the "Onderwijslocaties" dataset
#' fields <- rio_get_fields(dataset_name = "Onderwijslocaties")
#'
#' # Print the field names that can be used for filtering
#' print(fields$id)
#' }
#'
#' @keywords internal
rio_get_fields <- function(dataset_id = NULL, dataset_name = NULL) {
  # Check that either dataset_id or dataset_name is provided
  if (is.null(dataset_id) && is.null(dataset_name)) {
    stop("Either dataset_id or dataset_name must be provided")
  }

  # If dataset_name is provided and dataset_id is not, look up the ID
  if (is.null(dataset_id) && !is.null(dataset_name)) {
    dataset_id <- get_dataset_id_from_name(dataset_name)
    if (is.null(dataset_id)) {
      return(tibble::tibble())
    }
  }

  # Execute API call to get field information
  response <- rio_api_call(
    "datastore_search",
    method = "POST",
    body = list(dataset_id = dataset_id, limit = 0)
  )

  if (!is.null(response$result) && !is.null(response$result$fields)) {
    fields_df <- tibble::as_tibble(response$result$fields)
    return(fields_df)
  } else {
    warning("Field information not found for the specified dataset")
    return(tibble::tibble())
  }
}

#' List available tables in the RIO package
#'
#' This function retrieves a list of all available tables in the RIO package.
#'
#' @param include_description Logical indicating whether to include table descriptions.
#'        Default is TRUE.
#' @param pattern Optional pattern to filter table names. Default is NULL (no filtering).
#'
#' @return A tibble with information about each table.
#'
#' @examples
#' \dontrun{
#' # List all available tables
#' tables <- rio_list_tables()
#'
#' # List only tables with "opleiding" in the name
#' opleiding_tables <- rio_list_tables(pattern = "opleiding")
#' }
#'
#' @export
rio_list_tables <- function(include_description = TRUE, pattern = NULL) {
  # Retrieve tables from API
  tables_df <- rio_list_datasets()  # Internal function still uses 'datasets'

  # Filter by pattern if provided
  if (!is.null(pattern)) {
    tables_df <- tables_df[grepl(pattern, tables_df$name, ignore.case = TRUE), ]
  }

  # Select relevant columns based on include_description
  if (include_description) {
    cols_to_select <- intersect(c("id", "name", "description", "last_modified"), names(tables_df))
  } else {
    cols_to_select <- intersect(c("id", "name", "last_modified"), names(tables_df))
  }

  # Prepare return value
  result <- tables_df[, cols_to_select, drop = FALSE]

  # Rename columns for more clarity
  result <- dplyr::rename(result,
                          table_id = id,
                          table_name = name)

  return(result)
}

#' Get detailed information about a specific dataset
#'
#' This function retrieves detailed information about a specific dataset (resource) in the RIO package.
#'
#' @param dataset_id The ID of the resource to retrieve. Default is NULL.
#' @param dataset_name The name of the dataset resource to retrieve. Default is NULL.
#'        Either dataset_id or dataset_name must be provided.
#'
#' @return A list containing detailed information about the dataset.
#'
#' @keywords internal
rio_get_resource_info <- function(dataset_id = NULL, dataset_name = NULL) {
  # Check that either dataset_id or dataset_name is provided
  if (is.null(dataset_id) && is.null(dataset_name)) {
    stop("Either dataset_id or dataset_name must be provided")
  }

  # If dataset_name is provided and dataset_id is not, look up the ID
  if (is.null(dataset_id) && !is.null(dataset_name)) {
    dataset_id <- get_dataset_id_from_name(dataset_name)
    if (is.null(dataset_id)) {
      return(list())
    }
  }

  # Execute API call to get resource information
  response <- rio_api_call(
    "resource_show",
    method = "POST",
    body = list(id = dataset_id)
  )

  if (!is.null(response$result)) {
    return(response$result)
  } else {
    warning("Resource information not found for the specified dataset")
    return(list())
  }
}

#' List fields in a RIO table
#'
#' This function lists all fields (columns) available in a specified RIO table.
#'
#' @param table_name The name of the table to retrieve fields for.
#' @param table_id The ID of the table. If NULL, table_name will be used to look up the ID.
#' @param include_info Logical indicating whether to include field type information.
#'        Default is TRUE.
#' @param pattern Optional pattern to filter field names. Default is NULL (no filtering).
#'
#' @return A tibble with information about each field.
#'
#' @examples
#' \dontrun{
#' # List all fields in the onderwijslocaties table
#' fields <- rio_list_fields("onderwijslocaties")
#'
#' # Filter fields containing "datum"
#' date_fields <- rio_list_fields("onderwijslocaties", pattern = "datum")
#' }
#'
#' @export
rio_list_fields <- function(table_name, table_id = NULL, include_info = TRUE, pattern = NULL) {
  # If table_id is not provided, look it up by name
  if (is.null(table_id)) {
    table_id <- get_dataset_id_from_name(table_name)
    if (is.null(table_id)) {
      stop("Table not found: ", table_name)
    }
  }

  # Get fields information
  fields <- rio_get_fields(dataset_id = table_id)

  if (nrow(fields) == 0) {
    message("No fields found for table: ", table_name)
    return(tibble::tibble())
  }

  # Filter by pattern if provided
  if (!is.null(pattern)) {
    fields <- fields[grepl(pattern, fields$id, ignore.case = TRUE), ]
  }

  # Select and rename columns
  if (include_info) {
    result <- dplyr::select(fields, field_name = id, field_type = type, .data$info)
  } else {
    result <- dplyr::select(fields, field_name = id)
  }

  return(result)
}


#' Detect available datasets and their fields
#'
#' This function retrieves all available RIO datasets and their fields and stores
#' this information for later use. Unlike rio_detect_structure(), no relationships
#' between datasets are detected.
#'
#' @param force Logical indicating whether to perform a new detection,
#'        even if a recent dataset detection exists. Default is FALSE.
#' @param days_threshold Number of days after which a dataset detection is considered outdated.
#'        Default is 28 (4 weeks).
#' @param save_dir Directory where the result is saved. Default is the data directory of the package.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return Returns a list with the detected RIO datasets (invisible).
#'
#' @keywords internal
rio_detect_datasets <- function(force = FALSE, days_threshold = 28,
                                save_dir = get_rio_data_dir(), quiet = FALSE) {

  # Get the path for the dataset file
  dataset_file <- file.path(save_dir, "rio_datasets_info.rds")

  # Check if dataset info already exists and is recent
  if (!force && file.exists(dataset_file)) {
    file_info <- file.info(dataset_file)
    file_age <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))

    if (file_age < days_threshold) {
      if (!quiet) {
        cli::cli_alert_info("Using existing RIO dataset info (age: {round(file_age, 1)} days)")
      }
      return(invisible(readRDS(dataset_file)))
    } else if (!quiet) {
      cli::cli_alert_warning("Existing dataset info is {round(file_age, 1)} days old (threshold: {days_threshold}). Regenerating...")
    }
  }

  if (!quiet) {
    cli::cli_alert_info("Detecting RIO datasets. This may take several minutes...")
  }

  # Create output directory if it doesn't exist
  if (!dir.exists(save_dir)) {
    dir.create(save_dir, recursive = TRUE)
  }

  # Get list of all available datasets
  datasets <- rio_list_datasets()

  if (nrow(datasets) == 0) {
    stop("No datasets found in RIO API")
  }

  if (!quiet) {
    cli::cli_alert_info("Found {nrow(datasets)} datasets")
    cli::cli_progress_bar(
      format = "{cli::pb_spin} Analyzing dataset {cli::pb_current}/{cli::pb_total}: {dataset_name}",
      total = nrow(datasets),
      clear = FALSE
    )
  }

  # Initialize structure to store dataset information
  rio_datasets <- list(
    datasets = list(),
    metadata = list(
      created = Sys.time(),
      version = "1.0.0",
      dataset_count = nrow(datasets)
    )
  )

  # Analyze each dataset
  for (i in 1:nrow(datasets)) {
    dataset_info <- datasets[i, ]
    dataset_id <- dataset_info$id
    dataset_name <- dataset_info$name

    if (!quiet) {
      cli::cli_progress_update(set = i, status = dataset_name)
    }

    # Get fields for this dataset
    tryCatch({
      fields <- rio_get_fields(dataset_id = dataset_id)

      if (nrow(fields) > 0) {
        # Store dataset information
        rio_datasets$datasets[[dataset_name]] <- list(
          id = dataset_id,
          name = dataset_name,
          description = dataset_info$description,
          fields = fields
        )
      }
    }, error = function(e) {
      if (!quiet) {
        cli::cli_alert_danger("Error retrieving fields for dataset {dataset_name}: {e$message}")
      }
    })
  }

  if (!quiet) {
    cli::cli_progress_done()
  }

  # Save the dataset info to a file
  saveRDS(rio_datasets, dataset_file)

  if (!quiet) {
    cli::cli_alert_success("RIO dataset detection completed: found {length(rio_datasets$datasets)} datasets")
    cli::cli_alert_info("Dataset info saved to: {dataset_file}")
  }

  invisible(rio_datasets)
}

#' Load the detected RIO dataset information
#'
#' This function loads the saved RIO dataset information. If no information exists or it is
#' outdated, a new detection can optionally be performed.
#'
#' @param auto_detect Logical indicating whether to automatically perform a new dataset detection
#'        if none exists or if it is outdated. Default is TRUE.
#' @param days_threshold Number of days after which a dataset detection is considered outdated.
#'        Default is 28 (4 weeks).
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return A list with the detected RIO datasets.
#'
#' @keywords internal
rio_load_datasets <- function(auto_detect = TRUE, days_threshold = 28, quiet = FALSE) {
  dataset_file <- file.path(get_rio_data_dir(), "rio_datasets_info.rds")

  # Check if dataset info exists
  if (!file.exists(dataset_file)) {
    if (auto_detect) {
      if (!quiet) {
        cli::cli_alert_info("No existing RIO dataset info found. Creating new dataset info...")
      }
      return(rio_detect_datasets(quiet = quiet))
    } else {
      stop("No RIO dataset info found. Run rio_detect_datasets() first.")
    }
  }

  # Check if dataset info is recent
  file_info <- file.info(dataset_file)
  file_age <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))

  if (file_age >= days_threshold) {
    if (auto_detect) {
      if (!quiet) {
        cli::cli_alert_warning("Existing dataset info is {round(file_age, 1)} days old (threshold: {days_threshold}). Regenerating...")
      }
      return(rio_detect_datasets(quiet = quiet))
    } else if (!quiet) {
      cli::cli_alert_warning("Existing dataset info is {round(file_age, 1)} days old (threshold: {days_threshold})")
    }
  } else if (!quiet) {
    cli::cli_alert_info("Using existing RIO dataset info (age: {round(file_age, 1)} days)")
  }

  # Load the dataset info
  datasets <- readRDS(dataset_file)
  return(datasets)
}

#' Add metadata to a dataset
#'
#' This function adds metadata to a dataset in the RIO structure.
#'
#' @param datasets The RIO datasets structure as returned by rio_load_datasets().
#' @param dataset_name Name of the dataset.
#' @param category Main category of the dataset (e.g., "Educational Structure Reality").
#' @param subcategory Subcategory of the dataset (e.g., "Who What Where How").
#' @param dimension Dimension of the dataset (e.g., "Primary Education").
#' @param description Description of the dataset. If NULL, the existing description is kept.
#' @param extra_metadata List with any additional metadata.
#'
#' @return The updated RIO datasets structure.
#'
#' @keywords internal
rio_add_dataset_metadata <- function(datasets, dataset_name,
                                     category = NULL, subcategory = NULL,
                                     dimension = NULL, description = NULL,
                                     extra_metadata = NULL) {
  # Check if the dataset exists
  if (!dataset_name %in% names(datasets$datasets)) {
    stop("Dataset '", dataset_name, "' does not exist in the structure")
  }

  # Add metadata
  if (!is.null(category)) {
    datasets$datasets[[dataset_name]]$category <- category
  }

  if (!is.null(subcategory)) {
    datasets$datasets[[dataset_name]]$subcategory <- subcategory
  }

  if (!is.null(dimension)) {
    datasets$datasets[[dataset_name]]$dimension <- dimension
  }

  if (!is.null(description)) {
    datasets$datasets[[dataset_name]]$description <- description
  }

  # Add any extra metadata
  if (!is.null(extra_metadata) && is.list(extra_metadata)) {
    for (name in names(extra_metadata)) {
      datasets$datasets[[dataset_name]][[name]] <- extra_metadata[[name]]
    }
  }

  # Update last modified timestamp
  datasets$metadata$last_modified <- Sys.time()

  return(datasets)
}

