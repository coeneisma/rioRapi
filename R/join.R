#' Join RIO tables based on predefined relations
#'
#' This function joins multiple RIO tables based on the relations defined in the package.
#' It automatically detects relations between tables and chooses the optimal join path.
#' When GPS coordinates are available, the result can be converted to an sf object.
#'
#' @param ... Character vector of table names to load or named tibbles to join.
#' @param by_names Logical indicating whether the arguments are table names
#'        rather than actual datasets. Default is TRUE.
#' @param reference_date Optional date to filter valid records. Default is the current date (Sys.Date()).
#'        Set to NULL to disable date filtering.
#' @param nested Logical indicating whether to return a nested tibble. Default is FALSE.
#' @param find_paths Logical indicating whether to find indirect connection paths between datasets
#'        that are not directly connected. Default is TRUE.
#' @param max_path_length Maximum length of connection paths to consider when find_paths is TRUE.
#'        Default is 10.
#' @param as_sf Logical indicating whether to convert the result to an sf object when GPS coordinates
#'        are available. Default is FALSE.
#' @param remove_invalid Logical indicating whether to remove rows with invalid or missing coordinates
#'        when converting to sf. Default is FALSE.
#' @param filters Optional named list of filters to apply when loading data. Filters will be applied
#'        to all tables where the filter column exists.
#' @param query Optional search query string for full-text search. Default is NULL.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return A joined tibble, or an sf object if as_sf = TRUE and GPS coordinates are available.
#'
#' @examples
#' \dontrun{
#' # Join tables by name (default method)
#' joined_data <- rio_join("vestigingserkenningen", "onderwijslocaties")
#'
#' # Return as sf object for mapping
#' geo_data <- rio_join("vestigingserkenningen", "onderwijslocaties", as_sf = TRUE)
#'
#' # Join with a specific reference date
#' joined_data <- rio_join("vestigingserkenningen", "onderwijslocaties",
#'                         reference_date = as.Date("2023-01-01"))
#'
#' # Join with filters on the data
#' joined_data <- rio_join("vestigingserkenningen", "onderwijslocaties",
#'                         filters = list(PLAATSNAAM = "Utrecht"))
#'
#' # Return a nested tibble
#' nested_data <- rio_join("vestigingserkenningen", "onderwijslocaties", nested = TRUE)
#' }
#'
#' @export
rio_join <- function(..., by_names = TRUE, reference_date = Sys.Date(),
                     nested = FALSE, find_paths = TRUE, max_path_length = 10,
                     as_sf = FALSE, remove_invalid = FALSE,
                     filters = NULL, query = NULL, quiet = FALSE) {
  # Load relations from YAML file
  relations <- rio_load_relations()

  # Process inputs to handle both named tibbles and table names
  input_args <- list(...)

  if (by_names) {
    # Input is table names, load the data
    table_names <- unlist(input_args)

    # Use the common helper function to find paths between tables
    path_result <- rio_find_table_paths(
      tables = table_names,
      relations = relations$relations,
      min_confidence = "medium",
      find_paths = find_paths,
      max_path_length = max_path_length,
      quiet = quiet
    )

    # Update with all tables that need to be included
    table_names <- path_result$tables

    if (!quiet) {
      cli::cli_alert_info("Loading {length(table_names)} tables")
    }

    # Load each table
    tibbles <- list()
    loading_errors <- character(0)

    for (name in table_names) {
      if (!quiet) {
        cli::cli_alert_info("Loading table: {name}")
      }

      # Bereid parameters voor rio_get_data voor
      get_data_params <- list(
        table_name = name,
        quiet = quiet
      )

      # Voeg reference_date toe indien beschikbaar
      if (!is.null(reference_date)) {
        get_data_params$reference_date <- reference_date
      }

      # Voeg query toe indien beschikbaar
      if (!is.null(query)) {
        get_data_params$query <- query
      }

      # Voeg filters toe indien beschikbaar
      if (!is.null(filters) && length(filters) > 0) {
        # Haal velden op voor deze tabel
        fields_info <- tryCatch({
          rio_list_fields(name)
        }, error = function(e) {
          if (!quiet) {
            message("Could not get fields for table '", name, "': ", e$message)
          }
          NULL
        })

        # Als we veldinfo hebben, pas alleen de relevante filters toe
        if (!is.null(fields_info) && nrow(fields_info) > 0) {
          available_fields <- fields_info$field_name

          # Filter op velden die bestaan in deze tabel
          applicable_filters <- filters[names(filters) %in% available_fields]

          if (length(applicable_filters) > 0) {
            # Voeg toe aan get_data_params
            get_data_params <- c(get_data_params, applicable_filters)

            if (!quiet) {
              applied_filter_names <- paste(names(applicable_filters), collapse = ", ")
              cli::cli_alert_info("Applying filters for columns: {applied_filter_names}")
            }
          }
        }
      }

      # Haal de data op met alle parameters
      tibble_result <- tryCatch({
        do.call(rio_get_data, get_data_params)
      }, error = function(e) {
        loading_errors <- c(loading_errors, paste("Error loading table '", name, "': ", e$message, sep = ""))
        if (!quiet) {
          cli::cli_alert_danger("Error loading table '{name}': {e$message}")
        }
        # Return empty tibble
        tibble::tibble()
      })

      if (nrow(tibble_result) == 0) {
        if (!quiet) {
          cli::cli_alert_warning("No data found for table '{name}'")
        }
      }

      tibbles[[name]] <- tibble_result
    }

    # Check if we have enough tables with data
    tables_with_data <- names(tibbles)[sapply(tibbles, nrow) > 0]
    if (length(tables_with_data) < 2) {
      if (length(loading_errors) > 0) {
        stop(paste("Failed to load enough tables for joining:",
                   paste(loading_errors, collapse = "\n"), sep = "\n"))
      } else {
        stop("Not enough tables with data for joining (minimum 2 required)")
      }
    }
  } else {
    # Input is already loaded tibbles
    tibbles <- input_args
    table_names <- names(tibbles)
  }

  # Validate inputs
  if (length(tibbles) < 2) {
    stop("At least two tables must be provided for joining")
  }

  if (!by_names && "" %in% table_names) {
    stop("When providing tibbles directly, all must be named. Example: rio_join(table1 = df1, table2 = df2)")
  }

  # Find the optimal path to connect the tables, forcing start from the first table
  path_info <- find_optimal_path(table_names, relations$relations,
                                 verbose = !quiet, start_with_first = TRUE)

  # Execute the joins based on the optimal path
  result <- execute_dataset_joins(tibbles, path_info, relations$relations,
                                  verbose = !quiet, reference_date = reference_date)

  # Format as nested tibble if requested
  if (nested) {
    tryCatch({
      result <- nest_result(result, table_names, path_info)
    }, error = function(e) {
      warning("Failed to create nested result: ", e$message,
              "\nReturning flat joined table instead.")
    })
  }

  # Convert to sf object if requested and GPS coordinates zijn aanwezig
  if (as_sf && all(c("GPS_LONGITUDE", "GPS_LATITUDE") %in% names(result))) {
    if (!requireNamespace("sf", quietly = TRUE)) {
      if (!quiet) {
        warning("Cannot convert to sf: sf package is not installed")
      }
    } else {
      # Identificeer rijen met ontbrekende of ongeldige coördinaten
      invalid_coords <- is.na(result$GPS_LONGITUDE) |
        is.na(result$GPS_LATITUDE) |
        !is.numeric(result$GPS_LONGITUDE) |
        !is.numeric(result$GPS_LATITUDE)

      # Toon aantal rijen met ongeldige coördinaten
      invalid_count <- sum(invalid_coords)
      if (invalid_count > 0) {
        message(sprintf("Found %d rows with invalid or missing coordinates.", invalid_count))
      }

      # Filter ongeldige coördinaten altijd uit voor sf conversie (anders werkt het niet)
      valid_result <- result[!invalid_coords, ]

      if (nrow(valid_result) == 0) {
        warning("No valid coordinates found, returning non-sf result")
        return(result)  # Return original data if no valid coordinates
      }

      # Toon hoeveel rijen overblijven
      if (!quiet && invalid_count > 0) {
        message(sprintf("Converting %d/%d rows with valid coordinates to sf object.",
                        nrow(valid_result), nrow(result)))
      }

      # Convert to sf object
      tryCatch({
        result_sf <- sf::st_as_sf(
          valid_result,
          coords = c("GPS_LONGITUDE", "GPS_LATITUDE"),
          crs = 4326
        )

        if (!quiet) {
          cli::cli_alert_success("Successfully created sf object with {nrow(result_sf)} points")
        }

        # Vervang het originele resultaat door het sf-object
        result <- result_sf
      }, error = function(e) {
        if (!quiet) {
          cli::cli_alert_danger("Error converting to sf: {e$message}")
        }
      })
    }
  } else if (as_sf && !all(c("GPS_LONGITUDE", "GPS_LATITUDE") %in% names(result))) {
    if (!quiet) {
      warning("Cannot convert to sf: GPS_LONGITUDE and/or GPS_LATITUDE columns not found in the result")
    }
  }

  if (!quiet) {
    cli::cli_alert_success("All tables successfully joined. Final result has {nrow(result)} rows and {ncol(result)} columns")
  }

  return(result)
}


#' Execute joins between datasets based on a connection path
#'
#' This function performs joins between datasets following the optimal path
#' determined by find_optimal_path(), using relations defined in YAML.
#'
#' @param tibbles List of tibbles to join
#' @param path_info Path information as returned by find_optimal_path()
#' @param relations List of relation definitions from the YAML configuration
#' @param verbose Logical indicating whether to display progress messages
#' @param reference_date Optional date to filter records by
#' @param start_dataset Optional name of the dataset to start with. Default is NULL,
#'        which will use the first dataset from the first connection.
#'
#' @return A joined tibble
#'
#' @keywords internal
execute_dataset_joins <- function(tibbles, path_info, relations, verbose = TRUE,
                                  reference_date = NULL, start_dataset = NULL) {
  # If no connections, return NULL
  if (length(path_info$connections) == 0) {
    warning("No connections found in path_info")
    return(NULL)
  }

  # Check if connections are valid
  valid_connections <- list()
  for (conn in path_info$connections) {
    if (!is.null(conn$from) && !is.null(conn$to) &&
        !is.na(conn$from) && !is.na(conn$to)) {
      valid_connections[[length(valid_connections) + 1]] <- conn
    }
  }

  if (length(valid_connections) == 0) {
    warning("No valid connections found in path_info")
    return(NULL)
  }

  # Apply reference_date filtering to each individual dataset before joining
  if (!is.null(reference_date)) {
    if (verbose) {
      cli::cli_alert_info("Applying reference date filtering to all datasets: {reference_date}")
    }

    for (dataset_name in names(tibbles)) {
      # Probeer de tabel te filteren op datum - bij fout, gebruik de originele data
      tryCatch({
        filtered_data <- rio_filter_by_reference_date(tibbles[[dataset_name]], reference_date)
        tibbles[[dataset_name]] <- filtered_data
        if (verbose) {
          cli::cli_alert_info("Filtered '{dataset_name}' by reference date, now has {nrow(filtered_data)} rows")
        }
      }, error = function(e) {
        # Bij een fout, log een waarschuwing en gebruik de originele data
        if (verbose) {
          cli::cli_alert_warning("Could not filter '{dataset_name}' by reference date: {e$message}")
          cli::cli_alert_info("Using all data from '{dataset_name}' ({nrow(tibbles[[dataset_name]])} rows)")
        }
      })
    }
  }

  # Determine starting dataset
  if (!is.null(start_dataset) && start_dataset %in% names(tibbles)) {
    current_dataset <- start_dataset
  } else {
    # Default: Start with the dataset specified in the first connection
    first_connection <- valid_connections[[1]]
    current_dataset <- first_connection$from

    # Fallback if from is NA or NULL
    if (is.null(current_dataset) || is.na(current_dataset)) {
      current_dataset <- names(tibbles)[1]
    }
  }

  result <- tibbles[[current_dataset]]

  # Check if ID column exists in the first dataset
  has_id_column <- "id" %in% names(result)

  # Keep track of datasets we've already included
  visited <- c(current_dataset)

  if (verbose) {
    cli::cli_alert_info("Starting with dataset '{current_dataset}'")
  }

  # We need to create a joining plan that starts with our current_dataset
  # and joins all other datasets in a valid order
  remaining_connections <- valid_connections

  while (length(remaining_connections) > 0 && length(visited) < length(tibbles)) {
    # Find the next connection that links to a dataset we've already visited
    next_conn_idx <- NULL
    next_dataset <- NULL

    for (i in seq_along(remaining_connections)) {
      conn <- remaining_connections[[i]]

      if (conn$from %in% visited && !(conn$to %in% visited)) {
        next_conn_idx <- i
        next_dataset <- conn$to
        break
      } else if (conn$to %in% visited && !(conn$from %in% visited)) {
        next_conn_idx <- i
        next_dataset <- conn$from
        break
      }
    }

    # If we couldn't find a next connection, break the loop
    if (is.null(next_conn_idx)) {
      if (verbose) {
        cli::cli_alert_warning("Could not find a valid path to join all datasets from '{current_dataset}'")
      }
      break
    }

    # Get the current connection and remove it from the list
    curr_conn <- remaining_connections[[next_conn_idx]]
    remaining_connections <- remaining_connections[-next_conn_idx]

    # Determine from_dataset based on our visited datasets
    if (curr_conn$from %in% visited) {
      from_dataset <- curr_conn$from
      to_dataset <- curr_conn$to
    } else {
      from_dataset <- curr_conn$to
      to_dataset <- curr_conn$from
    }

    # Get the relationship information
    rel_key <- NULL

    # Find the relation in the relations list that matches these datasets
    for (key in names(relations)) {
      rel <- relations[[key]]
      if ((rel$from == from_dataset && rel$to == to_dataset) ||
          (rel$from == to_dataset && rel$to == from_dataset)) {
        rel_key <- key
        break
      }
    }

    if (is.null(rel_key)) {
      warning("Could not find relation definition for ", from_dataset, " -> ", to_dataset)
      next
    }

    rel <- relations[[rel_key]]

    # Format joining fields for display
    by_fields <- rel$by

    # Check if by_fields is properly structured
    if (is.null(by_fields)) {
      warning("No joining fields defined for relation between ", from_dataset, " and ", to_dataset)
      next
    }

    # Convert by_fields to string representation for display
    if (is.character(by_fields) && !is.null(names(by_fields)) && any(names(by_fields) != "")) {
      # Named character vector with different field names
      by_parts <- character(length(by_fields))
      for (i in seq_along(by_fields)) {
        if (names(by_fields)[i] == "") {
          by_parts[i] <- by_fields[i]
        } else {
          by_parts[i] <- paste0(names(by_fields)[i], " = ", by_fields[i])
        }
      }
      by_str <- paste(by_parts, collapse = ", ")
    } else {
      # Simple vector of field names (same in both tables)
      by_str <- paste(by_fields, collapse = ", ")
    }

    if (verbose) {
      cli::cli_alert_info("Joining '{to_dataset}' to result using: {by_str}")
    }

    # Determine which fields to use based on the direction
    if (rel$from != from_dataset) {
      # If the relation is defined in the opposite direction of our join,
      # we need to swap the names and values for named fields
      if (is.character(by_fields) && !is.null(names(by_fields)) && any(names(by_fields) != "")) {
        rev_by_fields <- setNames(names(by_fields), unname(by_fields))
        # Keep unnamed fields as is
        for (i in seq_along(by_fields)) {
          if (names(by_fields)[i] == "") {
            rev_by_fields <- c(rev_by_fields, by_fields[i])
          }
        }
        by_fields <- rev_by_fields
      }
    }

    # Prepare the next tibble to join (handle duplicate column names)
    next_tibble <- tibbles[[to_dataset]]

    # Handle the 'id' column - remove it from secondary tables
    if (has_id_column && "id" %in% names(next_tibble) && current_dataset != to_dataset) {
      next_tibble <- next_tibble[, !names(next_tibble) %in% "id"]
    }

    # Find overlapping column names (except join keys)
    join_keys <- if (is.character(by_fields) && !is.null(names(by_fields))) {
      unique(c(names(by_fields), unname(by_fields)))
    } else {
      by_fields
    }

    # Find columns with the same names in both datasets, excluding join keys
    overlap_cols <- setdiff(intersect(names(result), names(next_tibble)), join_keys)

    # Rename overlapping columns in the dataset to be joined
    if (length(overlap_cols) > 0) {
      for (col in overlap_cols) {
        names(next_tibble)[names(next_tibble) == col] <- paste0(to_dataset, "_", col)
      }
    }

    # Perform the join
    result <- dplyr::left_join(result, next_tibble, by = by_fields)

    if (verbose) {
      cli::cli_alert_success("Successfully joined '{to_dataset}', now at {nrow(result)} rows and {ncol(result)} columns")
    }

    # Mark as visited
    visited <- c(visited, to_dataset)
  }

  # Check if we visited all datasets
  if (length(visited) < length(tibbles)) {
    unvisited <- setdiff(names(tibbles), visited)
    warning("Could not join all datasets. The following datasets were not included: ",
            paste(unvisited, collapse = ", "))
  }

  return(result)
}

#' Find the optimal path to connect datasets
#'
#' This function finds the optimal path to connect multiple datasets based on
#' predefined relations. It uses graph theory to determine the best possible
#' connections between datasets.
#'
#' @param dataset_names Character vector with names of datasets to connect
#' @param relations List of relation definitions
#' @param verbose Logical indicating whether to display progress messages
#' @param start_with_first Logical indicating whether to force starting with the first dataset.
#'        Default is TRUE.
#'
#' @return A list with path information, including:
#'   - path: The sequence of datasets to visit
#'   - connections: Details about each connection in the path
#'
#' @keywords internal
find_optimal_path <- function(dataset_names, relations, verbose = TRUE, start_with_first = TRUE) {
  # Create an empty graph
  g <- igraph::make_empty_graph(n = 0, directed = FALSE)

  # Add all datasets as vertices
  for (dataset in dataset_names) {
    g <- igraph::add_vertices(g, 1, name = dataset)
  }

  # Add edges for all applicable relations
  applicable_relations <- list()

  for (rel_name in names(relations)) {
    rel <- relations[[rel_name]]

    # Only consider relations between the datasets we have
    if (rel$from %in% dataset_names && rel$to %in% dataset_names) {
      # Add this relation as an edge
      from_idx <- which(igraph::V(g)$name == rel$from)
      to_idx <- which(igraph::V(g)$name == rel$to)

      if (length(from_idx) > 0 && length(to_idx) > 0) {
        g <- igraph::add_edges(g, c(from_idx, to_idx))

        # Store relation details for the edge
        edge_id <- igraph::ecount(g)
        igraph::E(g)$relation_name[edge_id] <- rel_name

        # Set weight based on confidence
        confidence_level <- ifelse(is.null(rel$confidence), "medium", rel$confidence)
        igraph::E(g)$weight[edge_id] <- ifelse(confidence_level == "high", 1, 2)

        # Store directly which vertices this edge connects for later reference
        igraph::E(g)$from_dataset[edge_id] <- rel$from
        igraph::E(g)$to_dataset[edge_id] <- rel$to

        # Add to applicable relations
        applicable_relations[[rel_name]] <- rel

        if (verbose) {
          cli::cli_alert_success("Found relation between '{rel$from}' and '{rel$to}'")
        }
      }
    }
  }

  # Check if we have any applicable relations
  if (igraph::ecount(g) == 0) {
    stop("No applicable relations found between the specified datasets")
  }

  # Check if we can connect all datasets
  if (!igraph::is_connected(g)) {
    unconnected_sets <- igraph::components(g)$membership
    groups <- split(dataset_names, unconnected_sets)

    stop("Cannot connect all datasets with the provided relations. Detected disconnected groups: ",
         paste(sapply(groups, function(group) paste("(", paste(group, collapse = ", "), ")", sep = "")),
               collapse = " and "))
  }

  # Determine which dataset to start with
  if (start_with_first && length(dataset_names) > 0) {
    start_dataset <- dataset_names[1]

    if (verbose) {
      cli::cli_alert_info("Forcing start from dataset: '{start_dataset}'")
    }
  } else {
    # Choose a central node as a good starting point
    eigen_cent <- igraph::eigen_centrality(g)
    central_idx <- which.max(eigen_cent$vector)
    start_dataset <- igraph::V(g)$name[central_idx]

    if (verbose) {
      cli::cli_alert_info("Using central dataset as start: '{start_dataset}'")
    }
  }

  # Instead of using BFS tree, we'll manually build connections starting from start_dataset
  visited <- start_dataset
  unvisited <- setdiff(dataset_names, visited)
  path_info <- list(
    path = dataset_names,
    connections = list()
  )

  # Function to find the best next dataset to connect to
  find_next_dataset <- function() {
    best_dataset <- NULL
    best_relation <- NULL

    # Check all edges between visited and unvisited datasets
    for (rel_name in names(relations)) {
      rel <- relations[[rel_name]]

      if (rel$from %in% visited && rel$to %in% unvisited) {
        best_dataset <- rel$to
        best_relation <- rel_name
        break
      } else if (rel$to %in% visited && rel$from %in% unvisited) {
        best_dataset <- rel$from
        best_relation <- rel_name
        break
      }
    }

    return(list(dataset = best_dataset, relation = best_relation))
  }

  # Iteratively build connections until all datasets are visited
  while (length(unvisited) > 0) {
    next_info <- find_next_dataset()

    if (is.null(next_info$dataset)) {
      warning("Unable to find path to connect all datasets from '", start_dataset, "'")
      break
    }

    # Get the relation
    rel <- relations[[next_info$relation]]

    # Determine from/to correctly based on whether from or to is in visited
    if (rel$from %in% visited) {
      from_dataset <- rel$from
      to_dataset <- rel$to
    } else {
      from_dataset <- rel$to
      to_dataset <- rel$from
    }

    # Add this connection to path_info
    path_info$connections[[length(path_info$connections) + 1]] <- list(
      from = from_dataset,
      to = to_dataset,
      relationship = rel
    )

    if (verbose) {
      cli::cli_alert_info("Adding connection: {from_dataset} -> {to_dataset}")
    }

    # Update visited/unvisited
    visited <- c(visited, to_dataset)
    unvisited <- setdiff(unvisited, to_dataset)
  }

  return(path_info)
}

#' Create a nested tibble from join results
#'
#' This function reorganizes the joined data into a nested tibble structure,
#' which can be more intuitive for certain types of analysis.
#'
#' @param joined_data The result of joining datasets
#' @param dataset_names Names of the original datasets
#' @param path_info Path information as returned by find_optimal_path()
#'
#' @return A nested tibble
#'
#' @keywords internal
nest_result <- function(joined_data, dataset_names, path_info) {
  # Check for required packages
  if (!requireNamespace("tidyr", quietly = TRUE)) {
    stop("Package 'tidyr' is required for nested results")
  }

  # Copy the joined data to avoid modifying the original
  nested_data <- joined_data

  # First identify columns that likely belong to each dataset
  # We'll use column prefixes to identify the source dataset
  datasets_columns <- list()

  # Define some common key columns that should not be nested
  common_keys <- c("id", grep("CODE$|ID$|NUMBER$", names(nested_data), value = TRUE))

  # For datasets without assigned columns, try to extract them from the remaining columns
  remaining_cols <- setdiff(names(nested_data), common_keys)

  # Loop through each dataset and find corresponding columns
  for (dataset in dataset_names) {
    # Look for columns with the dataset name as a prefix
    prefix_pattern <- paste0("^", dataset, "_")
    dataset_cols <- grep(prefix_pattern, remaining_cols, value = TRUE)

    if (length(dataset_cols) > 0) {
      datasets_columns[[dataset]] <- dataset_cols
      remaining_cols <- setdiff(remaining_cols, dataset_cols)
    }
  }

  # Assign remaining columns to datasets based on connections in path_info
  if (length(remaining_cols) > 0 && !is.null(path_info$connections)) {
    # First, determine the order of datasets from connections
    dataset_order <- character(0)
    for (conn in path_info$connections) {
      if (!is.null(conn$from) && !is.null(conn$to)) {
        dataset_order <- c(dataset_order, conn$from, conn$to)
      }
    }
    dataset_order <- unique(dataset_order)

    # Then, distribute remaining columns based on this order
    for (dataset in dataset_order) {
      if (!(dataset %in% names(datasets_columns))) {
        datasets_columns[[dataset]] <- character(0)
      }
    }

    # Distribute remaining columns to datasets based on best guess
    # For simplicity, we'll assign each column to the first dataset that doesn't have any columns yet
    for (col in remaining_cols) {
      assigned <- FALSE
      for (dataset in names(datasets_columns)) {
        if (length(datasets_columns[[dataset]]) == 0) {
          datasets_columns[[dataset]] <- c(datasets_columns[[dataset]], col)
          assigned <- TRUE
          break
        }
      }

      # If not assigned to any dataset yet, add to the first dataset
      if (!assigned && length(datasets_columns) > 0) {
        first_dataset <- names(datasets_columns)[1]
        datasets_columns[[first_dataset]] <- c(datasets_columns[[first_dataset]], col)
      }
    }
  }

  # Create a nested tibble using tidyr
  for (dataset in names(datasets_columns)) {
    cols <- datasets_columns[[dataset]]

    if (length(cols) > 0) {
      # Nest these columns
      tryCatch({
        nested_data <- tidyr::nest(nested_data, !!dataset := cols)
      }, error = function(e) {
        warning("Failed to nest columns for dataset '", dataset, "': ", e$message)
      })
    }
  }

  # If no nesting happened, warn the user
  if (ncol(nested_data) == ncol(joined_data)) {
    warning("Could not create nested structure. No columns identified for nesting.")
  }

  return(nested_data)
}
