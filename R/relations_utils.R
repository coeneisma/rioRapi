#' Find the optimal path to connect datasets
#'
#' This function finds the optimal path to connect multiple datasets based on
#' predefined relations. It uses graph theory to determine the best possible
#' connections between datasets.
#'
#' @param dataset_names Character vector with names of datasets to connect
#' @param relations List of relation definitions
#' @param verbose Logical indicating whether to display progress messages
#'
#' @return A list with path information, including:
#'   - path: The sequence of datasets to visit
#'   - connections: Details about each connection in the path
#'
#' @keywords internal
find_optimal_path <- function(dataset_names, relations, verbose = TRUE) {
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
        igraph::E(g)$weight[edge_id] <- ifelse(rel$confidence == "high", 1, 2)  # Weight by confidence
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

  # Find the optimal path (spanning tree)
  if (length(dataset_names) > 2) {
    # For more than 2 datasets, use minimum spanning tree to find optimal connections
    mst <- igraph::mst(g)
  } else {
    # For just 2 datasets, use the shortest path
    shortest_path <- igraph::shortest_paths(g,
                                            from = which(igraph::V(g)$name == dataset_names[1]),
                                            to = which(igraph::V(g)$name == dataset_names[2]),
                                            weights = igraph::E(g)$weight)
    path_vertices <- shortest_path$vpath[[1]]
    mst <- igraph::induced_subgraph(g, path_vertices)
  }

  # Extract the path information
  path_info <- list(
    path = dataset_names,
    connections = list()
  )

  # Process each edge in the MST to build the connection sequence
  for (e_id in 1:igraph::ecount(mst)) {
    # Gebruik de opgeslagen dataset namen
    from_dataset <- igraph::E(mst)$from_dataset[e_id]
    to_dataset <- igraph::E(mst)$to_dataset[e_id]

    # Als de dataset namen niet direct beschikbaar zijn, gebruik de vertex indices
    if (is.null(from_dataset) || is.null(to_dataset)) {
      edge <- igraph::E(mst)[[e_id]]
      vertices <- igraph::ends(mst, edge)
      from_dataset <- igraph::V(mst)$name[vertices[1]]
      to_dataset <- igraph::V(mst)$name[vertices[2]]
    }

    rel_name <- igraph::E(mst)$relation_name[e_id]

    # Add this connection to the path_info
    path_info$connections[[length(path_info$connections) + 1]] <- list(
      from = from_dataset,
      to = to_dataset,
      relationship = relations[[rel_name]]
    )

    if (verbose) {
      cli::cli_alert_info("Adding connection: {from_dataset} -> {to_dataset}")
    }
  }

  return(path_info)
}


#' Execute joins between datasets based on a connection path
#'
#' This function performs joins between datasets following the optimal path
#' determined by find_optimal_path().
#'
#' @param tibbles List of tibbles to join
#' @param path_info Path information as returned by find_optimal_path()
#' @param relations List of relation definitions
#' @param verbose Logical indicating whether to display progress messages
#' @param reference_date Optional date to filter records by
#'
#' @return A joined tibble
#'
#' @keywords internal
execute_dataset_joins <- function(tibbles, path_info, relations, verbose = TRUE, reference_date = NULL) {
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

  # Start with the first dataset in the first connection
  first_connection <- valid_connections[[1]]
  current_dataset <- first_connection$from

  # Fallback if from is NA or NULL
  if (is.null(current_dataset) || is.na(current_dataset)) {
    current_dataset <- names(tibbles)[1]
  }

  result <- tibbles[[current_dataset]]

  # Keep track of datasets we've already included
  visited <- c(current_dataset)

  if (verbose) {
    cli::cli_alert_info("Starting with dataset '{current_dataset}'")
  }

  # Process each connection in the path
  for (conn in valid_connections) {
    # Skip invalid connections
    if (is.null(conn$from) || is.null(conn$to) ||
        is.na(conn$from) || is.na(conn$to)) {
      next
    }

    # Determine which dataset to join next
    if (conn$from %in% visited && !(conn$to %in% visited)) {
      from_dataset <- conn$from
      next_dataset <- conn$to
    } else if (conn$to %in% visited && !(conn$from %in% visited)) {
      from_dataset <- conn$to
      next_dataset <- conn$from
    } else {
      # Both datasets already visited, skip this connection
      next
    }

    # Get the relationship information
    rel <- conn$relationship

    # Format joining fields for display
    if (is.null(names(rel$by))) {
      by_str <- paste(rel$by, collapse = ", ")
    } else {
      by_parts <- character(length(rel$by))
      for (i in seq_along(rel$by)) {
        if (names(rel$by)[i] == "") {
          by_parts[i] <- rel$by[i]
        } else {
          by_parts[i] <- paste0(names(rel$by)[i], " = ", rel$by[i])
        }
      }
      by_str <- paste(by_parts, collapse = ", ")
    }

    if (verbose) {
      cli::cli_alert_info("Joining '{next_dataset}' to result using: {by_str}")
    }

    # Determine which fields to use based on the direction
    by_fields <- rel$by
    if (rel$from != from_dataset) {
      # If the relation is defined in the opposite direction of our join,
      # we need to swap the names and values for named fields
      if (!is.null(names(by_fields)) && any(names(by_fields) != "")) {
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

    # Perform the join
    result <- dplyr::left_join(result, tibbles[[next_dataset]], by = by_fields)

    if (verbose) {
      cli::cli_alert_success("Successfully joined '{next_dataset}', now at {nrow(result)} rows and {ncol(result)} columns")
    }

    # Mark as visited
    visited <- c(visited, next_dataset)
  }

  # Apply reference date filtering if specified
  if (!is.null(reference_date)) {
    if (verbose) {
      cli::cli_alert_info("Filtering by reference date: {reference_date}")
    }

    result <- rio_filter_by_reference_date(result, reference_date)
  }

  return(result)
}


#' Visualize the connection path between datasets
#'
#' This function creates a visualization of how datasets are connected based
#' on the optimal path determined by find_optimal_path().
#'
#' @param path_info Path information as returned by find_optimal_path()
#' @param relations List of relation definitions
#' @param dataset_names Character vector with names of datasets to visualize
#' @param interactive Logical indicating whether to create an interactive visualization
#'
#' @return A visualization object (igraph plot or visNetwork object)
#'
#' @keywords internal
visualize_path <- function(path_info, relations, dataset_names, interactive = TRUE) {
  # Create a graph
  g <- igraph::make_empty_graph(n = length(dataset_names), directed = FALSE)
  igraph::V(g)$name <- dataset_names

  # Add edges for the connections in the path
  edge_labels <- character(0)
  edge_colors <- character(0)
  edge_widths <- numeric(0)

  for (conn in path_info$connections) {
    if (is.null(conn$from) || is.null(conn$to) || is.na(conn$from) || is.na(conn$to)) {
      next  # Skip invalid connections
    }

    # Get indices of dataset names
    from_idx <- which(igraph::V(g)$name == conn$from)
    to_idx <- which(igraph::V(g)$name == conn$to)

    if (length(from_idx) == 0 || length(to_idx) == 0) {
      next  # Skip if dataset names are not found
    }

    # Add edge
    g <- igraph::add_edges(g, c(from_idx, to_idx))

    # Get relationship
    rel <- conn$relationship

    # Create edge label from the joining fields
    if (is.null(names(rel$by))) {
      # Unnamed fields (same name in both datasets)
      label <- paste(rel$by, collapse = "\n")
    } else {
      # Named fields (different names in source and target)
      label_parts <- character(0)
      for (i in seq_along(rel$by)) {
        if (names(rel$by)[i] == "") {
          label_parts <- c(label_parts, rel$by[i])
        } else {
          label_parts <- c(label_parts, paste(names(rel$by)[i], "=", rel$by[i]))
        }
      }
      label <- paste(label_parts, collapse = "\n")
    }

    edge_id <- igraph::ecount(g)
    edge_labels[edge_id] <- label

    # Set color and width based on confidence
    edge_colors[edge_id] <- ifelse(rel$confidence == "high", "green", "orange")
    edge_widths[edge_id] <- ifelse(rel$confidence == "high", 3, 2)
  }

  # Set edge attributes
  if (length(edge_labels) > 0) {
    igraph::E(g)$label <- edge_labels
    igraph::E(g)$color <- edge_colors
    igraph::E(g)$width <- edge_widths
  }

  # Set vertex attributes
  igraph::V(g)$label <- igraph::V(g)$name
  igraph::V(g)$color <- "lightblue"

  # Create visualization
  if (interactive) {
    # Interactive visualization with visNetwork
    if (!requireNamespace("visNetwork", quietly = TRUE)) {
      warning("Package 'visNetwork' is required for interactive visualization. Using static visualization instead.")
      interactive <- FALSE
    } else {
      nodes <- data.frame(
        id = igraph::V(g)$name,
        label = igraph::V(g)$name,
        title = igraph::V(g)$name,  # Tooltip
        color = "lightblue",
        stringsAsFactors = FALSE
      )

      if (igraph::ecount(g) > 0) {
        edges <- data.frame(
          from = igraph::get.edgelist(g)[, 1],
          to = igraph::get.edgelist(g)[, 2],
          label = igraph::E(g)$label,
          title = igraph::E(g)$label,  # Tooltip for hover
          color = igraph::E(g)$color,
          width = igraph::E(g)$width,
          font = list(color = "red", size = 12),
          stringsAsFactors = FALSE
        )

        return(visNetwork::visNetwork(nodes, edges) |>
                 visNetwork::visOptions(highlightNearest = TRUE, selectedBy = "label") |>
                 visNetwork::visEdges(font = list(color = "red", size = 12)) |>
                 visNetwork::visNodes(font = list(size = 14)) |>
                 visNetwork::visLayout(randomSeed = 123) |>
                 visNetwork::visPhysics(solver = "forceAtlas2Based"))
      } else {
        # No edges, just display nodes
        return(visNetwork::visNetwork(nodes) |>
                 visNetwork::visOptions(highlightNearest = TRUE, selectedBy = "label") |>
                 visNetwork::visNodes(font = list(size = 14)) |>
                 visNetwork::visLayout(randomSeed = 123))
      }
    }
  }

  if (!interactive) {
    # Static visualization with igraph
    plot(g,
         layout = igraph::layout_with_fr(g),
         vertex.size = 20,
         vertex.label.color = "black",
         vertex.label.cex = 0.8,
         edge.label.cex = 0.7,
         edge.curved = 0.2,
         main = "Dataset Connection Path")

    # Add legend if we have edges
    if (igraph::ecount(g) > 0) {
      legend("bottomright",
             legend = c("High Confidence", "Medium Confidence"),
             col = c("green", "orange"),
             lwd = c(3, 2),
             cex = 0.8)
    }

    # Return the graph for possible further usage
    invisible(g)
  }
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
  # This is a simplified implementation
  # A more comprehensive version would need to analyze column prefixes
  # and properly organize the nesting structure

  # First, identify columns from each dataset
  # This is a simplification; in reality you'd need a more robust way to
  # determine which columns came from which dataset

  # For now, we'll just create a single level of nesting based on common prefixes
  # or using the dataset names to guide the nesting

  # Find all column names
  all_cols <- colnames(joined_data)

  # Try to determine columns from each dataset
  # This is a very simple approach and might need refinement
  nested_data <- joined_data

  # Attempt to nest columns with common prefixes
  for (dataset in dataset_names) {
    # Look for columns that might belong to this dataset
    # We'll look for columns with the dataset name as a prefix
    dataset_cols <- grep(paste0("^", dataset, "_"), all_cols, value = TRUE)

    if (length(dataset_cols) > 0) {
      # Nest these columns
      nested_data <- nested_data |>
        dplyr::nest(!!dataset := dataset_cols)
    } else {
      # Simple fallback - look for common prefixes in general
      # This is very simplistic and would need refinement
      col_prefixes <- unique(sub("_.*$", "", all_cols))

      for (prefix in col_prefixes) {
        prefix_cols <- grep(paste0("^", prefix, "_"), all_cols, value = TRUE)

        if (length(prefix_cols) > 3) {  # Arbitrary threshold
          nested_data <- nested_data |>
            dplyr::nest(!!prefix := prefix_cols)
        }
      }
    }
  }

  return(nested_data)
}


#' Join datasets using predefined relations
#'
#' This function automatically joins multiple datasets using predefined relations.
#' It finds the optimal path between datasets and performs the joins accordingly.
#'
#' @param ... Named tibbles to join or character vector of dataset names to load.
#' @param by_names Logical indicating whether the arguments are dataset names
#'        rather than actual datasets. Default is FALSE.
#' @param relations Optional list of relations. If NULL, relations are loaded from the default file.
#' @param reference_date Optional date to filter valid records. Default is NULL (no filtering).
#' @param nested Logical indicating whether to return the result as a nested tibble.
#'        Default is FALSE.
#' @param visualize Logical indicating whether to return a visualization of the connection path
#'        instead of the joined data. Default is FALSE.
#' @param verbose Logical indicating whether to display detailed connection information.
#'        Default is TRUE.
#'
#' @return A joined tibble with data from all input tibbles, or a visualization object if visualize = TRUE.
#'
#' @examples
#' \dontrun{
#' # Provide actual tibbles
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#' locaties <- rio_get_data(dataset_name = "onderwijslocaties")
#' joined_data <- rio_join_datasets(vestigingen = vestigingen, locaties = locaties)
#'
#' # Or provide dataset names (will load data automatically)
#' joined_data <- rio_join_datasets("vestigingserkenningen",
#'                                  "onderwijslocaties",
#'                                  by_names = TRUE)
#'
#' # Create a nested result
#' nested_data <- rio_join_datasets("vestigingserkenningen",
#'                                  "onderwijslocaties",
#'                                  by_names = TRUE,
#'                                  nested = TRUE)
#'
#' # Visualize the connection path
#' viz <- rio_join_datasets("vestigingserkenningen",
#'                          "onderwijslocaties",
#'                          by_names = TRUE,
#'                          visualize = TRUE)
#' }
#'
#' @export
rio_join_datasets <- function(..., by_names = FALSE, relations = NULL,
                              reference_date = NULL, nested = FALSE,
                              visualize = FALSE, verbose = TRUE) {
  # Process inputs
  input_args <- list(...)

  # Handle two different input types
  if (by_names) {
    # Input is dataset names, load the data
    dataset_names <- unlist(input_args)
    if (verbose) {
      cli::cli_alert_info("Loading {length(dataset_names)} datasets")
    }
    tibbles <- list()
    for (name in dataset_names) {
      if (verbose) {
        cli::cli_alert_info("Loading dataset: {name}")
      }
      tibbles[[name]] <- rio_get_data(dataset_name = name)
    }
  } else {
    # Input is already loaded tibbles
    tibbles <- input_args
    dataset_names <- names(tibbles)
  }

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for joining")
  }

  # Load relations if not provided
  if (is.null(relations)) {
    relations <- rio_load_relations()
    if (length(relations) == 0) {
      stop("No relations defined. Please define relations first.")
    }
  }

  # Check for unnamed parameters when not using by_names
  if (!by_names && "" %in% dataset_names) {
    stop("When providing tibbles directly, all must be named. Example: rio_join_datasets(dataset1 = df1, dataset2 = df2)")
  }

  if (verbose) {
    cli::cli_alert_info("Joining {length(dataset_names)} datasets: {paste(dataset_names, collapse = ', ')}")
  }

  # Find the optimal path to connect the datasets
  path_info <- find_optimal_path(dataset_names, relations, verbose)

  # If visualization is requested, return that instead
  if (visualize) {
    return(visualize_path(path_info, relations, dataset_names))
  }

  # Execute the joins based on the optimal path
  result <- execute_dataset_joins(tibbles, path_info, relations, verbose, reference_date)

  # Format as nested tibble if requested
  if (nested) {
    result <- nest_result(result, dataset_names, path_info)
  }

  if (verbose) {
    cli::cli_alert_success("All datasets successfully joined. Final result has {nrow(result)} rows and {ncol(result)} columns")
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

#' Get metadata of a dataset
#'
#' This function retrieves the metadata of a specific dataset in the RIO structure.
#'
#' @param datasets The RIO datasets structure as returned by rio_load_datasets().
#' @param dataset_name Name of the dataset.
#'
#' @return A list with the metadata of the dataset.
#'
#' @keywords internal
rio_get_dataset_metadata <- function(datasets, dataset_name) {
  # Check if the dataset exists
  if (!dataset_name %in% names(datasets$datasets)) {
    stop("Dataset '", dataset_name, "' does not exist in the structure")
  }

  # Collect all metadata (excluding the fields)
  metadata <- datasets$datasets[[dataset_name]]

  # We make a copy without the fields, which are often very large
  if (!is.null(metadata$fields)) {
    metadata$fields <- NULL
  }

  return(metadata)
}

#' Create a new relation for the RIO structure
#'
#' This function creates a new relation definition between two datasets.
#'
#' @param from Name of the source dataset.
#' @param to Name of the target dataset.
#' @param by A character vector of joining fields. If these fields have the same name in both datasets,
#'        provide them as a character vector. If they have different names, provide a named vector
#'        where names correspond to fields in the 'from' dataset and values to fields in the 'to' dataset.
#' @param type Type of relation (one-to-one, one-to-many, many-to-one, many-to-many). Default is "one-to-many".
#' @param confidence Confidence of the relation (high, medium). Default is "high".
#' @param description Optional description of the relation.
#'
#' @return A relation object.
#'
#' @examples
#' # Same field names in both datasets
#' relation1 <- rio_create_relation(
#'   from = "dataset1",
#'   to = "dataset2",
#'   by = "COMMON_ID"
#' )
#'
#' # Different field names
#' relation2 <- rio_create_relation(
#'   from = "dataset1",
#'   to = "dataset2",
#'   by = c("ID_FROM" = "ID_TO")
#' )
#'
#' # Multiple fields
#' relation3 <- rio_create_relation(
#'   from = "dataset1",
#'   to = "dataset2",
#'   by = c("FIELD1", "FIELD2")  # Same names in both datasets
#' )
#'
#' # Multiple fields with different names
#' relation4 <- rio_create_relation(
#'   from = "dataset1",
#'   to = "dataset2",
#'   by = c("ID1" = "ID_A", "ID2" = "ID_B")
#' )
#'
#' @keywords internal
rio_create_relation <- function(from, to, by, type = "one-to-many",
                                confidence = "high", description = NULL) {
  # Check if 'by' is properly specified
  if (!is.character(by) || length(by) == 0) {
    stop("'by' must be a character vector specifying joining fields")
  }

  # Create a new relation
  relation <- list(
    from = from,
    to = to,
    type = type,
    by = by,  # Store the original 'by' for user reference
    confidence = confidence
  )

  # Add description if present
  if (!is.null(description)) {
    relation$description <- description
  }

  return(relation)
}

#' Add a relation to a list of RIO relations
#'
#' This function adds a relation to a list of RIO relations.
#'
#' @param relations The list of RIO relations.
#' @param relation The relation to add or a list with arguments for rio_create_relation().
#'
#' @return The updated list of RIO relations.
#'
#' @keywords internal
rio_add_relation <- function(relations, relation) {
  # If relation is not a list with the required fields, assume it's a relation created by rio_create_relation
  if (!is.list(relation) || !all(c("from", "to", "by") %in% names(relation))) {
    stop("relation must be a relation object created with rio_create_relation()")
  }

  # Generate a key for the relation
  relation_key <- paste(relation$from, "to", relation$to)

  # Check if the relation already exists
  if (relation_key %in% names(relations)) {
    warning("A relation between '", relation$from, "' and '", relation$to,
            "' already exists and will be overwritten")
  }

  # Add the relation to the list
  relations[[relation_key]] <- relation

  return(relations)
}


#' Remove a relation from a list of RIO relations
#'
#' This function removes a relation from a list of RIO relations.
#'
#' @param relations The list of RIO relations.
#' @param from Name of the source dataset.
#' @param to Name of the target dataset.
#'
#' @return The updated list of RIO relations.
#'
#' @keywords internal
rio_remove_relation <- function(relations, from, to) {
  # Generate the key for the relation
  relation_key <- paste(from, "to", to)

  # Check if the relation exists
  if (!relation_key %in% names(relations)) {
    warning("Relation between '", from, "' and '", to, "' not found")
    return(relations)
  }

  # Remove the relation
  relations[[relation_key]] <- NULL

  return(relations)
}

#' List all relations in the RIO structure
#'
#' This function provides an overview of all defined relations.
#'
#' @param relations The list of RIO relations.
#' @param as_dataframe Logical indicating whether the result should be returned as a dataframe.
#'        Default is TRUE.
#'
#' @return A dataframe or list with all relations.
#'
#' @keywords internal
rio_list_relations <- function(relations, as_dataframe = TRUE) {
  if (length(relations) == 0) {
    if (as_dataframe) {
      return(data.frame(
        from = character(0),
        to = character(0),
        type = character(0),
        by = character(0),
        confidence = character(0),
        description = character(0),
        stringsAsFactors = FALSE
      ))
    } else {
      return(list())
    }
  }

  if (as_dataframe) {
    # Convert to dataframe
    relations_list <- list()

    for (rel_name in names(relations)) {
      rel <- relations[[rel_name]]

      # Convert 'by' to a string representation
      if (is.null(names(rel$by))) {
        # Unnamed vector, all fields have the same name in both datasets
        by_str <- paste(rel$by, collapse = ", ")
      } else {
        # Named vector, fields have different names
        by_parts <- character(length(rel$by))
        for (i in seq_along(rel$by)) {
          if (names(rel$by)[i] == "") {
            # Unnamed element
            by_parts[i] <- rel$by[i]
          } else {
            # Named element, format as "from_field = to_field"
            by_parts[i] <- paste0(names(rel$by)[i], " = ", rel$by[i])
          }
        }
        by_str <- paste(by_parts, collapse = ", ")
      }

      relations_list[[length(relations_list) + 1]] <- data.frame(
        from = rel$from,
        to = rel$to,
        type = rel$type,
        by = by_str,
        confidence = rel$confidence,
        description = ifelse(is.null(rel$description), NA, rel$description),
        stringsAsFactors = FALSE
      )
    }

    relations_df <- do.call(rbind, relations_list)
    return(relations_df)
  } else {
    # Return as is
    return(relations)
  }
}

#' Load the RIO relations from a YAML file
#'
#' This function loads the RIO relations between tables from a YAML file.
#' If the file doesn't exist, an empty relation object is returned.
#'
#' @param file_path Path to the YAML file. Default is 'inst/extdata/rio_relations.yaml'.
#'
#' @return A list with the RIO relations.
#'
#' @keywords internal
rio_load_relations <- function(file_path = system.file("extdata", "rio_relations.yaml", package = "rioRapi")) {
  # Check if the file exists
  if (!file.exists(file_path)) {
    message("No relations file found at ", file_path, ". An empty relation object will be returned.")
    return(list())
  }

  # Load YAML file
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required to load the relations")
  }

  yaml_data <- yaml::read_yaml(file_path)

  return(yaml_data)
}

#' Save the RIO relations as a YAML file
#'
#' This function saves the RIO relations as a YAML file.
#'
#' @param relations The RIO relations.
#' @param file_path Path to the YAML file where the relations will be saved.
#'        Default is 'inst/extdata/rio_relations.yaml'.
#' @param create_dir Logical indicating whether to create the directory if it doesn't exist.
#'        Default is TRUE.
#'
#' @return Invisibly the name of the file where the relations were saved.
#'
#' @keywords internal
rio_save_relations <- function(relations, file_path = "inst/extdata/rio_relations.yaml", create_dir = TRUE) {
  # Check if the directory exists and create it if necessary
  dir_path <- dirname(file_path)
  if (!dir.exists(dir_path) && create_dir) {
    dir.create(dir_path, recursive = TRUE)
  }

  # Check if yaml package is installed
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required to save the relations")
  }

  # Save relations as YAML
  yaml::write_yaml(relations, file_path)

  message("RIO relations saved to: ", file_path)

  return(invisible(file_path))
}

#' Export RIO relations to a CSV file
#'
#' This function exports RIO relations to a CSV file.
#'
#' @param relations The list of RIO relations.
#' @param file_path Path to the CSV file where the relations will be saved.
#'
#' @return Invisibly the name of the file where the relations were saved.
#'
#' @keywords internal
rio_export_relations_csv <- function(relations, file_path = "rio_relations.csv") {
  # Convert relations to dataframe
  relations_df <- rio_list_relations(relations, as_dataframe = TRUE)

  # Write to CSV
  utils::write.csv(relations_df, file_path, row.names = FALSE)

  message("RIO relations exported to CSV: ", file_path)

  return(invisible(file_path))
}

#' Import relations from a CSV file
#'
#' This function imports relations from a CSV file and returns a list of relations.
#'
#' @param file_path Path to the CSV file with the relations.
#'
#' @return A list of RIO relations.
#'
#' @keywords internal
rio_import_relations_csv <- function(file_path) {
  # Read the CSV file
  relations_df <- utils::read.csv(file_path, stringsAsFactors = FALSE)

  # Initialize an empty list
  relations <- list()

  # Add each relation to the list
  for (i in 1:nrow(relations_df)) {
    rel <- relations_df[i, ]

    # Parse 'by' string into a named vector
    by_parts <- strsplit(rel$by, ",\\s*")[[1]]
    by_vector <- character(0)

    for (part in by_parts) {
      # Check if the part has format "field1 = field2"
      if (grepl("\\s*=\\s*", part)) {
        field_parts <- strsplit(part, "\\s*=\\s*")[[1]]
        if (length(field_parts) == 2) {
          by_vector[field_parts[1]] <- field_parts[2]
        } else {
          warning("Invalid 'by' format: ", part)
        }
      } else {
        # Simple field name, add as unnamed element
        by_vector <- c(by_vector, part)
      }
    }

    # Create a new relation
    relation <- list(
      from = rel$from,
      to = rel$to,
      type = rel$type,
      by = by_vector,
      confidence = rel$confidence
    )

    # Add description if present
    if ("description" %in% names(rel) && !is.na(rel$description)) {
      relation$description <- rel$description
    }

    # Generate a key for the relation
    relation_key <- paste(relation$from, "to", relation$to)

    # Add to the list
    relations[[relation_key]] <- relation
  }

  return(relations)
}

#' Join datasets using defined relations
#'
#' This function joins multiple datasets by finding the optimal path through predefined relations.
#' It uses graph theory to determine the best possible way to connect the datasets.
#'
#' @param ... Named tibbles to join.
#' @param relations Optional list of relations. If NULL, relations are loaded from the default file.
#' @param reference_date Optional date to filter valid records. Default is NULL (no filtering).
#' @param visualize Logical indicating whether to return a visualization of the connection path
#'        instead of the joined data. Default is FALSE.
#' @param verbose Logical indicating whether to display detailed connection information.
#'        Default is TRUE.
#'
#' @return A joined tibble with data from all input tibbles, or a visualization object if visualize = TRUE.
#'
#' @examples
#' \dontrun{
#' # Retrieve data from different datasets
#' locations <- rio_get_data(dataset_name = "onderwijslocaties")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#'
#' # Join the datasets with predefined relations
#' joined_data <- rio_join_by_relations(
#'   locations = locations,
#'   institutions = institutions
#' )
#'
#' # Visualize the connection path instead of joining
#' connection_viz <- rio_join_by_relations(
#'   locations = locations,
#'   institutions = institutions,
#'   visualize = TRUE
#' )
#' }
#'
#' @export
rio_join_by_relations <- function(..., relations = NULL, reference_date = NULL,
                                  visualize = FALSE, verbose = TRUE) {
  # Get the input tibbles
  tibbles <- list(...)

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for joining")
  }

  # Load relations if not provided
  if (is.null(relations)) {
    relations <- rio_load_relations()
    if (length(relations) == 0) {
      stop("No relations defined. Please define relations first.")
    }
  }

  # Get dataset names
  dataset_names <- names(tibbles)

  # Check for unnamed parameters
  if ("" %in% dataset_names) {
    stop("All datasets must be named. Example: rio_join_by_relations(dataset1 = df1, dataset2 = df2)")
  }

  if (verbose) {
    cli::cli_alert_info("Joining {length(dataset_names)} datasets: {paste(dataset_names, collapse = ', ')}")
  }

  # Create a graph to find the best path through the datasets
  g <- igraph::graph.empty(n = length(dataset_names), directed = FALSE)
  igraph::V(g)$name <- dataset_names

  # Check which relations we can use between our datasets
  applicable_relations <- list()
  edges_added <- 0

  for (rel_name in names(relations)) {
    rel <- relations[[rel_name]]

    # Only consider relations between datasets we have
    if (rel$from %in% dataset_names && rel$to %in% dataset_names) {
      # Add this relation as an edge
      g <- igraph::add_edges(g, c(which(dataset_names == rel$from), which(dataset_names == rel$to)))

      # Store relation details for the edge
      edge_id <- igraph::ecount(g)
      igraph::E(g)$relation_key[edge_id] <- rel_name
      igraph::E(g)$weight[edge_id] <- ifelse(rel$confidence == "high", 1, 2)  # Weight by confidence

      # Add to applicable relations
      applicable_relations[[rel_name]] <- rel
      edges_added <- edges_added + 1

      if (verbose) {
        cli::cli_alert_success("Found relation between '{rel$from}' and '{rel$to}'")
      }
    }
  }

  if (edges_added == 0) {
    stop("No applicable relations found between the provided datasets")
  }

  # Check if we can connect all datasets
  if (!igraph::is_connected(g)) {
    unconnected_sets <- igraph::components(g)$membership
    groups <- split(dataset_names, unconnected_sets)

    stop("Cannot connect all datasets with the provided relations. Detected disconnected groups: ",
         paste(sapply(groups, function(group) paste("(", paste(group, collapse = ", "), ")", sep = "")),
               collapse = " and "))
  }

  # Find shortest paths between all datasets
  if (length(dataset_names) > 2) {
    # Find the optimal path using the minimum spanning tree (MST)
    mst <- igraph::mst(g)
  } else {
    # For just two datasets, use the original graph
    mst <- g
  }

  # If we only want the visualization, return that instead
  if (visualize) {
    # Create visualization using existing rio_visualize_structure function
    # with only the connections that are part of our MST
    used_relations <- list()
    for (e_id in 1:igraph::ecount(mst)) {
      rel_key <- igraph::E(mst)$relation_key[e_id]
      if (!is.null(rel_key) && rel_key %in% names(relations)) {
        used_relations[[rel_key]] <- relations[[rel_key]]
      }
    }

    # Return visualization
    return(rio_visualize_structure(
      datasets = dataset_names,
      structure = list(
        datasets = tibbles,
        relationships = used_relations
      ),
      interactive = TRUE,
      highlight_datasets = dataset_names
    ))
  }

  # Start with the first dataset
  current_dataset <- dataset_names[1]
  result <- tibbles[[current_dataset]]
  visited <- c(current_dataset)

  if (verbose) {
    cli::cli_alert_info("Starting with dataset '{current_dataset}'")
  }

  # Process each dataset
  while (length(visited) < length(dataset_names)) {
    # Find an edge that connects a visited node with an unvisited one
    next_dataset <- NULL
    from_dataset <- NULL
    edge_id <- NULL

    for (e_id in 1:igraph::ecount(mst)) {
      edge <- igraph::E(mst)[[e_id]]
      nodes <- igraph::ends(mst, edge)
      node1 <- dataset_names[nodes[1, 1]]
      node2 <- dataset_names[nodes[1, 2]]

      if (node1 %in% visited && !(node2 %in% visited)) {
        next_dataset <- node2
        from_dataset <- node1
        edge_id <- e_id
        break
      } else if (node2 %in% visited && !(node1 %in% visited)) {
        next_dataset <- node1
        from_dataset <- node2
        edge_id <- e_id
        break
      }
    }

    # Get the relation for this edge
    rel_key <- igraph::E(mst)$relation_key[edge_id]
    rel <- relations[[rel_key]]

    # Convert 'by' to a string for display
    if (is.null(names(rel$by))) {
      by_str <- paste(rel$by, collapse = ", ")
    } else {
      by_parts <- character(length(rel$by))
      for (i in seq_along(rel$by)) {
        if (names(rel$by)[i] == "") {
          by_parts[i] <- rel$by[i]
        } else {
          by_parts[i] <- paste0(names(rel$by)[i], " = ", rel$by[i])
        }
      }
      by_str <- paste(by_parts, collapse = ", ")
    }

    if (verbose) {
      cli::cli_alert_info("Joining '{next_dataset}' to '{from_dataset}' using: {by_str}")
    }

    # Determine which fields to use based on the direction
    by_fields <- rel$by
    if (rel$from != from_dataset) {
      # If the relation is defined in the opposite direction of our join,
      # we need to swap the names and values for named fields
      if (!is.null(names(by_fields)) && any(names(by_fields) != "")) {
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

    # Perform the join
    result <- dplyr::left_join(result, tibbles[[next_dataset]], by = by_fields)

    if (verbose) {
      cli::cli_alert_success("Successfully joined '{next_dataset}', now at {nrow(result)} rows")
    }

    # Mark as visited
    visited <- c(visited, next_dataset)
  }

  # Apply reference date filtering if specified
  if (!is.null(reference_date)) {
    if (verbose) {
      cli::cli_alert_info("Filtering by reference date: {reference_date}")
    }

    result <- rio_filter_by_reference_date(result, reference_date)
  }

  if (verbose) {
    cli::cli_alert_success("All datasets successfully joined. Final result has {nrow(result)} rows and {ncol(result)} columns")
  }

  return(result)
}
