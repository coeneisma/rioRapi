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
