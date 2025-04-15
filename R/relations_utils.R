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

#' Visualize relationships between RIO tables
#'
#' This function creates a visualization of the relationships between RIO tables
#' based on the relations defined in the package.
#'
#' @param ... Character vector of table names to visualize.
#' @param by_names Logical indicating whether the arguments are table names
#'        rather than actual tibbles. Default is TRUE.
#' @param highlight_tables Character vector of table names to highlight in the visualization.
#' @param min_confidence Minimum confidence level for relationships.
#'        Can be "high" or "medium". Default is "medium".
#' @param interactive Logical indicating whether to create an interactive visualization
#'        (requires visNetwork package). Default is TRUE.
#' @param as_table Logical indicating whether to return relations as a table.
#'        Default is FALSE.
#' @param find_paths Logical indicating whether to find connection paths between tables
#'        that are not directly connected. Default is TRUE.
#' @param max_path_length Maximum length of connection paths to consider when find_paths is TRUE.
#'        Default is NULL (no limit).
#' @param quiet Logical indicating whether to suppress progress messages.
#'        Default is FALSE.
#'
#' @return A visualization object showing the relationships between tables.
#'
#' @examples
#' \dontrun{
#' # Visualize the relationship between two specific tables
#' rio_visualize("onderwijslocaties", "vestigingserkenningen")
#'
#' # Visualize all relationships (no arguments)
#' rio_visualize()
#'
#' # Highlight specific tables
#' rio_visualize("onderwijslocaties", "vestigingserkenningen",
#'              highlight_tables = "onderwijslocaties")
#' }
#'
#' @export
rio_visualize <- function(..., by_names = TRUE, highlight_tables = NULL,
                          min_confidence = "medium", interactive = TRUE,
                          as_table = FALSE, find_paths = TRUE,
                          max_path_length = NULL, quiet = FALSE) {
  # Load relations
  relations <- rio_load_relations()

  # If requested as table, return the formatted list of relations
  if (as_table) {
    return(rio_list_relations(as_dataframe = TRUE, relations = relations))
  }

  # Process inputs to handle both named tibbles and table names
  input_args <- list(...)

  # If no arguments, visualize all relations
  if (length(input_args) == 0) {
    # Use the rio_visualize_structure function directly with no specific tables
    result <- rio_visualize_structure(
      datasets = NULL,
      relations = relations$relations,
      min_confidence = min_confidence,
      interactive = interactive,
      highlight_datasets = highlight_tables,
      find_paths = find_paths,
      max_path_length = max_path_length
    )

    return(result)
  } else {
    # We have specific tables to visualize
    if (by_names) {
      # Input is table names
      tables <- unlist(input_args)
    } else {
      # Input is tibbles, extract their names
      tables <- names(input_args)
    }

    # For specific tables, use the common helper function
    path_result <- rio_find_table_paths(
      tables = tables,
      relations = relations$relations,
      min_confidence = min_confidence,
      find_paths = find_paths,
      max_path_length = max_path_length,
      quiet = quiet
    )

    # Use the rio_visualize_structure function with the found tables
    result <- rio_visualize_structure(
      datasets = path_result$tables,
      relations = relations$relations,
      min_confidence = min_confidence,
      interactive = interactive,
      highlight_datasets = highlight_tables,
      find_paths = find_paths,
      max_path_length = max_path_length
    )

    return(result)
  }
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


#' Visualize connections between RIO datasets
#'
#' This function creates a visualization of connections between RIO datasets.
#'
#' @param connections List of connections between datasets
#' @param min_confidence Minimum confidence level for relationships to display
#' @param interactive Logical indicating whether to create an interactive visualization
#'
#' @return A visualization object (igraph plot or visNetwork object)
#'
#' @keywords internal
rio_visualize_connections <- function(connections, min_confidence = "medium", interactive = TRUE) {
  # Create an empty graph
  g <- igraph::make_empty_graph(n = 0, directed = FALSE)

  if (length(connections) == 0) {
    cli::cli_alert_warning("No connections to visualize")
    return(g)
  }

  # Get all unique dataset names
  all_datasets <- unique(unlist(lapply(connections, function(conn) {
    c(conn$from, conn$to)
  })))

  # Add all datasets as vertices
  for (dataset in all_datasets) {
    g <- igraph::add_vertices(g, 1, name = dataset)
  }

  # Add edges for connections
  edge_labels <- character(0)
  edge_colors <- character(0)
  edge_widths <- numeric(0)

  for (conn in connections) {
    # Skip relations with insufficient confidence
    if (min_confidence == "high" && conn$confidence != "high") {
      next
    }

    # Add an edge
    g <- igraph::add_edges(g, c(conn$from, conn$to))

    # Get the joining field(s) for the edge label
    if (!is.null(conn$by)) {
      if (is.character(conn$by) && !is.null(names(conn$by)) && any(names(conn$by) != "")) {
        # Named fields (different names in source and target)
        label_parts <- character(0)
        for (i in seq_along(conn$by)) {
          if (names(conn$by)[i] == "") {
            label_parts <- c(label_parts, conn$by[i])
          } else {
            label_parts <- c(label_parts, paste(names(conn$by)[i], "=", conn$by[i]))
          }
        }
        join_fields <- paste(label_parts, collapse = "\n")
      } else {
        # Unnamed fields (same name in both datasets)
        join_fields <- paste(conn$by, collapse = "\n")
      }
    } else {
      join_fields <- "Unknown connection"
    }

    edge_id <- igraph::ecount(g)
    edge_labels[edge_id] <- join_fields

    # Assign color and width based on confidence
    edge_colors[edge_id] <- ifelse(conn$confidence == "high", "green", "orange")
    edge_widths[edge_id] <- ifelse(conn$confidence == "high", 3, 2)
  }

  # Set edge attributes
  if (igraph::ecount(g) > 0) {
    igraph::E(g)$label <- edge_labels
    igraph::E(g)$color <- edge_colors
    igraph::E(g)$width <- edge_widths
  }

  # Set vertex attributes
  igraph::V(g)$label <- igraph::V(g)$name
  igraph::V(g)$color <- "lightblue"

  # Create visualization
  if (interactive) {
    # Create interactive visualization with visNetwork
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
          title = igraph::E(g)$label,  # Tooltip
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
         main = "RIO Datasets Connection Network")

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

#' Join RIO tables based on predefined relations
#'
#' This function joins multiple RIO tables based on the relations defined in the package.
#' It automatically detects relations between tables and chooses the optimal join path.
#' When 'onderwijslocaties' is included, the result can be converted to an sf object.
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
#'        Default is NULL (no limit).
#' @param as_sf Logical indicating whether to convert the result to an sf object when 'onderwijslocaties'
#'        is included. Default is FALSE.
#' @param remove_invalid Logical indicating whether to remove rows with invalid or missing coordinates
#'        when converting to sf. Default is FALSE.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return A joined tibble, or an sf object if as_sf = TRUE and 'onderwijslocaties' is included.
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
#' # Return a nested tibble
#' nested_data <- rio_join("vestigingserkenningen", "onderwijslocaties", nested = TRUE)
#' }
#'
#' @export
rio_join <- function(..., by_names = TRUE, reference_date = Sys.Date(),
                     nested = FALSE, find_paths = TRUE, max_path_length = 10,
                     as_sf = FALSE, remove_invalid = FALSE, quiet = FALSE) {
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
      tibble_result <- tryCatch({
        # Apply reference_date when loading data (if provided)
        if (!is.null(reference_date)) {
          rio_get_data(table_name = name, quiet = quiet)
        } else {
          rio_get_data(table_name = name, quiet = quiet)
        }
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

  # Convert to sf object if requested and 'onderwijslocaties' is included
  if (as_sf && "onderwijslocaties" %in% table_names) {
    if (!requireNamespace("sf", quietly = TRUE)) {
      warning("Cannot convert to sf: sf package is not installed")
    } else {
      # Check if required columns exist
      if (!all(c("GPS_LONGITUDE", "GPS_LATITUDE") %in% names(result))) {
        if (!quiet) {
          warning("Cannot convert to sf: GPS_LONGITUDE and/or GPS_LATITUDE columns not found")
        }
      } else {
        # If remove_invalid is TRUE, remove rows with missing or invalid coordinates
        if (remove_invalid) {
          valid_rows <- !is.na(result$GPS_LONGITUDE) &
            !is.na(result$GPS_LATITUDE) &
            is.numeric(result$GPS_LONGITUDE) &
            is.numeric(result$GPS_LATITUDE)

          if (sum(!valid_rows) > 0 && !quiet) {
            cli::cli_alert_info("Removing {sum(!valid_rows)} rows with invalid or missing coordinates")
          }

          result <- result[valid_rows, ]

          # If all rows were invalid, return original result
          if (nrow(result) == 0) {
            if (!quiet) {
              cli::cli_alert_warning("No valid coordinates found, returning non-sf result")
            }
            return(result)
          }
        }

        # Convert to sf object
        tryCatch({
          result_sf <- sf::st_as_sf(
            result,
            coords = c("GPS_LONGITUDE", "GPS_LATITUDE"),
            crs = 4326
          )

          if (!quiet) {
            cli::cli_alert_success("Converted to sf object with {nrow(result_sf)} points")
          }

          result <- result_sf
        }, error = function(e) {
          if (!quiet) {
            cli::cli_alert_danger("Error converting to sf: {e$message}")
          }
        })
      }
    }
  }

  if (!quiet) {
    cli::cli_alert_success("All tables successfully joined. Final result has {nrow(result)} rows and {ncol(result)} columns")
  }

  return(result)
}



#' Join RIO tables based on predefined relations
#'
#' This function joins multiple RIO tables based on the relations defined in the package.
#' It automatically detects relations between tables and chooses the optimal join path.
#' When 'onderwijslocaties' is included, the result can be converted to an sf object.
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
#'        Default is NULL (no limit).
#' @param as_sf Logical indicating whether to convert the result to an sf object when 'onderwijslocaties'
#'        is included. Default is FALSE.
#' @param remove_invalid Logical indicating whether to remove rows with invalid or missing coordinates
#'        when converting to sf. Default is FALSE.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return A joined tibble, or an sf object if as_sf = TRUE and 'onderwijslocaties' is included.
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
#' # Return a nested tibble
#' nested_data <- rio_join("vestigingserkenningen", "onderwijslocaties", nested = TRUE)
#' }
#'
#' @export
rio_join <- function(..., by_names = TRUE, reference_date = Sys.Date(),
                     nested = FALSE, find_paths = TRUE, max_path_length = 10,
                     as_sf = FALSE, remove_invalid = FALSE, quiet = FALSE) {
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
      tibble_result <- tryCatch({
        # Apply reference_date when loading data (if provided)
        if (!is.null(reference_date)) {
          rio_get_data(table_name = name, quiet = quiet)
        } else {
          rio_get_data(table_name = name, quiet = quiet)
        }
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

#' Join RIO tables based on predefined relations, including indirect paths
#'
#' This function joins multiple RIO tables based on the relations defined in the package.
#' It automatically detects relations between tables, including indirect relations via intermediate tables.
#'
#' @param ... Named tibbles to join or character vector of table names to load.
#' @param by_names Logical indicating whether the arguments are table names
#'        rather than actual datasets. Default is FALSE.
#' @param reference_date Optional date to filter valid records. Default is NULL (no filtering).
#' @param nested Logical indicating whether to return a nested tibble. Default is FALSE.
#' @param find_paths Logical indicating whether to find indirect paths between tables.
#'        Default is TRUE.
#' @param max_path_length Maximum length of connection paths to consider when find_paths is TRUE.
#'        Default is 3.
#' @param verbose Logical indicating whether to display progress messages. Default is FALSE.
#'
#' @return A joined tibble with data from all input tibbles.
#'
#' @export
rio_join_with_paths <- function(..., by_names = FALSE, reference_date = NULL,
                                nested = FALSE, find_paths = TRUE,
                                max_path_length = 3, verbose = FALSE) {

  # Laad relaties
  relations <- rio_load_relations()

  # Verwerk invoerparameters
  input_args <- list(...)

  if (by_names) {
    # Input zijn tabelnamen, laad de data
    table_names <- unlist(input_args)
    if (!verbose) {
      message("Tabellen laden: ", paste(table_names, collapse = ", "))
    }

    # Als we indirecte paden willen vinden
    if (find_paths && length(table_names) > 1) {
      # Zoek alle tabellen op
      datasets <- rio_list_datasets()
      all_tables <- datasets$name

      # Maak een grafiek van alle relaties
      g <- igraph::make_empty_graph(directed = FALSE)

      # Voeg alle tabellen toe als knopen
      for (table in all_tables) {
        g <- igraph::add_vertices(g, 1, name = table)
      }

      # Voeg alle relaties toe als verbindingen
      for (rel_name in names(relations$relations)) {
        rel <- relations$relations[[rel_name]]
        from_idx <- which(igraph::V(g)$name == rel$from)
        to_idx <- which(igraph::V(g)$name == rel$to)

        if (length(from_idx) > 0 && length(to_idx) > 0) {
          g <- igraph::add_edges(g, c(from_idx, to_idx))
        }
      }

      # Zoek het kortste pad tussen alle paren van tabellen
      all_tables_to_join <- table_names

      for (i in 1:(length(table_names)-1)) {
        for (j in (i+1):length(table_names)) {
          from_table <- table_names[i]
          to_table <- table_names[j]

          # Controleer of er een direct pad is
          direct_relation_exists <- FALSE
          for (rel_name in names(relations$relations)) {
            rel <- relations$relations[[rel_name]]
            if ((rel$from == from_table && rel$to == to_table) ||
                (rel$from == to_table && rel$to == from_table)) {
              direct_relation_exists <- TRUE
              break
            }
          }

          # Als er geen directe relatie is, zoek een pad
          if (!direct_relation_exists) {
            from_idx <- which(igraph::V(g)$name == from_table)
            to_idx <- which(igraph::V(g)$name == to_table)

            if (length(from_idx) > 0 && length(to_idx) > 0) {
              # Zoek alle paden tot max_path_length
              paths <- igraph::all_simple_paths(g,
                                                from = from_idx,
                                                to = to_idx,
                                                mode = "all",
                                                cutoff = max_path_length)

              if (length(paths) > 0) {
                # Vind het kortste pad
                shortest_path <- paths[[1]]
                shortest_length <- length(shortest_path)

                for (p in 2:length(paths)) {
                  if (length(paths[[p]]) < shortest_length) {
                    shortest_path <- paths[[p]]
                    shortest_length <- length(paths[[p]])
                  }
                }

                # Voeg alle tabellen in het pad toe aan de lijst
                path_tables <- igraph::V(g)$name[shortest_path]
                all_tables_to_join <- unique(c(all_tables_to_join, path_tables))

                if (verbose) {
                  message("Pad gevonden tussen ", from_table, " en ", to_table,
                          ": ", paste(path_tables, collapse = " -> "))
                }
              } else if (verbose) {
                message("Geen pad gevonden tussen ", from_table, " en ", to_table,
                        " binnen max_path_length ", max_path_length)
              }
            }
          }
        }
      }

      # Update de lijst met tabellen om te laden
      table_names <- all_tables_to_join

      if (verbose) {
        message("Tabellen om te joinen (inclusief tussentabellen): ",
                paste(table_names, collapse = ", "))
      }
    }

    # Laad alle benodigde tabellen
    tibbles <- list()
    for (name in table_names) {
      if (verbose) {
        message("Tabel laden: ", name)
      }
      tibbles[[name]] <- rio_get_data(table_name = name, quiet = !verbose)
    }
  } else {
    # Input zijn reeds geladen tibbles
    tibbles <- input_args
    table_names <- names(tibbles)
  }

  # Controleer of er genoeg tabellen zijn om te joinen
  if (length(tibbles) < 2) {
    stop("Tenminste twee tabellen zijn nodig om te joinen")
  }

  # Controleer op onbenoemde parameters
  if (!by_names && "" %in% table_names) {
    stop("Bij het direct meegeven van tibbles moeten alle argumenten benoemd zijn. Bijvoorbeeld: rio_join_with_paths(tabel1 = df1, tabel2 = df2)")
  }

  if (verbose) {
    message("Joinen van ", length(table_names), " tabellen: ", paste(table_names, collapse = ", "))
  }

  # Zoek het optimale pad om de tabellen te verbinden
  path_info <- find_optimal_path(table_names, relations$relations, verbose = verbose)

  # Voer joins uit op basis van het optimale pad
  result <- execute_dataset_joins(tibbles, path_info, relations$relations, verbose = verbose, reference_date = reference_date)

  # Formatteer als geneste tibble indien gewenst
  if (nested) {
    result <- nest_result(result, table_names, path_info)
  }

  if (verbose) {
    message("Alle tabellen succesvol gejoind. Resultaat heeft ", nrow(result), " rijen en ", ncol(result), " kolommen")
  }

  return(result)
}


# # Detecteer de structuur
# structure <- rio_detect_structure()
#
# # Extraheer relaties uit de structuur
# relations <- list()
# for (rel_name in names(structure$relationships)) {
#   detected_rel <- structure$relationships[[rel_name]]
#
#   # Maak een relatie-object
#   relation <- rio_create_relation(
#     from = detected_rel$source,
#     to = detected_rel$target,
#     type = "one-to-many",  # aanpassen indien nodig
#     from_field = detected_rel$exact_matches[1],  # eerste match gebruiken
#     to_field = detected_rel$exact_matches[1]
#   )
#
#   # Voeg toe aan relaties
#   relations <- rio_add_relation(relations, relation)
# }
#
# # Bewerk relaties
# relations <- rio_remove_relation(relations, "tabel1", "tabel2")
# new_relation <- rio_create_relation("tabel1", "tabel2", "one-to-many", "ID1", "ID2")
# relations <- rio_add_relation(relations, new_relation)
#
# # Exporteer naar CSV voor bewerking
# rio_export_relations_csv(relations, "rio_relations.csv")
#
# # Later: importeer bijgewerkte relaties
# relations <- rio_import_relations_csv("rio_relations_edited.csv")
#
# # Sla op als YAML
# rio_save_relations_yaml(relations)
#
# # Gebruik de relaties bij het combineren van datasets
# data1 <- rio_get_data(dataset_name = "tabel1")
# data2 <- rio_get_data(dataset_name = "tabel2")
# combined <- rio_combine_with_relations(relations, tabel1 = data1, tabel2 = data2)



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



#' List relationships between RIO tables
#'
#' This function provides an overview of all defined relationships between RIO tables.
#'
#' @param from Optional filter to show only relationships from a specific table.
#' @param to Optional filter to show only relationships to a specific table.
#' @param pattern Optional pattern to filter relationship descriptions.
#' @param include_metadata Logical indicating whether to include relationship metadata.
#'        Default is FALSE.
#'
#' @return A dataframe with relationship information.
#'
#' @examples
#' \dontrun{
#' # List all table relationships
#' all_relations <- rio_list_relations()
#'
#' # Filter relationships from the "onderwijslocaties" table
#' loc_relations <- rio_list_relations(from = "onderwijslocaties")
#'
#' # Filter relationships between two specific tables
#' specific_relations <- rio_list_relations(
#'   from = "onderwijslocaties",
#'   to = "vestigingserkenningen"
#' )
#' }
#'
#' @export
rio_list_relations <- function(from = NULL, to = NULL, pattern = NULL, include_metadata = FALSE) {
  # Load relations
  relations <- rio_load_relations()

  # Convert to dataframe
  relations_df <- rio_list_relations_internal(relations, as_dataframe = TRUE)

  # Apply filters
  if (!is.null(from)) {
    relations_df <- relations_df[relations_df$from == from, ]
  }

  if (!is.null(to)) {
    relations_df <- relations_df[relations_df$to == to, ]
  }

  if (!is.null(pattern)) {
    pattern_matches <- grepl(pattern, relations_df$description, ignore.case = TRUE) |
      grepl(pattern, relations_df$from, ignore.case = TRUE) |
      grepl(pattern, relations_df$to, ignore.case = TRUE)
    relations_df <- relations_df[pattern_matches, ]
  }

  # Remove metadata column if not requested
  if (!include_metadata && "metadata" %in% names(relations_df)) {
    relations_df$metadata <- NULL
  }

  if (!include_metadata && "has_metadata" %in% names(relations_df)) {
    relations_df$has_metadata <- NULL
  }

  return(relations_df)
}

# The internal function (renamed from the current rio_list_relations)
rio_list_relations_internal <- function(relations, as_dataframe = TRUE) {
  # Check if relations structure exists and contains relations
  if (is.null(relations$relations) || length(relations$relations) == 0) {
    if (as_dataframe) {
      return(data.frame(
        relation_key = character(0),
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

    for (rel_name in names(relations$relations)) {
      rel <- relations$relations[[rel_name]]

      # Convert 'by' to a string representation
      if (is.character(rel$by) && !is.null(names(rel$by)) && any(names(rel$by) != "")) {
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
      } else {
        # Unnamed vector or list
        by_str <- paste(rel$by, collapse = ", ")
      }

      # Extract metadata if any
      metadata_str <- NA
      if (!is.null(rel$metadata)) {
        if (requireNamespace("jsonlite", quietly = TRUE)) {
          metadata_str <- jsonlite::toJSON(rel$metadata, auto_unbox = TRUE)
        } else {
          metadata_str <- "Metadata present (install jsonlite package to view)"
        }
      }

      relations_list[[length(relations_list) + 1]] <- data.frame(
        relation_key = rel_name,
        from = rel$from,
        to = rel$to,
        type = ifelse(is.null(rel$type), "one-to-many", rel$type),
        by = by_str,
        confidence = ifelse(is.null(rel$confidence), "medium", rel$confidence),
        description = ifelse(is.null(rel$description), NA, rel$description),
        has_metadata = !is.null(rel$metadata),
        metadata = metadata_str,
        stringsAsFactors = FALSE
      )
    }

    if (length(relations_list) > 0) {
      relations_df <- do.call(rbind, relations_list)
      return(relations_df)
    } else {
      return(data.frame())
    }
  } else {
    # Return just the relations part of the structure
    return(relations$relations)
  }
}
#' Export default relation definitions to a user-specific version
#'
#' This function exports the default relation definitions from the package to
#' a user-specific location for customization. This allows users to modify the
#' RIO relation definitions without changing the package files.
#'
#' @param overwrite Logical value indicating whether to overwrite existing files.
#'        Default is FALSE for safety.
#' @param destination Path where to save the file. Default is NULL, which will use
#'        the standard user data directory as returned by get_rio_data_dir().
#' @param quiet Logical value indicating whether to suppress informational messages.
#'        Default is FALSE.
#'
#' @return Path to the exported file (invisibly).
#'
#' @details
#' The function copies the default relations YAML file from the package to the user's
#' data directory, allowing for customization without modifying the package itself.
#' Once exported, the user can edit the YAML file to add, modify, or remove relations.
#'
#' By default, the rio_load_relations() function will automatically detect and use the
#' user-specific version if it exists, providing a seamless way to override the default
#' relations.
#'
#' @examples
#' \dontrun{
#' # Export default relations for customization
#' rio_export_default_relations()
#'
#' # Export and overwrite existing user definitions
#' rio_export_default_relations(overwrite = TRUE)
#'
#' # Export to a specific location
#' rio_export_default_relations(destination = "~/my_project/rio_relations.yaml")
#' }
#'
#' @seealso
#' \code{\link{rio_load_relations}} for loading relation definitions.
#' \code{\link{get_rio_data_dir}} for information about the user data directory.
#'
#' @export
rio_export_default_relations <- function(overwrite = FALSE, destination = NULL,
                                         quiet = FALSE) {
  # Determine the destination path
  if (is.null(destination)) {
    # Path to the user directory
    user_dir <- get_rio_data_dir()
    user_file <- file.path(user_dir, "rio_relations.yaml")
  } else {
    user_file <- destination

    # Create directory if it doesn't exist
    dest_dir <- dirname(destination)
    if (!dir.exists(dest_dir)) {
      dir.create(dest_dir, recursive = TRUE)
    }
  }

  # Check if the file already exists
  if (file.exists(user_file) && !overwrite) {
    stop("User-specific version already exists. Use overwrite = TRUE to overwrite.")
  }

  # Path to default file
  default_file <- system.file("extdata", "rio_relations.yaml", package = "rioRapi")

  # Check if the default file exists
  if (default_file == "") {
    stop("Default relations file not found in the package.")
  }

  # Copy the file
  copy_success <- file.copy(default_file, user_file, overwrite = overwrite)

  if (!copy_success) {
    stop("Failed to copy the relations file. Check write permissions.")
  }

  if (!quiet) {
    message("Default relation definitions exported to: ", user_file)
  }

  return(invisible(user_file))
}


#' Load RIO relations from a YAML file
#'
#' This function loads the RIO relations between tables from a YAML file.
#' If the file doesn't exist, an empty relation object is returned.
#'
#' @param file_path Path to the YAML file. Default is the package's built-in relations file.
#'
#' @return A list with the RIO relations structure.
#'
#' @export
rio_load_relations <- function(file_path = system.file("extdata", "rio_relations.yaml", package = "rioRapi")) {
  # Check if the file exists
  if (!file.exists(file_path)) {
    message("No relations file found at ", file_path, ". An empty relation object will be returned.")
    return(list(metadata = list(version = "1.0.0", created = Sys.Date()), relations = list()))
  }

  # Check if yaml package is installed
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required to load the relations")
  }

  # Load YAML file
  yaml_data <- yaml::read_yaml(file_path)

  # Add current time if loading from a different file than the default
  if (file_path != system.file("extdata", "rio_relations.yaml", package = "rioRapi")) {
    yaml_data$metadata$last_loaded <- Sys.time()
  }

  return(yaml_data)
}

#' Save RIO relations to a YAML file
#'
#' This function saves the RIO relations structure to a YAML file.
#'
#' @param relations The RIO relations structure.
#' @param file_path Path to the YAML file where the relations will be saved.
#' @param create_dir Logical indicating whether to create the directory if it doesn't exist.
#'        Default is TRUE.
#'
#' @return Invisibly the name of the file where the relations were saved.
#'
#' @export
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

  # Update metadata
  if (is.null(relations$metadata)) {
    relations$metadata <- list()
  }

  relations$metadata$last_modified <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  # Save relations as YAML
  yaml::write_yaml(relations, file_path)

  message("RIO relations saved to: ", file_path)

  return(invisible(file_path))
}

#' Find connections between RIO datasets based on predefined relations
#'
#' This function identifies connections between RIO datasets using the relations
#' defined in the YAML configuration file.
#'
#' @param dataset_names Character vector with names of datasets to find connections for
#' @param relations List of relation definitions from the YAML configuration
#' @param min_confidence Minimum confidence level for relationships ("high" or "medium")
#' @param verbose Logical indicating whether to display progress messages. Default is FALSE.
#'
#' @return A list of connections between the specified datasets
#'
#' @keywords internal
rio_find_connections <- function(dataset_names, relations, min_confidence = "medium", verbose = FALSE) {
  if (verbose) {
    cli::cli_alert_info("Finding connections between {length(dataset_names)} datasets")
  }

  # Initialize empty list to store connections
  connections <- list()

  # Create all possible pairs of datasets
  dataset_pairs <- utils::combn(dataset_names, 2, simplify = FALSE)

  # For each pair, find direct relations
  for (pair in dataset_pairs) {
    from_dataset <- pair[1]
    to_dataset <- pair[2]

    if (verbose) {
      cli::cli_alert_info("Checking connection: {from_dataset} -> {to_dataset}")
    }

    # Look for direct relations between these datasets
    for (rel_name in names(relations)) {
      rel <- relations[[rel_name]]

      # Skip relations with insufficient confidence
      if (min_confidence == "high" && rel$confidence != "high") {
        next
      }

      # Check if this relation connects our datasets (in either direction)
      if ((rel$from == from_dataset && rel$to == to_dataset) ||
          (rel$from == to_dataset && rel$to == from_dataset)) {

        # Add this relation to our list of connections
        connections[[length(connections) + 1]] <- list(
          from = rel$from,
          to = rel$to,
          relationship = rel_name,
          by = rel$by,
          confidence = rel$confidence,
          description = ifelse(is.null(rel$description), NA, rel$description)
        )

        if (verbose) {
          cli::cli_alert_success("Found relation between '{rel$from}' and '{rel$to}'")
        }
      }
    }
  }

  if (length(connections) == 0 && verbose) {
    cli::cli_alert_warning("No direct connections found between the specified datasets")
  }

  return(connections)
}

#' Find paths between tables based on predefined relations
#'
#' This helper function identifies paths between tables using the relations
#' defined in the YAML configuration. It is used by both rio_visualize() and rio_join()
#' to ensure consistent behavior.
#'
#' @param tables Character vector of table names to find paths between
#' @param relations List of relation definitions
#' @param min_confidence Minimum confidence level for relationships ("high" or "medium")
#' @param find_paths Logical indicating whether to look for indirect paths between tables
#' @param max_path_length Maximum path length to consider when find_paths is TRUE.
#'        Default is NULL (no limit).
#' @param quiet Logical indicating whether to suppress progress messages
#'
#' @return A list containing:
#'  - tables: Character vector of all tables to include (original + intermediate)
#'  - paths: List of connection paths between tables
#'
#' @keywords internal
rio_find_table_paths <- function(tables, relations, min_confidence = "medium",
                                 find_paths = TRUE, max_path_length = NULL, quiet = FALSE) {
  # Make sure we have a valid list of relations
  if (is.list(relations) && !is.null(relations$relations)) {
    # If complete structure is provided, extract just the relations
    relations <- relations$relations
  }

  # Ensure tables is a character vector
  tables <- as.character(tables)

  # Get all unique tables mentioned in relations
  all_relation_tables <- unique(c(
    sapply(relations, function(rel) rel$from),
    sapply(relations, function(rel) rel$to)
  ))

  # Check if specified tables exist in relations, attempt to correct names if needed
  corrected_tables <- character(length(tables))
  for (i in seq_along(tables)) {
    if (tables[i] %in% all_relation_tables) {
      # Exact match
      corrected_tables[i] <- tables[i]
    } else {
      # Try case-insensitive match
      lower_input <- tolower(tables[i])
      lower_relation_tables <- tolower(all_relation_tables)
      match_idx <- which(lower_relation_tables == lower_input)

      if (length(match_idx) > 0) {
        corrected_tables[i] <- all_relation_tables[match_idx[1]]
        if (!quiet) {
          message("Table name '", tables[i], "' corrected to '", corrected_tables[i], "'")
        }
      } else {
        # No match found, keep original
        corrected_tables[i] <- tables[i]
        if (!quiet) {
          message("Warning: Table '", tables[i], "' not found in relations")
        }
      }
    }
  }
  tables <- corrected_tables

  # Start with the original tables
  result_tables <- tables

  # If we need to find paths between tables
  if (find_paths && length(tables) > 1) {
    if (!quiet) {
      message("Looking for paths between tables...")
    }

    # Create a graph representation for finding paths
    g <- igraph::graph_from_data_frame(d = data.frame(source = character(0),
                                                      target = character(0)),
                                       directed = FALSE)

    # Add all tables from relations as vertices
    for (table in all_relation_tables) {
      g <- igraph::add_vertices(g, 1, name = table)
    }

    # Add all edges based on relations
    for (rel_name in names(relations)) {
      rel <- relations[[rel_name]]

      # Skip relations with insufficient confidence
      if ((min_confidence == "high" && rel$confidence != "high")) {
        next
      }

      # Skip invalid relations
      if (is.null(rel$from) || is.null(rel$to) ||
          !(rel$from %in% all_relation_tables) || !(rel$to %in% all_relation_tables)) {
        next
      }

      # Add the edge
      from_idx <- which(igraph::V(g)$name == rel$from)
      to_idx <- which(igraph::V(g)$name == rel$to)

      if (length(from_idx) > 0 && length(to_idx) > 0) {
        g <- igraph::add_edges(g, c(from_idx, to_idx))

        # Store relationship details
        edge_id <- igraph::ecount(g)
        igraph::E(g)$relationship_name[edge_id] <- rel_name
        igraph::E(g)$weight[edge_id] <- ifelse(rel$confidence == "high", 1, 2)
      }
    }

    # Find paths between each pair of input tables
    connection_paths <- list()

    for (i in 1:(length(tables) - 1)) {
      for (j in (i + 1):length(tables)) {
        from_table <- tables[i]
        to_table <- tables[j]

        # Check if there's a direct connection
        direct_connection <- FALSE
        for (rel_name in names(relations)) {
          rel <- relations[[rel_name]]
          if ((rel$from == from_table && rel$to == to_table) ||
              (rel$from == to_table && rel$to == from_table)) {
            direct_connection <- TRUE
            break
          }
        }

        # If no direct connection, find a path
        if (!direct_connection) {
          # First check if both tables exist in the graph
          from_idx <- which(igraph::V(g)$name == from_table)
          to_idx <- which(igraph::V(g)$name == to_table)

          if (length(from_idx) > 0 && length(to_idx) > 0) {
            # Find all simple paths between these tables, with appropriate cutoff
            try({
              # Set cutoff to max_path_length if specified, otherwise no limit
              cutoff_value <- if (!is.null(max_path_length)) max_path_length else -1

              paths <- igraph::all_simple_paths(
                g,
                from = from_idx,
                to = to_idx,
                mode = "all",
                cutoff = cutoff_value
              )

              if (length(paths) > 0) {
                # Get the shortest path
                shortest_path <- paths[[1]]
                for (p in paths) {
                  if (length(p) < length(shortest_path)) {
                    shortest_path <- p
                  }
                }

                # Add all tables in the path to the result
                path_tables <- igraph::V(g)$name[shortest_path]
                result_tables <- unique(c(result_tables, path_tables))

                # Store path information
                connection_paths[[length(connection_paths) + 1]] <- list(
                  from = from_table,
                  to = to_table,
                  path = path_tables
                )

                if (!quiet) {
                  message("Found path between '", from_table, "' and '", to_table,
                          "': ", paste(path_tables, collapse = " -> "))
                }
              } else if (!quiet) {
                message("No path found between '", from_table, "' and '", to_table, "'")
              }
            }, silent = TRUE)
          } else if (!quiet) {
            missing_tables <- c()
            if (length(from_idx) == 0) missing_tables <- c(missing_tables, from_table)
            if (length(to_idx) == 0) missing_tables <- c(missing_tables, to_table)

            message("Table(s) not found in relations: ", paste(missing_tables, collapse = ", "))
          }
        }
      }
    }
  }

  return(list(
    tables = result_tables,
    paths = connection_paths
  ))
}
