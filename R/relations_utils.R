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
#' Find the optimal path to connect datasets
#'
#' This function finds the optimal path to connect multiple datasets based on
#' predefined relations stored in YAML. It uses graph theory to determine the best
#' possible connections between datasets.
#'
#' @param dataset_names Character vector with names of datasets to connect
#' @param relations List of relation definitions from the YAML configuration
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
#' determined by find_optimal_path(), using relations defined in YAML.
#'
#' @param tibbles List of tibbles to join
#' @param path_info Path information as returned by find_optimal_path()
#' @param relations List of relation definitions from the YAML configuration
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

    # Get the relationship information - this needs to be adapted for the YAML structure
    rel_key <- NULL

    # Find the relation in the relations list that matches these datasets
    for (key in names(relations)) {
      rel <- relations[[key]]
      if ((rel$from == from_dataset && rel$to == next_dataset) ||
          (rel$from == next_dataset && rel$to == from_dataset)) {
        rel_key <- key
        break
      }
    }

    if (is.null(rel_key)) {
      warning("Could not find relation definition for ", from_dataset, " -> ", next_dataset)
      next
    }

    rel <- relations[[rel_key]]

    # Format joining fields for display
    by_fields <- rel$by

    # Check if by_fields is properly structured
    if (is.null(by_fields)) {
      warning("No joining fields defined for relation between ", from_dataset, " and ", next_dataset)
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
      cli::cli_alert_info("Joining '{next_dataset}' to result using: {by_str}")
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

rio_visualize <- function(tables = NULL, highlight_tables = NULL,
                          min_confidence = "medium", interactive = TRUE,
                          as_table = FALSE, quiet = FALSE,
                          max_path_length = 3) {  # Toegevoegde parameter
  # Load relations
  relations <- rio_load_relations()

  # If requested as table, return the formatted list of relations
  if (as_table) {
    return(rio_list_relations(as_dataframe = TRUE, relations = relations))
  }

  # Create visualization
  result <- rio_visualize_structure(
    datasets = tables,
    relations = relations$relations,
    min_confidence = min_confidence,
    interactive = interactive,
    highlight_datasets = highlight_tables,
    find_paths = TRUE,
    max_path_length = max_path_length  # Doorgeven van de parameter
  )

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

#' Join RIO tables based on predefined relations
#'
#' This function joins multiple RIO tables based on the relations defined in the package.
#' It automatically detects relations between tables and chooses the optimal join path.
#'
#' @param ... Named tibbles to join or character vector of table names to load.
#' @param by_names Logical indicating whether the arguments are table names
#'        rather than actual datasets. Default is FALSE.
#' @param reference_date Optional date to filter valid records. Default is NULL (no filtering).
#' @param nested Logical indicating whether to return a nested tibble. Default is FALSE.
#' @param visualize Logical indicating whether to return a visualization instead of joined data.
#'        Default is FALSE.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return A joined tibble, or a visualization object if visualize = TRUE.
#'
#' @examples
#' \dontrun{
#' # Join tables by providing the actual tibbles
#' vestigingen <- rio_get_data("vestigingserkenningen")
#' locaties <- rio_get_data("onderwijslocaties")
#' joined_data <- rio_join(vestigingen = vestigingen, locaties = locaties)
#'
#' # Join tables by name (easiest method)
#' joined_data <- rio_join("vestigingserkenningen", "onderwijslocaties", by_names = TRUE)
#'
#' # Return a nested tibble
#' nested_data <- rio_join("vestigingserkenningen", "onderwijslocaties",
#'                         by_names = TRUE, nested = TRUE)
#'
#' # Visualize the relations between tables
#' viz <- rio_join("vestigingserkenningen", "onderwijslocaties",
#'                 by_names = TRUE, visualize = TRUE)
#' }
#'
#' @export
rio_join <- function(..., by_names = FALSE, reference_date = NULL,
                     nested = FALSE, visualize = FALSE, quiet = FALSE) {
  # Load relations from YAML file
  relations <- rio_load_relations()

  # Process inputs to handle both named tibbles and table names
  input_args <- list(...)

  if (by_names) {
    # Input is table names, load the data
    table_names <- unlist(input_args)
    if (!quiet) {
      cli::cli_alert_info("Loading {length(table_names)} tables")
    }

    tibbles <- list()
    for (name in table_names) {
      if (!quiet) {
        cli::cli_alert_info("Loading table: {name}")
      }
      tibbles[[name]] <- rio_get_data(table_name = name)
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

  # If visualization is requested, return visualization
  if (visualize) {
    return(rio_visualize(tables = table_names, interactive = TRUE))
  }

  # Find the optimal path to connect the tables
  path_info <- find_optimal_path(table_names, relations$relations, verbose = !quiet)

  # Execute the joins based on the optimal path
  result <- execute_dataset_joins(tibbles, path_info, relations$relations,
                                  verbose = !quiet, reference_date = reference_date)

  # Format as nested tibble if requested
  if (nested) {
    result <- nest_result(result, table_names, path_info)
  }

  if (!quiet) {
    cli::cli_alert_success("All tables successfully joined. Final result has {nrow(result)} rows and {ncol(result)} columns")
  }

  return(result)
}


#' Join tables using predefined relations from YAML configuration
#'
#' This function joins multiple tables based on predefined relations stored in the
#' RIO relations YAML file. It finds the optimal path between tables and performs
#' the joins accordingly.
#'
#' @param ... Named tibbles to join or character vector of table names to load.
#' @param by_names Logical indicating whether the arguments are table names
#'        rather than actual table. Default is FALSE.
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
#' joined_data <- rio_join_tables(vestigingen = vestigingen, locaties = locaties)
#'
#' # Or provide dataset names (will load data automatically)
#' joined_data <- rio_join_tables("vestigingserkenningen",
#'                                  "onderwijslocaties",
#'                                  by_names = TRUE)
#'
#' # Create a nested result
#' nested_data <- rio_join_tables("vestigingserkenningen",
#'                                  "onderwijslocaties",
#'                                  by_names = TRUE,
#'                                  nested = TRUE)
#'
#' # Visualize the connection path
#' viz <- rio_join_tables("vestigingserkenningen",
#'                          "onderwijslocaties",
#'                          by_names = TRUE,
#'                          visualize = TRUE)
#' }
#'
#' @keywords internal
rio_join_tables <- function(..., by_names = FALSE, relations = NULL,
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
    rel_data <- rio_load_relations()
    relations <- rel_data$relations
    if (length(relations) == 0) {
      stop("No relations defined in the YAML configuration. Please define relations first.")
    }
  } else if (is.list(relations) && !is.null(relations$relations)) {
    # If complete relations structure is provided, extract just the relations part
    relations <- relations$relations
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
