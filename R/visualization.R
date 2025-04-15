#' Create a map of educational locations
#'
#' This function creates a leaflet map of educational locations from RIO data.
#'
#' @param locations A tibble containing educational location data from the RIO API.
#' @param popup_vars Character vector of variable names to include in popups. Default is NULL.
#' @param color The color of the markers. Default is "blue".
#' @param radius The radius of the markers. Default is 5.
#'
#' @return A leaflet map object.
#'
#' @examples
#' \dontrun{
#' locations <- rio_get_locations(city = "Rotterdam")
#' map <- rio_map_locations(
#'   locations,
#'   popup_vars = c("ONDERWIJSLOCATIENAAM", "PLAATSNAAM")
#' )
#' }
#'
#' @importFrom sf st_as_sf
#' @importFrom leaflet leaflet addTiles addCircleMarkers
#' @export
rio_map_locations <- function(locations, popup_vars = NULL, color = "blue", radius = 5) {
  # Check if required packages are available
  if (!requireNamespace("sf", quietly = TRUE) || !requireNamespace("leaflet", quietly = TRUE)) {
    stop("Packages 'sf' and 'leaflet' must be installed to use this function")
  }

  # Check for required longitude and latitude columns
  if (!all(c("GPS_LONGITUDE", "GPS_LATITUDE") %in% names(locations))) {
    stop("The 'locations' data must include GPS_LONGITUDE and GPS_LATITUDE columns")
  }

  # Remove rows with missing coordinates
  valid_locations <- dplyr::filter(
    locations,
    !is.na(GPS_LONGITUDE) & !is.na(GPS_LATITUDE)
  )

  # Convert to sf object
  locations_sf <- sf::st_as_sf(
    valid_locations,
    coords = c("GPS_LONGITUDE", "GPS_LATITUDE"),
    crs = 4326
  )

  # Create popup content if variables are specified
  if (!is.null(popup_vars)) {
    # Check if all popup_vars exist in the data
    missing_vars <- popup_vars[!popup_vars %in% names(valid_locations)]
    if (length(missing_vars) > 0) {
      warning("The following popup variables were not found in the data: ",
              paste(missing_vars, collapse = ", "))
      popup_vars <- popup_vars[popup_vars %in% names(valid_locations)]
    }

    # Create popup content
    if (length(popup_vars) > 0) {
      popup_content <- apply(valid_locations[, popup_vars, drop = FALSE], 1, function(row) {
        paste0(
          "<div style='max-width: 200px;'>",
          paste0("<b>", popup_vars, ":</b> ", row, collapse = "<br>"),
          "</div>"
        )
      })
    } else {
      popup_content <- NULL
    }
  } else {
    popup_content <- NULL
  }

  # Create the map
  map <- leaflet::leaflet(locations_sf) |>
    leaflet::addTiles() |>
    leaflet::addCircleMarkers(
      radius = radius,
      color = color,
      stroke = FALSE,
      fillOpacity = 0.8,
      popup = popup_content
    )

  return(map)
}

#' Plot timeline of institutions or entities
#'
#' This function creates a timeline plot showing the validity periods of institutions or other entities.
#'
#' @param data A tibble containing data with start and end dates.
#' @param id_col The name of the column containing entity IDs.
#' @param name_col The name of the column containing entity names. Default is NULL.
#' @param start_date_col The name of the column containing start dates. Default is "BEGINDATUM".
#' @param end_date_col The name of the column containing end dates. Default is "EINDDATUM".
#'
#' @return A ggplot object showing the timeline.
#'
#' @examples
#' \dontrun{
#' institutions <- rio_get_data(dataset_id = "institution-resource-id")
#' plot <- rio_timeline(
#'   institutions,
#'   id_col = "INSTELLINGSCODE",
#'   name_col = "INSTELLINGSNAAM"
#' )
#' }
#'
#' @export
rio_visualize_timeline <- function(data, id_col, name_col = NULL,
                         start_date_col = "BEGINDATUM", end_date_col = "EINDDATUM") {
  # Check if required packages are available
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("Package 'ggplot2' must be installed to use this function")
  }

  # Ensure date columns are Date objects
  if (!inherits(data[[start_date_col]], "Date")) {
    data[[start_date_col]] <- purrr::map_vec(data[[start_date_col]], rio_parse_date)
  }

  if (end_date_col %in% names(data) && !inherits(data[[end_date_col]], "Date")) {
    data[[end_date_col]] <- purrr::map_vec(data[[end_date_col]], rio_parse_date)
  }

  # Replace NA end dates with current date
  if (end_date_col %in% names(data)) {
    data[[end_date_col]] <- dplyr::if_else(is.na(data[[end_date_col]]), Sys.Date(), data[[end_date_col]])
  } else {
    # If end date column doesn't exist, use current date
    data[[end_date_col]] <- Sys.Date()
  }

  # Create label column
  if (is.null(name_col)) {
    data$label <- data[[id_col]]
  } else {
    data$label <- paste0(data[[id_col]], ": ", data[[name_col]])
  }

  # Create the plot
  plot <- ggplot2::ggplot(data, ggplot2::aes(x = .data[[start_date_col]],
                                             xend = .data[[end_date_col]],
                                             y = .data$label,
                                             yend = .data$label)) +
    ggplot2::geom_segment(size = 4, alpha = 0.7) +
    ggplot2::labs(
      title = "Timeline",
      x = "Date",
      y = ""
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.title = ggplot2::element_text(hjust = 0.5),
      axis.text.y = ggplot2::element_text(size = 8)
    )

  return(plot)
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
rio_visualize_relations <- function(..., by_names = TRUE, highlight_tables = NULL,
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





#' Visualize relationships between RIO datasets
#'
#' This function creates a visualization of the relationships between RIO datasets
#' based on the YAML-defined structure. It can show connections between specific datasets,
#' including indirect connections through intermediary datasets.
#'
#' @param datasets Optional character vector of dataset names to visualize.
#'        If NULL, all datasets in the structure will be visualized.
#' @param relations Optional relation definitions. If NULL, relations are loaded from the YAML file.
#' @param min_confidence Minimum confidence level for relationships.
#'        Can be "high" or "medium". Default is "medium".
#' @param interactive Logical indicating whether to create an interactive visualization
#'        (requires visNetwork package). Default is TRUE.
#' @param highlight_datasets Character vector of dataset names to highlight in the visualization.
#' @param find_paths Logical indicating whether to find connection paths between datasets
#'        that are not directly connected. Default is TRUE.
#' @param max_path_length Maximum length of connection paths to consider when find_paths is TRUE.
#'        Default is 3.
#' @param ... Optional named tibbles to visualize connections between (alternative method).
#'
#' @return A visualization object showing the relationships between datasets.
#'
#' @keywords internal
rio_visualize_structure <- function(datasets = NULL, relations = NULL, min_confidence = "medium",
                                    interactive = TRUE, highlight_datasets = NULL,
                                    find_paths = TRUE, max_path_length = 3, ...) {
  # Get the input tibbles if provided via ...
  tibbles <- list(...)

  # If tibbles are provided, use them for connection detection
  if (length(tibbles) > 0) {
    # Detect connections between the provided tibbles using the defined relations
    # instead of auto-detecting them
    if (is.null(relations)) {
      relations_data <- rio_load_relations()
      relations <- relations_data$relations
    } else if (!is.null(relations$relations)) {
      # If complete structure is provided, extract just the relations
      relations <- relations$relations
    }

    # Find connections between the provided tibbles based on known relations
    connections <- rio_find_connections(names(tibbles), relations,
                                        min_confidence = min_confidence)

    # Visualize the detected connections
    return(rio_visualize_connections(
      connections,
      min_confidence = min_confidence,
      interactive = interactive
    ))
  } else {
    # Load relations if not provided
    if (is.null(relations)) {
      relations_data <- rio_load_relations()
      relations <- relations_data$relations
    } else if (!is.null(relations$relations)) {
      # If complete structure is provided, extract just the relations
      relations <- relations$relations
    }

    # Determine which datasets to visualize
    if (is.null(datasets)) {
      # If no datasets specified, use all datasets mentioned in the relations
      all_datasets <- unique(c(
        sapply(relations, function(rel) rel$from),
        sapply(relations, function(rel) rel$to)
      ))
      dataset_names <- all_datasets
    } else {
      dataset_names <- datasets
    }

    # Create a graph representation of the structure for finding paths
    full_graph <- igraph::graph_from_data_frame(d = data.frame(source = character(0),
                                                               target = character(0)),
                                                directed = FALSE)

    # Add all datasets as vertices to the full graph
    all_dataset_names <- unique(c(
      sapply(relations, function(rel) rel$from),
      sapply(relations, function(rel) rel$to)
    ))

    for (dataset in all_dataset_names) {
      full_graph <- igraph::add_vertices(full_graph, 1, name = dataset)
    }

    # Add all edges to the full graph (we'll use this for path finding)
    for (rel_name in names(relations)) {
      rel <- relations[[rel_name]]

      # Skip relations with insufficient confidence
      if ((min_confidence == "high" && rel$confidence != "high")) {
        next
      }

      # Add the edge
      full_graph <- igraph::add_edges(full_graph, c(rel$from, rel$to))

      # Store relationship details
      edge_id <- igraph::ecount(full_graph)
      igraph::E(full_graph)$relationship_name[edge_id] <- rel_name
    }

    # Create the visualization graph
    g <- igraph::graph_from_data_frame(d = data.frame(source = character(0),
                                                      target = character(0)),
                                       directed = FALSE)

    # If we need to find paths between datasets
    datasets_to_include <- dataset_names
    connection_paths <- list()

    if (find_paths && length(dataset_names) > 1 && length(dataset_names) < length(all_dataset_names)) {
      # Find all paths between each pair of datasets
      for (i in 1:(length(dataset_names) - 1)) {
        for (j in (i + 1):length(dataset_names)) {
          from_dataset <- dataset_names[i]
          to_dataset <- dataset_names[j]

          # Check if there's a direct connection
          direct_connection <- FALSE
          for (rel_name in names(relations)) {
            rel <- relations[[rel_name]]
            if ((rel$from == from_dataset && rel$to == to_dataset) ||
                (rel$from == to_dataset && rel$to == from_dataset)) {
              direct_connection <- TRUE
              break
            }
          }

          # If no direct connection, find a path
          if (!direct_connection) {
            # Find all simple paths between these datasets
            # Set cutoff to max_path_length if specified, otherwise no limit
            cutoff_value <- if (!is.null(max_path_length)) max_path_length else -1

            paths <- igraph::all_simple_paths(
              full_graph,
              from = from_dataset,
              to = to_dataset,
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

              # Add all datasets in the path to datasets_to_include
              path_datasets <- igraph::V(full_graph)$name[shortest_path]
              datasets_to_include <- unique(c(datasets_to_include, path_datasets))

              # Store path information
              connection_paths[[length(connection_paths) + 1]] <- list(
                from = from_dataset,
                to = to_dataset,
                path = path_datasets
              )
            } else {
              warning("No connection path found between ", from_dataset, " and ", to_dataset,
                      " with maximum path length ", max_path_length)
            }
          }
        }
      }
    }

    # Add all relevant datasets as vertices
    for (dataset in datasets_to_include) {
      g <- igraph::add_vertices(g, 1, name = dataset)
    }

    # Add highlight property to vertices
    igraph::V(g)$highlight <- igraph::V(g)$name %in% highlight_datasets

    # Add special property for datasets that were in the original selection
    igraph::V(g)$original <- igraph::V(g)$name %in% dataset_names

    # Add edges for direct connections and connection paths
    edge_labels <- character(0)
    edge_colors <- character(0)
    edge_widths <- numeric(0)
    edge_types <- character(0)  # To track if an edge is part of a path

    # Add direct connections
    for (rel_name in names(relations)) {
      rel <- relations[[rel_name]]

      # Only add edges between datasets we're including
      if (rel$from %in% datasets_to_include && rel$to %in% datasets_to_include) {

        # Skip relations with insufficient confidence
        if ((min_confidence == "high" && rel$confidence != "high")) {
          next
        }

        # Add an edge
        g <- igraph::add_edges(g, c(rel$from, rel$to))

        # Get the joining field(s) for the edge label
        if (!is.null(rel$by)) {
          if (is.character(rel$by) && !is.null(names(rel$by)) && any(names(rel$by) != "")) {
            # Named fields (different names in source and target)
            label_parts <- character(0)
            for (i in seq_along(rel$by)) {
              if (names(rel$by)[i] == "") {
                label_parts <- c(label_parts, rel$by[i])
              } else {
                label_parts <- c(label_parts, paste(names(rel$by)[i], "=", rel$by[i]))
              }
            }
            join_fields <- paste(label_parts, collapse = "\n")
          } else {
            # Unnamed fields (same name in both datasets)
            join_fields <- paste(rel$by, collapse = "\n")
          }
        } else {
          join_fields <- "Unknown connection"
        }

        edge_id <- igraph::ecount(g)
        edge_labels[edge_id] <- join_fields

        # Assign color and width based on confidence
        edge_colors[edge_id] <- ifelse(rel$confidence == "high", "green", "orange")
        edge_widths[edge_id] <- ifelse(rel$confidence == "high", 3, 2)

        # Direct connection
        edge_types[edge_id] <- "direct"
      }
    }

    # Set edge attributes if there are any edges
    if (igraph::ecount(g) > 0) {
      igraph::E(g)$label <- edge_labels
      igraph::E(g)$color <- edge_colors
      igraph::E(g)$width <- edge_widths
      igraph::E(g)$type <- edge_types
    } else {
      warning("No connections found between the specified datasets with the given confidence level")
    }

    # Set vertex attributes
    igraph::V(g)$label <- igraph::V(g)$name
    igraph::V(g)$color <- ifelse(igraph::V(g)$highlight, "yellow",
                                 ifelse(igraph::V(g)$original, "lightblue", "lightgrey"))

    # Create visualization
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
          color = igraph::V(g)$color,
          borderWidth = ifelse(igraph::V(g)$original, 3, 1),
          font = list(color = "black"),
          shape = "dot",
          size = 20,  # Larger nodes
          stringsAsFactors = FALSE
        )

        if (igraph::ecount(g) > 0) {
          edges <- data.frame(
            from = igraph::get.edgelist(g)[, 1],
            to = igraph::get.edgelist(g)[, 2],
            label = igraph::E(g)$label,
            color = igraph::E(g)$color,
            width = igraph::E(g)$width,
            title = igraph::E(g)$label,  # Tooltip
            font = list(color = "red", size = 12),  # Red text for connection fields
            length = 250,  # Longer edges for more space
            arrows = "to",
            smooth = TRUE,
            stringsAsFactors = FALSE
          )
        } else {
          edges <- data.frame(
            from = character(0),
            to = character(0),
            label = character(0),
            color = character(0),
            width = numeric(0),
            title = character(0),
            stringsAsFactors = FALSE
          )
        }

        # Create the network with better layout
        net <- visNetwork::visNetwork(nodes, edges) |>
          visNetwork::visOptions(highlightNearest = TRUE, selectedBy = "label") |>
          visNetwork::visEdges(font = list(color = "red", size = 12)) |>
          visNetwork::visNodes(font = list(size = 14)) |>
          visNetwork::visLayout(randomSeed = 123) |>  # For consistency
          visNetwork::visPhysics(solver = "forceAtlas2Based",
                                 forceAtlas2Based = list(gravitationalConstant = -100,
                                                         springLength = 200,  # More space between nodes
                                                         springConstant = 0.05),
                                 stabilization = list(iterations = 150))

        # Apply igraph layout if possible
        if (length(nodes$id) > 1) {
          net <- visNetwork::visIgraphLayout(net, layout = "layout_nicely",
                                             physics = FALSE,  # Turn off physics after layout
                                             randomSeed = 123)
        }

        return(net)
      }
    }

    if (!interactive) {
      # Create static visualization with igraph
      if (igraph::ecount(g) > 0) {
        plot(g,
             layout = igraph::layout_with_fr(g),
             vertex.size = 20,
             vertex.label.color = "black",
             vertex.label.cex = 0.8,
             edge.label.cex = 0.7,
             edge.curved = 0.2,
             main = "RIO Datasets Connection Network")

        # Add legend
        legend("bottomright",
               legend = c("High Confidence", "Medium Confidence",
                          "Selected Dataset", "Intermediate Dataset", "Highlighted Dataset"),
               col = c("green", "orange", "lightblue", "lightgrey", "yellow"),
               pch = c(NA, NA, 16, 16, 16),
               lwd = c(3, 2, NA, NA, NA),
               cex = 0.8)
      } else {
        plot.new()
        title(main = "RIO Datasets Connection Network")
        text(0.5, 0.5, "No connections found with the specified confidence level")
      }
    }
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
