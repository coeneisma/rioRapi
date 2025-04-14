

#' Visualize relationships between RIO datasets
#'
#' This function creates a visualization of the relationships between RIO datasets
#' based on the detected structure. It can show connections between specific datasets,
#' including indirect connections through intermediary datasets.
#'
#' @param datasets Optional character vector of dataset names to visualize.
#'        If NULL, all datasets in the structure will be visualized.
#' @param structure Optional RIO structure as returned by rio_load_structure().
#'        If NULL, the structure will be loaded automatically.
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
#' @examples
#' \dontrun{
#' # Visualize all relationships from the detected structure
#' rio_visualize_structure()
#'
#' # Visualize only the relationship between two specific datasets
#' rio_visualize_structure(datasets = c("onderwijslocaties", "vestigingserkenningen"))
#'
#' # Find indirect connection paths between datasets that aren't directly connected
#' rio_visualize_structure(
#'   datasets = c("onderwijslocaties", "onderwijsinstellingserkenningen"),
#'   find_paths = TRUE
#' )
#'
#' # Specify additional parameters
#' rio_visualize_structure(
#'   datasets = c("onderwijslocaties", "vestigingserkenningen", "onderwijsinstellingserkenningen"),
#'   highlight_datasets = c("onderwijslocaties"),
#'   min_confidence = "high"
#' )
#'
#' # Alternative method with loaded datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' rio_visualize_structure(
#'   locations = locations,
#'   institutions = institutions
#' )
#' }
#'
#' @export
rio_visualize_structure <- function(datasets = NULL, structure = NULL, min_confidence = "medium",
                                    interactive = TRUE, highlight_datasets = NULL,
                                    find_paths = TRUE, max_path_length = 3, ...) {
  # Get the input tibbles if provided via ...
  tibbles <- list(...)

  # If tibbles are provided, use them for connection detection
  if (length(tibbles) > 0) {
    # Detect connections between the provided tibbles
    connections <- rio_auto_connect(..., auto_join = FALSE, verbose = FALSE)

    # Visualize the detected connections
    return(rio_visualize_connections(
      connections,
      min_confidence = min_confidence,
      interactive = interactive
    ))
  } else {
    # Load structure if not provided
    if (is.null(structure)) {
      structure <- rio_load_structure(auto_detect = TRUE)
    }

    # Determine which datasets to visualize
    if (is.null(datasets)) {
      # If no datasets specified, use all datasets in the structure
      dataset_names <- names(structure$datasets)
    } else {
      # Check if specified datasets exist in the structure
      all_datasets <- names(structure$datasets)
      missing_datasets <- datasets[!datasets %in% all_datasets]

      if (length(missing_datasets) > 0) {
        warning("The following datasets were not found in the RIO structure: ",
                paste(missing_datasets, collapse = ", "))
        # Use only the valid datasets
        dataset_names <- datasets[datasets %in% all_datasets]
      } else {
        dataset_names <- datasets
      }

      if (length(dataset_names) == 0) {
        stop("No valid datasets specified for visualization")
      }
    }

    # Create a graph representation of the structure for finding paths
    full_graph <- igraph::graph_from_data_frame(d = data.frame(source = character(0),
                                                               target = character(0)),
                                                directed = FALSE)

    # Add all datasets as vertices to the full graph
    all_dataset_names <- names(structure$datasets)
    for (dataset in all_dataset_names) {
      full_graph <- igraph::add_vertices(full_graph, 1, name = dataset)
    }

    # Add all edges to the full graph (we'll use this for path finding)
    for (rel_name in names(structure$relationships)) {
      rel <- structure$relationships[[rel_name]]

      # Skip relations with insufficient confidence
      if ((min_confidence == "high" && rel$confidence != "high")) {
        next
      }

      # Add the edge
      full_graph <- igraph::add_edges(full_graph, c(rel$source, rel$target))

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
          for (rel_name in names(structure$relationships)) {
            rel <- structure$relationships[[rel_name]]
            if ((rel$source == from_dataset && rel$target == to_dataset) ||
                (rel$source == to_dataset && rel$target == from_dataset)) {
              direct_connection <- TRUE
              break
            }
          }

          # If no direct connection, find a path
          if (!direct_connection) {
            # Find all simple paths between these datasets
            paths <- igraph::all_simple_paths(
              full_graph,
              from = from_dataset,
              to = to_dataset,
              mode = "all",
              cutoff = max_path_length
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
    for (rel_name in names(structure$relationships)) {
      rel <- structure$relationships[[rel_name]]

      # Only add edges between datasets we're including
      if (rel$source %in% datasets_to_include && rel$target %in% datasets_to_include) {

        # Skip relations with insufficient confidence
        if ((min_confidence == "high" && rel$confidence != "high")) {
          next
        }

        # Add an edge
        g <- igraph::add_edges(g, c(rel$source, rel$target))

        # Get the joining field(s) for the edge label
        if (length(rel$exact_matches) > 0) {
          join_fields <- rel$exact_matches
        } else if (length(rel$relation_matches) > 0) {
          join_fields <- sapply(rel$relation_matches, function(x) x$rel_field)
        } else if (length(rel$similar_matches) > 0) {
          join_fields <- sapply(rel$similar_matches, function(x) paste(x$source_field, "=", x$target_field))
        } else {
          join_fields <- "Unknown connection"
        }

        edge_id <- igraph::ecount(g)
        edge_labels[edge_id] <- paste(join_fields, collapse = "\n")

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
        net <- visNetwork::visNetwork(nodes, edges) %>%
          visNetwork::visOptions(highlightNearest = TRUE, selectedBy = "label") %>%
          visNetwork::visEdges(font = list(color = "red", size = 12)) %>%
          visNetwork::visNodes(font = list(size = 14)) %>%
          visNetwork::visLayout(randomSeed = 123) %>%  # For consistency
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




#' Vind datumvelden in een dataframe
#'
#' @param df Een dataframe
#'
#' @return Een character vector met namen van datumvelden
#'
#' @keywords internal
find_date_fields <- function(df) {
  date_field_patterns <- c(
    "^BEGINDATUM", "^EINDDATUM", "^STARTDATUM", "^OPHEFFINGSDATUM",
    "^INGANGSDATUM", "^INBEDRIJFDATUM", "^UITBEDRIJFDATUM",
    "_PERIODE$"
  )

  # Find columns matching the patterns
  date_fields <- character(0)
  for (pattern in date_field_patterns) {
    matching_cols <- grep(pattern, names(df), value = TRUE)
    date_fields <- c(date_fields, matching_cols)
  }

  return(unique(date_fields))
}

#' Verkrijg de directory voor het opslaan van RIO package data
#'
#' @return Een character string met het pad naar de RIO data directory
#'
#' @keywords internal
get_rio_data_dir <- function() {
  # Use rappdirs if available, otherwise use a subdirectory in the user's home directory
  if (requireNamespace("rappdirs", quietly = TRUE)) {
    dir <- rappdirs::user_data_dir("rioRapi", "R")
  } else {
    dir <- file.path(Sys.getenv("HOME"), ".rioRapi")
  }

  # Create the directory if it doesn't exist
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }

  return(dir)
}
