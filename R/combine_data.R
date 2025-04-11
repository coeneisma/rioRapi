#' Detecteer de structuur van RIO datasets en hun onderlinge relaties
#'
#' Deze functie analyseert alle beschikbare RIO datasets en hun velden om potentiële
#' relaties tussen datasets te identificeren. De resultaten worden opgeslagen voor later gebruik.
#'
#' @param force Logische waarde die aangeeft of een nieuwe detectie moet worden uitgevoerd,
#'        zelfs als er al een recente structuurdetectie bestaat. Standaard is FALSE.
#' @param days_threshold Aantal dagen waarna een structuurdetectie als verouderd wordt beschouwd.
#'        Standaard is 28 (4 weken).
#' @param save_dir Map waarin het resultaat wordt opgeslagen. Standaard is de data-map van het pakket.
#' @param quiet Logische waarde die aangeeft of voortgangsberichten onderdrukt moeten worden. Standaard is FALSE.
#'
#' @return Geeft een lijst terug met de gedetecteerde RIO structuur (invisible).
#'
#' @examples
#' \dontrun{
#' # Detecteer de RIO structuur en sla deze op
#' rio_detect_structure()
#'
#' # Forceer een nieuwe detectie, zelfs als er een recente bestaat
#' rio_detect_structure(force = TRUE)
#' }
#'
#' @export
rio_detect_structure <- function(force = FALSE, days_threshold = 28,
                                 save_dir = get_rio_data_dir(), quiet = FALSE) {

  # Get the path for the mapping file
  mapping_file <- file.path(save_dir, "rio_structure_mapping.rds")

  # Check if mapping already exists and is recent
  if (!force && file.exists(mapping_file)) {
    file_info <- file.info(mapping_file)
    file_age <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))

    if (file_age < days_threshold) {
      if (!quiet) {
        cli::cli_alert_info("Using existing RIO structure mapping (age: {round(file_age, 1)} days)")
      }
      return(invisible(readRDS(mapping_file)))
    } else if (!quiet) {
      cli::cli_alert_warning("Existing mapping is {round(file_age, 1)} days old (threshold: {days_threshold}). Regenerating...")
    }
  }

  if (!quiet) {
    cli::cli_alert_info("Detecting RIO structure. This may take several minutes...")
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

  # Initialize structure to store dataset information and relationships
  rio_structure <- list(
    datasets = list(),
    relationships = list(),
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
        rio_structure$datasets[[dataset_name]] <- list(
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
    cli::cli_alert_info("Analyzing potential relationships between datasets...")
  }

  # Define common joining field patterns
  common_id_patterns <- c(
    # Code patterns
    "CODE$", "CODES$",
    # ID patterns
    "ID$", "IDS$",
    # Special cases
    "^OIE_CODE$", "^BGE_CODE$", "^VESTIGINGSCODE$", "^ONDERWIJSLOCATIECODE$",
    "^ONDERWIJSAANBIEDERID$", "^ONDERWIJSBESTUURID$", "^ONDERWIJSAANBIEDERGROEPID$",
    "^ONDERWIJSBEGELEIDINGSAANBIEDERID$", "^OPLEIDINGSEENHEIDCODE$",
    "^ERKENDEOPLEIDINGSCODE$", "^AANGEBODEN_OPLEIDINGCODE$", "^UNIEKE_ERKENDEOPLEIDINGSCODE$"
  )

  # Analyze relationships between datasets
  relations <- list()
  dataset_names <- names(rio_structure$datasets)

  for (i in 1:(length(dataset_names) - 1)) {
    for (j in (i + 1):length(dataset_names)) {
      source_name <- dataset_names[i]
      target_name <- dataset_names[j]

      # Get fields for both datasets
      source_fields <- rio_structure$datasets[[source_name]]$fields$id
      target_fields <- rio_structure$datasets[[target_name]]$fields$id

      # Find potential joining fields
      source_id_fields <- source_fields[grepl(paste(common_id_patterns, collapse = "|"), source_fields)]
      target_id_fields <- target_fields[grepl(paste(common_id_patterns, collapse = "|"), target_fields)]

      # Find exact matches
      exact_matches <- intersect(source_id_fields, target_id_fields)

      # Find similar matches (e.g., same field name pattern)
      similar_matches <- list()

      for (src_field in source_id_fields) {
        for (tgt_field in target_id_fields) {
          # Check if fields have the same base part (after removing prefixes)
          src_base <- sub("^.*_", "", src_field)
          tgt_base <- sub("^.*_", "", tgt_field)

          if (src_base == tgt_base && length(src_base) >= 3) {
            similar_matches <- c(similar_matches, list(list(
              source_field = src_field,
              target_field = tgt_field,
              base = src_base
            )))
          }
        }
      }

      # Check for relational tables
      is_relation_table <- function(name) {
        grepl("^relaties_", name, ignore.case = TRUE)
      }

      relation_matches <- list()

      if (is_relation_table(source_name) || is_relation_table(target_name)) {
        # Determine which is the relation table
        rel_name <- if (is_relation_table(source_name)) source_name else target_name
        other_name <- if (is_relation_table(source_name)) target_name else source_name
        rel_fields <- if (is_relation_table(source_name)) source_fields else target_fields
        other_fields <- if (is_relation_table(source_name)) target_fields else source_fields

        # Get the potential linking fields by analyzing the relation table name
        rel_parts <- strsplit(rel_name, "_")[[1]]
        rel_parts <- rel_parts[rel_parts != "relaties"]

        for (part in rel_parts) {
          # Try to find a corresponding ID field in both tables
          potential_id_field <- paste0(toupper(part), "ID")
          potential_code_field <- paste0(toupper(part), "CODE")

          if (potential_id_field %in% rel_fields &&
              potential_id_field %in% other_fields) {
            relation_matches <- c(relation_matches, list(list(
              rel_field = potential_id_field,
              other_field = potential_id_field,
              type = "id"
            )))
          } else if (potential_code_field %in% rel_fields &&
                     potential_code_field %in% other_fields) {
            relation_matches <- c(relation_matches, list(list(
              rel_field = potential_code_field,
              other_field = potential_code_field,
              type = "code"
            )))
          }
        }
      }

      # Store relationship information if any matches were found
      if (length(exact_matches) > 0 || length(similar_matches) > 0 || length(relation_matches) > 0) {
        relation_key <- paste(source_name, "to", target_name)

        rio_structure$relationships[[relation_key]] <- list(
          source = source_name,
          target = target_name,
          exact_matches = exact_matches,
          similar_matches = similar_matches,
          relation_matches = relation_matches,
          confidence = if(length(exact_matches) > 0) "high" else if(length(relation_matches) > 0) "high" else "medium"
        )
      }
    }
  }

  # Save the mapping to a file
  saveRDS(rio_structure, mapping_file)

  if (!quiet) {
    relation_count <- length(rio_structure$relationships)
    cli::cli_alert_success("RIO structure detection completed: found {relation_count} potential relationships")
    cli::cli_alert_info("Structure saved to: {mapping_file}")
  }

  invisible(rio_structure)
}

#' Laad de gedetecteerde RIO structuur
#'
#' Deze functie laadt de opgeslagen RIO structuur. Als er geen structuur bestaat of deze
#' verouderd is, kan optioneel een nieuwe detectie worden uitgevoerd.
#'
#' @param auto_detect Logische waarde die aangeeft of automatisch een nieuwe structuurdetectie
#'        moet worden uitgevoerd als er geen bestaat of als deze verouderd is. Standaard is TRUE.
#' @param days_threshold Aantal dagen waarna een structuurdetectie als verouderd wordt beschouwd.
#'        Standaard is 28 (4 weken).
#' @param quiet Logische waarde die aangeeft of voortgangsberichten onderdrukt moeten worden. Standaard is FALSE.
#'
#' @return Een lijst met de gedetecteerde RIO structuur.
#'
#' @examples
#' \dontrun{
#' # Laad de RIO structuur
#' structure <- rio_load_structure()
#' }
#'
#' @keywords internal
rio_load_structure <- function(auto_detect = TRUE, days_threshold = 28, quiet = FALSE) {
  mapping_file <- file.path(get_rio_data_dir(), "rio_structure_mapping.rds")

  # Check if mapping exists
  if (!file.exists(mapping_file)) {
    if (auto_detect) {
      if (!quiet) {
        cli::cli_alert_info("No existing RIO structure found. Creating new structure...")
      }
      return(rio_detect_structure(quiet = quiet))
    } else {
      stop("No RIO structure found. Run rio_detect_structure() first.")
    }
  }

  # Check if mapping is recent
  file_info <- file.info(mapping_file)
  file_age <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))

  if (file_age >= days_threshold) {
    if (auto_detect) {
      if (!quiet) {
        cli::cli_alert_warning("Existing structure is {round(file_age, 1)} days old (threshold: {days_threshold}). Regenerating...")
      }
      return(rio_detect_structure(quiet = quiet))
    } else if (!quiet) {
      cli::cli_alert_warning("Existing structure is {round(file_age, 1)} days old (threshold: {days_threshold})")
    }
  } else if (!quiet) {
    cli::cli_alert_info("Using existing RIO structure (age: {round(file_age, 1)} days)")
  }

  # Load the mapping
  structure <- readRDS(mapping_file)
  return(structure)
}

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

#' Koppel meerdere RIO datasets intelligent
#'
#' Deze functie koppelt meerdere RIO datasets door automatisch de meest geschikte
#' verbindingen tussen de datasets te detecteren en te gebruiken. Het maakt gebruik van
#' een vooraf gedetecteerde structuur van de RIO database om optimale koppelingen te bepalen.
#'
#' @param ... Benoemde tibbles om te koppelen. Elk argument moet een tibble zijn met data uit RIO.
#' @param method De koppelingsmethode. Opties zijn:
#'        - "smart" (standaard): Gebruik de gedetecteerde structuur om optimale verbindingen te vinden
#'        - "auto": Detecteer automatisch verbindingen op basis van alleen de opgegeven datasets
#'        - "manual": Gebruik de expliciet gespecificeerde 'by' parameter voor koppeling
#' @param by Character vector met kolommen om op te koppelen (alleen gebruikt bij method = "manual").
#'        Als NULL en method = "manual", wordt geprobeerd te koppelen op alle kolommen met dezelfde naam.
#' @param reference_date Optionele datum om geldige records te filteren. Standaard is NULL (geen filtering).
#' @param max_distance Maximale verbindingsafstand bij gebruik van method = "smart".
#'        Standaard is 3.
#' @param create_structure Logische waarde die aangeeft of een nieuwe structuurdetectie moet worden uitgevoerd
#'        als er geen bestaat bij gebruik van method = "smart". Standaard is TRUE.
#' @param suffix Suffixen om toe te voegen aan niet-by kolomnamen om ze uniek te maken in de gekoppelde tabellen.
#' @param verbose Logische waarde die aangeeft of gedetailleerde verbindingsinformatie moet worden weergegeven.
#'        Standaard is TRUE.
#'
#' @return Een gekoppelde tibble met data uit alle input tibbles.
#'
#' @examples
#' \dontrun{
#' # Haal data op uit verschillende datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' # Smart koppeling op basis van gedetecteerde structuur
#' linked_data <- rio_link_datasets(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen
#' )
#'
#' # Automatisch detecteren van verbindingen zonder gebruik van gedetecteerde structuur
#' linked_data_auto <- rio_link_datasets(
#'   locations = locations,
#'   institutions = institutions,
#'   method = "auto"
#' )
#'
#' # Handmatige koppeling met gespecificeerde verbindingsvelden
#' linked_data_manual <- rio_link_datasets(
#'   locations = locations,
#'   vestigingen = vestigingen,
#'   method = "manual",
#'   by = c("VESTIGINGSCODE" = "ONDERWIJSLOCATIECODE")
#' )
#' }
#'
#' @export
rio_link_datasets <- function(..., method = "smart", by = NULL, reference_date = NULL,
                              max_distance = 3, create_structure = TRUE,
                              suffix = c("_x", "_y"), verbose = TRUE) {
  # Get the input tibbles
  tibbles <- list(...)

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for linking")
  }

  # Execute the appropriate linking method
  if (method == "smart") {
    # Check if we have a structure mapping file
    mapping_exists <- file.exists(file.path(get_rio_data_dir(), "rio_structure_mapping.rds"))

    if (!mapping_exists && !create_structure) {
      stop("No RIO structure found and create_structure = FALSE. ",
           "Run rio_detect_structure() first or set create_structure = TRUE.")
    }

    # Use smart combination based on the gedetecteerde structure
    return(rio_combine_smart(
      ...,
      auto_detect = create_structure,
      max_path_length = max_distance,
      reference_date = reference_date,
      verbose = verbose
    ))
  } else if (method == "auto") {
    # Use automatic detection based on the provided datasets only
    if (verbose) {
      message("Using automatic connection detection for the provided datasets")
    }

    return(rio_auto_connect(
      ...,
      auto_join = TRUE,
      reference_date = reference_date,
      verbose = verbose
    ))
  } else if (method == "manual") {
    # Use manual joining by specified fields
    if (verbose) {
      message("Using manual connection with specified 'by' parameter")
    }

    # Manual joining logic - implementation that replaces rio_combine()
    # Start with the first tibble
    result <- tibbles[[1]]
    dataset_names <- names(tibbles)

    # Join with each of the remaining tibbles
    for (i in 2:length(tibbles)) {
      if (verbose) {
        cli::cli_alert_info("Joining '{dataset_names[i]}' using manually specified fields")
      }
      result <- dplyr::left_join(result, tibbles[[i]], by = by, suffix = suffix)
    }

    # Apply reference date filtering if required
    if (!is.null(reference_date)) {
      if (verbose) {
        cli::cli_alert_info("Filtering by reference date: {reference_date}")
      }
      # Filter the result
      result <- rio_filter_by_reference_date(result, reference_date)
    }

    return(result)
  } else {
    stop("Invalid method: ", method, ". Use 'smart', 'auto', or 'manual'.")
  }
}

#' Intelligente koppeling van datasets op basis van gedetecteerde structuur
#'
#' @param ... Benoemde tibbles om te koppelen
#' @param auto_detect Logische waarde die aangeeft of automatisch een structuur moet worden gedetecteerd als er geen bestaat
#' @param max_path_length Maximale padlengte tussen datasets
#' @param reference_date Optionele datum om geldige records te filteren
#' @param verbose Logische waarde die aangeeft of gedetailleerde informatie moet worden weergegeven
#'
#' @return Een gekoppelde tibble
#'
#' @keywords internal
rio_combine_smart <- function(..., auto_detect = TRUE, max_path_length = 3,
                              reference_date = NULL, verbose = TRUE) {
  # Get the input tibbles
  tibbles <- list(...)

  # Load the structure
  structure <- rio_load_structure(auto_detect = auto_detect)

  # Get the names of the provided datasets
  dataset_names <- names(tibbles)

  # Find all possible paths between the datasets
  all_paths <- list()
  for (i in 1:(length(dataset_names) - 1)) {
    for (j in (i + 1):length(dataset_names)) {
      # Find paths between these two datasets
      paths <- rio_find_connection_paths(
        dataset_names[i], dataset_names[j],
        structure = structure,
        max_path_length = max_path_length
      )

      # Add to all paths
      all_paths <- c(all_paths, paths)
    }
  }

  # If no paths were found, return an error
  if (length(all_paths) == 0) {
    stop("Could not find connection paths between the specified datasets")
  }

  # Find the best path to connect all datasets
  best_path <- find_best_connection_path(all_paths)

  if (verbose) {
    cli::cli_alert_info("Using connection path: {paste(best_path$path, collapse = ' -> ')}")
  }

  # Execute the joins based on the best path
  result <- execute_dataset_joins(tibbles, best_path, verbose, reference_date)

  return(result)
}

#' Vind verbindingspaden tussen RIO datasets
#'
#' Deze functie vindt alle mogelijke verbindingspaden tussen opgegeven datasets
#' met behulp van de opgeslagen RIO structuur.
#'
#' @param ... Benoemde of onbenoemde character strings met datasetnamen om te verbinden.
#' @param structure De RIO structuur zoals geretourneerd door rio_load_structure().
#'        Als NULL, wordt deze automatisch geladen.
#' @param max_path_length Maximale lengte van verbindingspaden om te overwegen. Standaard is 3.
#' @param min_confidence Minimaal betrouwbaarheidsniveau voor verbindingen. Kan "high" of "medium" zijn.
#'        Standaard is "medium".
#'
#' @return Een lijst met mogelijke verbindingspaden tussen de datasets.
#'
#' @examples
#' \dontrun{
#' # Vind verbindingspaden tussen drie datasets
#' paths <- rio_find_connection_paths("onderwijslocaties", "vestigingserkenningen", "onderwijsaanbieders")
#' }
#'
#' @keywords internal
rio_find_connection_paths <- function(..., structure = NULL, max_path_length = 3, min_confidence = "medium") {
  # Load structure if not provided
  if (is.null(structure)) {
    structure <- rio_load_structure(auto_detect = TRUE)
  }

  # Get dataset names
  datasets <- c(...)

  # Check if all datasets exist in the structure
  all_datasets <- names(structure$datasets)
  missing_datasets <- datasets[!datasets %in% all_datasets]

  if (length(missing_datasets) > 0) {
    stop("The following datasets were not found in the RIO structure: ",
         paste(missing_datasets, collapse = ", "))
  }

  # Initialize results
  paths <- list()

  # Create a graph representation of the structure
  g <- igraph::make_graph(directed = FALSE)
  g <- igraph::add_vertices(g, length(all_datasets), name = all_datasets)

  # Add edges based on relationships with sufficient confidence
  for (rel_name in names(structure$relationships)) {
    rel <- structure$relationships[[rel_name]]

    # Skip relations with insufficient confidence
    if ((min_confidence == "high" && rel$confidence != "high")) {
      next
    }

    # Add edge
    g <- igraph::add_edges(g, c(rel$source, rel$target))

    # Store relationship details for edge
    edge_id <- igraph::ecount(g)
    igraph::E(g)$relationship[edge_id] <- rel_name
    igraph::E(g)$confidence[edge_id] <- rel$confidence
  }

  # Find connection paths between each pair of specified datasets
  if (length(datasets) > 1) {
    path_count <- 0

    for (i in 1:(length(datasets) - 1)) {
      for (j in (i + 1):length(datasets)) {
        from_dataset <- datasets[i]
        to_dataset <- datasets[j]

        # Find all simple paths between the two datasets
        all_paths <- igraph::all_simple_paths(
          g,
          from = from_dataset,
          to = to_dataset,
          mode = "all",
          cutoff = max_path_length
        )

        # Process and store paths with their connection details
        for (path_idx in seq_along(all_paths)) {
          path <- all_paths[[path_idx]]
          path_vertices <- igraph::V(g)$name[path]

          path_details <- list(
            path = path_vertices,
            connections = list()
          )

          # Get connection details for each step in the path
          for (step in 1:(length(path_vertices) - 1)) {
            from_vertex <- path_vertices[step]
            to_vertex <- path_vertices[step + 1]

            # Find the edge between these vertices
            edge_id <- igraph::get.edge.ids(g, c(from_vertex, to_vertex))
            rel_name <- igraph::E(g)$relationship[edge_id]

            # Get full relationship details
            connection <- structure$relationships[[rel_name]]

            path_details$connections[[step]] <- list(
              from = from_vertex,
              to = to_vertex,
              relationship = connection
            )
          }

          path_count <- path_count + 1
          paths[[paste0("path_", path_count)]] <- path_details
        }
      }
    }
  }

  # If no paths were found, return an empty list with a message
  if (length(paths) == 0) {
    message("No connection paths found between the specified datasets with the given constraints")
  }

  return(paths)
}

#' Visualiseer gedetecteerde verbindingen tussen datasets
#'
#' @param connections Een lijst met verbindingen zoals geretourneerd door rio_auto_connect()
#' @param min_confidence Minimaal betrouwbaarheidsniveau voor verbindingen
#' @param interactive Logische waarde die aangeeft of een interactieve visualisatie moet worden gemaakt
#'
#' @return Een visualisatie-object
#'
#' @keywords internal
rio_visualize_connections <- function(connections, min_confidence = "medium", interactive = TRUE) {
  # Create a graph
  g <- igraph::make_graph(directed = FALSE)

  # Extract unique dataset names from connections
  all_datasets <- unique(unlist(lapply(connections, function(conn) {
    c(conn$dataset1, conn$dataset2)
  })))

  # Add vertices
  g <- igraph::add_vertices(g, length(all_datasets), name = all_datasets)

  # Add edges based on connections
  edge_labels <- character(0)
  edge_colors <- character(0)
  edge_widths <- numeric(0)

  for (conn_name in names(connections)) {
    conn <- connections[[conn_name]]

    # Skip low confidence connections if min_confidence is higher
    if (min_confidence == "high" && conn$confidence != "high") {
      next
    }

    # Add edge
    g <- igraph::add_edges(g, c(conn$dataset1, conn$dataset2))

    # Create edge label
    if (is.character(conn$join_fields)) {
      label <- paste(conn$join_fields, collapse = "\n")
    } else if (length(conn$join_fields) > 0) {
      # For complex join fields (source and target field might be different)
      label_parts <- sapply(conn$join_fields, function(field) {
        if (is.list(field)) {
          paste(field$field1, "=", field$field2)
        } else {
          field
        }
      })
      label <- paste(label_parts, collapse = "\n")
    } else {
      label <- "Unknown"
    }

    edge_id <- igraph::ecount(g)
    edge_labels[edge_id] <- label

    # Set color and width based on confidence
    edge_colors[edge_id] <- ifelse(conn$confidence == "high", "green", "orange")
    edge_widths[edge_id] <- ifelse(conn$confidence == "high", 3, 2)
  }

  # Set edge attributes
  igraph::E(g)$label <- edge_labels
  igraph::E(g)$color <- edge_colors
  igraph::E(g)$width <- edge_widths

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

      edges <- data.frame(
        from = igraph::get.edgelist(g)[, 1],
        to = igraph::get.edgelist(g)[, 2],
        label = igraph::E(g)$label,
        color = igraph::E(g)$color,
        width = igraph::E(g)$width,
        title = igraph::E(g)$label,  # Tooltip
        stringsAsFactors = FALSE
      )

      return(visNetwork::visNetwork(nodes, edges) %>%
               visNetwork::visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
               visNetwork::visLayout(randomSeed = 123))
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
         main = "Dataset Connections")

    # Add legend
    legend("bottomright",
           legend = c("High Confidence", "Medium Confidence"),
           col = c("green", "orange"),
           lwd = c(3, 2),
           cex = 0.8)
  }
}

#' Vind het beste verbindingspad uit een lijst met paden
#'
#' @param paths Een lijst met verbindingspaden zoals geretourneerd door rio_find_connection_paths()
#'
#' @return Het beste verbindingspad op basis van lengte en betrouwbaarheid
#'
#' @keywords internal
find_best_connection_path <- function(paths) {
  if (length(paths) == 1) {
    return(paths[[1]])
  }

  # Score confidence levels
  confidence_scores <- c(high = 3, medium = 2, low = 1)

  # Calculate a score for each path
  path_scores <- numeric(length(paths))
  for (i in seq_along(paths)) {
    path <- paths[[i]]

    # Penalize for path length
    length_penalty <- length(path$path) - 1

    # Reward for high confidence connections
    confidence_sum <- 0
    for (conn in path$connections) {
      confidence_sum <- confidence_sum + confidence_scores[conn$relationship$confidence]
    }

    # Calculate final score (higher is better)
    path_scores[i] <- confidence_sum - length_penalty * 2
  }

  # Return the path with the highest score
  return(paths[[which.max(path_scores)]])
}

#' Voer joins uit om datasets te combineren op basis van een verbindingspad
#'
#' @param tibbles Lijst met tibbles om te combineren
#' @param path Verbindingspad dat specificeert hoe de tibbles moeten worden gekoppeld
#' @param verbose Logische waarde die aangeeft of gedetailleerde verbindingsinformatie moet worden weergegeven
#' @param reference_date Optionele datum om geldige records te filteren
#'
#' @return Een gecombineerde tibble
#'
#' @keywords internal
execute_dataset_joins <- function(tibbles, path, verbose, reference_date) {
  # Get the unique datasets in the path
  path_datasets <- unique(path$path)

  # Check that all necessary datasets are provided
  missing_datasets <- path_datasets[!path_datasets %in% names(tibbles)]
  if (length(missing_datasets) > 0) {
    stop("The following datasets required for the connection path are missing: ",
         paste(missing_datasets, collapse = ", "))
  }

  # Start with the first dataset in the path
  current_dataset <- path$path[1]
  result <- tibbles[[current_dataset]]

  # Process each connection in the path
  for (i in seq_along(path$connections)) {
    conn <- path$connections[[i]]

    # Determine which dataset to join next
    next_dataset <- if (conn$from == current_dataset) conn$to else conn$from

    # Get the joining fields
    if (length(conn$relationship$exact_matches) > 0) {
      # Use the first exact match if available
      by_field <- conn$relationship$exact_matches[1]

      if (verbose) {
        cli::cli_alert_info("Joining '{next_dataset}' using exact match field: {by_field}")
      }

      result <- dplyr::left_join(result, tibbles[[next_dataset]], by = by_field)
    } else if (length(conn$relationship$relation_matches) > 0) {
      # Use the first relation match
      rel_match <- conn$relationship$relation_matches[[1]]

      if (verbose) {
        cli::cli_alert_info("Joining '{next_dataset}' using relation field: {rel_match$rel_field}")
      }

      result <- dplyr::left_join(result, tibbles[[next_dataset]], by = rel_match$rel_field)
    } else if (length(conn$relationship$similar_matches) > 0) {
      # Use the first similar match
      sim_match <- conn$relationship$similar_matches[[1]]

      # Create a named list for the join
      by_field <- setNames(sim_match$target_field, sim_match$source_field)

      if (verbose) {
        cli::cli_alert_info("Joining '{next_dataset}' using similar fields: {sim_match$source_field} = {sim_match$target_field}")
      }

      result <- dplyr::left_join(result, tibbles[[next_dataset]], by = by_field)
    } else {
      stop("Could not determine how to join datasets: ", current_dataset, " and ", next_dataset)
    }

    # Update current dataset
    current_dataset <- next_dataset
  }

  # Apply reference date filtering if required
  if (!is.null(reference_date)) {
    # Find date fields in the result
    date_fields <- find_date_fields(result)

    if (length(date_fields) > 0 && verbose) {
      cli::cli_alert_info("Filtering by reference date: {reference_date}")
      cli::cli_alert_info("Found {length(date_fields)} date fields for filtering")
    }

    # Filter the result
    result <- rio_filter_by_reference_date(result, reference_date)
  }

  return(result)
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
