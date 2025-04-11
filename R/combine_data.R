#' Intelligently link multiple RIO datasets
#'
#' This function links multiple RIO datasets by automatically detecting and using
#' the most appropriate connections between them. It leverages a pre-built structure mapping
#' of the RIO database to determine optimal joining paths.
#'
#' @param ... Named tibbles to link. Each argument should be a tibble with data from RIO.
#' @param method The linking method to use. Options are:
#'        - "smart" (default): Use the pre-built structure mapping to find optimal connections
#'        - "auto": Automatically detect connections from the provided datasets only
#'        - "manual": Use the explicitly specified 'by' parameter for joining
#' @param by Character vector of columns to join by (only used with method = "manual").
#'        If NULL and method = "manual", will try to join by all columns with the same name.
#' @param reference_date Optional date to filter valid records. Default is NULL (no filtering).
#' @param max_distance Maximum connection distance to consider when using method = "smart".
#'        Default is 3.
#' @param create_mapping Logical indicating whether to create a new structure mapping
#'        if none exists when using method = "smart". Default is TRUE.
#' @param suffix Suffixes to add to make non-by column names unique across the joined tables.
#' @param verbose Logical indicating whether to show detailed connection information.
#'        Default is TRUE.
#'
#' @return A linked tibble with data from all input tibbles.
#'
#' @examples
#' \dontrun{
#' # Get data from different datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' # Smart linking using pre-built structure mapping
#' linked_data <- rio_link_datasets(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen
#' )
#'
#' # Auto-detect connections without using pre-built mapping
#' linked_data_auto <- rio_link_datasets(
#'   locations = locations,
#'   institutions = institutions,
#'   method = "auto"
#' )
#'
#' # Manual linking with specified joining fields
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
                              max_distance = 3, create_mapping = TRUE,
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

    if (!mapping_exists && !create_mapping) {
      stop("No RIO structure mapping found and create_mapping = FALSE. ",
           "Run rio_map_structure() first or set create_mapping = TRUE.")
    }

    # Use smart combination based on the pre-built structure mapping
    return(rio_combine_smart(
      ...,
      auto_map = create_mapping,
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
#' Generate a visualization of dataset connections
#'
#' This function creates a visualization showing the connections between RIO datasets,
#' either from a structure mapping or from the datasets provided.
#'
#' @param ... Optional named tibbles to visualize connections between.
#' @param structure Optional RIO structure mapping as returned by rio_load_structure().
#'        If NULL and no tibbles are provided, it will be loaded automatically.
#' @param min_confidence Minimum confidence level for connections.
#'        Can be "high" or "medium". Default is "medium".
#' @param interactive Logical indicating whether to create an interactive visualization
#'        (requires visNetwork package). Default is TRUE.
#' @param highlight_datasets Character vector of dataset names to highlight in the visualization.
#'
#' @return A visualization object showing the connections between datasets.
#'
#' @examples
#' \dontrun{
#' # Visualize connections from the structure mapping
#' rio_visualize_dataset_connections()
#'
#' # Visualize connections between specific datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' rio_visualize_dataset_connections(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen,
#'   highlight_datasets = c("locations", "vestigingen")
#' )
#' }
#'
#' @export
rio_visualize_dataset_connections <- function(..., structure = NULL, min_confidence = "medium",
                                              interactive = TRUE, highlight_datasets = NULL) {
  # Get the input tibbles if provided
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
    # No tibbles provided, use the structure mapping
    if (is.null(structure)) {
      structure <- rio_load_structure(auto_create = TRUE)
    }

    # Create a graph from the structure mapping
    g <- igraph::make_graph(directed = FALSE)

    # Add all datasets as vertices
    dataset_names <- names(structure$datasets)
    g <- igraph::add_vertices(g, length(dataset_names), name = dataset_names)

    # Add highlight property to vertices
    igraph::V(g)$highlight <- dataset_names %in% highlight_datasets

    # Add edges based on relationships with sufficient confidence
    edge_labels <- character(0)
    edge_colors <- character(0)
    edge_widths <- numeric(0)

    for (rel_name in names(structure$relationships)) {
      rel <- structure$relationships[[rel_name]]

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
    }

    # Set edge attributes
    igraph::E(g)$label <- edge_labels
    igraph::E(g)$color <- edge_colors
    igraph::E(g)$width <- edge_widths

    # Set vertex attributes
    igraph::V(g)$label <- igraph::V(g)$name
    igraph::V(g)$color <- ifelse(igraph::V(g)$highlight, "yellow", "lightblue")

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
          color = ifelse(igraph::V(g)$highlight, "yellow", "lightblue"),
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
                 visNetwork::visLayout(randomSeed = 123))  # For consistent layout
      }
    }

    if (!interactive) {
      # Create static visualization with igraph
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
             legend = c("High Confidence", "Medium Confidence", "Highlighted Dataset"),
             col = c("green", "orange", "yellow"),
             lwd = c(3, 2, 10),
             cex = 0.8)
    }
  }
}

#' Join RIO datasets with dataframe lookup
#'
#' This function joins data from a RIO dataset based on IDs in a dataframe.
#'
#' @param df A dataframe containing IDs to look up.
#' @param id_col The name of the column in the dataframe containing IDs.
#' @param dataset_id The ID of the dataset resource to retrieve.
#' @param rio_id_field The name of the ID field in the RIO dataset. If NULL, uses id_col.
#' @param reference_date Optional date to filter valid records. Default is the current date.
#'
#' @return A tibble with the input dataframe joined with data from the RIO dataset.
#'
#' @examples
#' \dontrun{
#' # Join a dataframe with institution data
#' df <- data.frame(inst_id = c("12345", "67890"), value = c(10, 20))
#' result <- rio_join(
#'   df = df,
#'   id_col = "inst_id",
#'   dataset_id = "institution-resource-id",
#'   rio_id_field = "INSTELLINGSCODE"
#' )
#' }
#'
#' @export
rio_join <- function(df, id_col, dataset_id, rio_id_field = NULL, reference_date = NULL) {
  # If rio_id_field is not specified, use id_col
  if (is.null(rio_id_field)) {
    rio_id_field <- id_col
  }

  # Extract unique IDs from the dataframe
  ids <- unique(df[[id_col]])

  # Look up the data using rio_lookup (internal function)
  rio_data <- rio_lookup(
    ids = ids,
    dataset_id = dataset_id,
    id_field = rio_id_field,
    reference_date = reference_date
  )

  # Join with the input dataframe
  if (id_col != rio_id_field) {
    # Rename rio_id_field to id_col for joining
    rio_data <- rio_data |>
      dplyr::rename(!!id_col := !!rio_id_field)
  }

  result <- dplyr::left_join(df, rio_data, by = id_col)

  return(result)
}

#' Map the structure of RIO datasets and their relationships
#'
#' This function analyzes all available RIO datasets and their fields to identify
#' potential relationships between them. The results are saved to a file for future use.
#'
#' @param force Logical indicating whether to force a remapping even if a recent mapping exists.
#'        Default is FALSE.
#' @param days_threshold Number of days after which a mapping is considered outdated.
#'        Default is 28 (4 weeks).
#' @param save_dir Directory where to save the mapping file. Default is the package's user data dir.
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return Invisibly returns a list containing the RIO structure mapping.
#'
#' @examples
#' \dontrun{
#' # Map the RIO structure and save it
#' rio_map_structure()
#'
#' # Force remapping even if a recent mapping exists
#' rio_map_structure(force = TRUE)
#' }
#'
#' @export
rio_map_structure <- function(force = FALSE, days_threshold = 28,
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
    cli::cli_alert_info("Mapping RIO structure. This may take several minutes...")
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
    cli::cli_alert_success("RIO structure mapping completed: found {relation_count} potential relationships")
    cli::cli_alert_info("Mapping saved to: {mapping_file}")
  }

  invisible(rio_structure)
}

#' Load the RIO structure mapping
#'
#' This function loads the saved RIO structure mapping. If no mapping exists or it's
#' outdated, it will optionally create a new one.
#'
#' @param auto_create Logical indicating whether to automatically create a mapping if
#'        none exists or if it's outdated. Default is TRUE.
#' @param days_threshold Number of days after which a mapping is considered outdated.
#'        Default is 28 (4 weeks).
#' @param quiet Logical indicating whether to suppress progress messages. Default is FALSE.
#'
#' @return A list containing the RIO structure mapping.
#'
#' @examples
#' \dontrun{
#' # Load the RIO structure mapping
#' mapping <- rio_load_structure()
#' }
#'
#' @export
rio_load_structure <- function(auto_create = TRUE, days_threshold = 28, quiet = FALSE) {
  mapping_file <- file.path(get_rio_data_dir(), "rio_structure_mapping.rds")

  # Check if mapping exists
  if (!file.exists(mapping_file)) {
    if (auto_create) {
      if (!quiet) {
        cli::cli_alert_info("No existing RIO structure mapping found. Creating new mapping...")
      }
      return(rio_map_structure(quiet = quiet))
    } else {
      stop("No RIO structure mapping found. Run rio_map_structure() first.")
    }
  }

  # Check if mapping is recent
  file_info <- file.info(mapping_file)
  file_age <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))

  if (file_age >= days_threshold) {
    if (auto_create) {
      if (!quiet) {
        cli::cli_alert_warning("Existing mapping is {round(file_age, 1)} days old (threshold: {days_threshold}). Regenerating...")
      }
      return(rio_map_structure(quiet = quiet))
    } else if (!quiet) {
      cli::cli_alert_warning("Existing mapping is {round(file_age, 1)} days old (threshold: {days_threshold})")
    }
  } else if (!quiet) {
    cli::cli_alert_info("Using existing RIO structure mapping (age: {round(file_age, 1)} days)")
  }

  # Load the mapping
  mapping <- readRDS(mapping_file)
  return(mapping)
}

#' Generate a visualization of dataset connections
#'
#' This function creates a visualization showing the connections between RIO datasets,
#' either from a structure mapping or from the datasets provided.
#'
#' @param ... Optional named tibbles to visualize connections between.
#' @param structure Optional RIO structure mapping as returned by rio_load_structure().
#'        If NULL and no tibbles are provided, it will be loaded automatically.
#' @param min_confidence Minimum confidence level for connections.
#'        Can be "high" or "medium". Default is "medium".
#' @param interactive Logical indicating whether to create an interactive visualization
#'        (requires visNetwork package). Default is TRUE.
#' @param highlight_datasets Character vector of dataset names to highlight in the visualization.
#'
#' @return A visualization object showing the connections between datasets.
#'
#' @examples
#' \dontrun{
#' # Visualize connections from the structure mapping
#' rio_visualize_dataset_connections()
#'
#' # Visualize connections between specific datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' rio_visualize_dataset_connections(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen,
#'   highlight_datasets = c("locations", "vestigingen")
#' )
#' }
#'
#' @export
rio_visualize_dataset_connections <- function(..., structure = NULL, min_confidence = "medium",
                                              interactive = TRUE, highlight_datasets = NULL) {
  # Get the input tibbles if provided
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
    # No tibbles provided, use the structure mapping
    if (is.null(structure)) {
      structure <- rio_load_structure(auto_create = TRUE)
    }

    # Create a graph from the structure mapping
    g <- igraph::make_graph(directed = FALSE)

    # Add all datasets as vertices
    dataset_names <- names(structure$datasets)
    g <- igraph::add_vertices(g, length(dataset_names), name = dataset_names)

    # Add highlight property to vertices
    igraph::V(g)$highlight <- dataset_names %in% highlight_datasets

    # Add edges based on relationships with sufficient confidence
    edge_labels <- character(0)
    edge_colors <- character(0)
    edge_widths <- numeric(0)

    for (rel_name in names(structure$relationships)) {
      rel <- structure$relationships[[rel_name]]

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
    }

    # Set edge attributes
    igraph::E(g)$label <- edge_labels
    igraph::E(g)$color <- edge_colors
    igraph::E(g)$width <- edge_widths

    # Set vertex attributes
    igraph::V(g)$label <- igraph::V(g)$name
    igraph::V(g)$color <- ifelse(igraph::V(g)$highlight, "yellow", "lightblue")

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
          color = ifelse(igraph::V(g)$highlight, "yellow", "lightblue"),
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
                 visNetwork::visLayout(randomSeed = 123))  # For consistent layout
      }
    }

    if (!interactive) {
      # Create static visualization with igraph
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
             legend = c("High Confidence", "Medium Confidence", "Highlighted Dataset"),
             col = c("green", "orange", "yellow"),
             lwd = c(3, 2, 10),
             cex = 0.8)
    }
  }
}

#' Automatically detect and create connections between RIO datasets
#'
#' This function analyzes the structure of RIO datasets to automatically detect
#' possible connections between them, and can optionally join the datasets based on
#' these connections.
#'
#' @param ... Named tibbles to analyze. Each argument should be a tibble with data from RIO.
#' @param auto_join Logical, whether to automatically join the datasets. Default is FALSE,
#'        which means the function will only return the suggested connections.
#' @param reference_date Optional date to filter valid records. Default is the current date.
#' @param prefer_codes Logical, whether to prefer using code fields over ID fields
#'        for joining when multiple options are available. Default is TRUE.
#' @param verbose Logical, whether to print detailed information about detected
#'        connections. Default is TRUE.
#'
#' @return If auto_join = FALSE, returns a list with detected connections.
#'         If auto_join = TRUE, returns a combined tibble with data from all input tibbles.
#'
#' @examples
#' \dontrun{
#' # Get data from different datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' # Detect possible connections without joining
#' connections <- rio_auto_connect(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen
#' )
#'
#' # Automatically join the datasets
#' combined_data <- rio_auto_connect(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen,
#'   auto_join = TRUE
#' )
#' }
#'
#' @export
rio_auto_connect <- function(..., auto_join = FALSE, reference_date = NULL,
                             prefer_codes = TRUE, verbose = TRUE) {
  # Get the list of tibbles
  tibbles <- list(...)

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for connection detection")
  }

  # Get the names of the datasets
  dataset_names <- names(tibbles)

  # Create a data structure to store connections between datasets
  connections <- list()

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

  # Function to detect candidate joining fields
  find_candidate_joining_fields <- function(df1, df2, name1, name2) {
    # Get column names
    cols1 <- colnames(df1)
    cols2 <- colnames(df2)

    # Find columns that match common patterns
    id_cols1 <- cols1[grepl(paste(common_id_patterns, collapse = "|"), cols1)]
    id_cols2 <- cols2[grepl(paste(common_id_patterns, collapse = "|"), cols2)]

    # Find exact matches between datasets
    exact_matches <- intersect(id_cols1, id_cols2)

    # If we have exact matches, prioritize them
    if (length(exact_matches) > 0) {
      return(list(
        dataset1 = name1,
        dataset2 = name2,
        join_fields = exact_matches,
        confidence = "high"
      ))
    }

    # Look for similar fields (e.g., CODE in one dataset might match XXX_CODE in another)
    potential_matches <- list()
    for (col1 in id_cols1) {
      for (col2 in id_cols2) {
        # Extract the base part of the field name (removing prefixes)
        base1 <- sub("^.*_", "", col1)
        base2 <- sub("^.*_", "", col2)

        if (base1 == base2) {
          # Check if the data types are compatible
          type1 <- class(df1[[col1]])[1]
          type2 <- class(df2[[col2]])[1]

          compatible <- (type1 == type2) ||
            (type1 %in% c("character", "factor") && type2 %in% c("character", "factor")) ||
            (type1 %in% c("numeric", "integer") && type2 %in% c("numeric", "integer"))

          if (compatible) {
            potential_matches <- c(potential_matches, list(list(
              field1 = col1,
              field2 = col2,
              confidence = "medium"
            )))
          }
        }
      }
    }

    # Check for relational tables (tables with names starting with "relaties_")
    is_relation_table <- function(name) {
      grepl("^relaties_", name, ignore.case = TRUE)
    }

    # If one of the tables is a relation table, handle it specially
    if (is_relation_table(name1) || is_relation_table(name2)) {
      # Determine which is the relation table
      rel_name <- if (is_relation_table(name1)) name1 else name2
      other_name <- if (is_relation_table(name1)) name2 else name1
      rel_df <- if (is_relation_table(name1)) df1 else df2
      other_df <- if (is_relation_table(name1)) df2 else df1

      # Get the potential linking fields by analyzing the relation table name
      # Example: relaties_onderwijsbesturen_onderwijsaanbieders would link to
      # onderwijsbesturen via ONDERWIJSBESTUURID and to onderwijsaanbieders via ONDERWIJSAANBIEDERID
      rel_parts <- strsplit(rel_name, "_")[[1]]
      rel_parts <- rel_parts[rel_parts != "relaties"]

      for (part in rel_parts) {
        # Try to find a corresponding ID field in both tables
        potential_id_field <- paste0(toupper(part), "ID")
        potential_code_field <- paste0(toupper(part), "CODE")

        if (potential_id_field %in% colnames(rel_df) &&
            potential_id_field %in% colnames(other_df)) {
          return(list(
            dataset1 = rel_name,
            dataset2 = other_name,
            join_fields = potential_id_field,
            confidence = "high"
          ))
        } else if (potential_code_field %in% colnames(rel_df) &&
                   potential_code_field %in% colnames(other_df)) {
          return(list(
            dataset1 = rel_name,
            dataset2 = other_name,
            join_fields = potential_code_field,
            confidence = "high"
          ))
        }
      }
    }

    # If we have potential matches, return them
    if (length(potential_matches) > 0) {
      # Sort potential matches by confidence
      potential_matches <- potential_matches[order(sapply(potential_matches, function(x) x$confidence), decreasing = TRUE)]

      return(list(
        dataset1 = name1,
        dataset2 = name2,
        join_fields = potential_matches,
        confidence = "medium"
      ))
    }

    # If no matches were found, return an empty result
    return(list(
      dataset1 = name1,
      dataset2 = name2,
      join_fields = character(0),
      confidence = "low"
    ))
  }

  # Detect connections between all pairs of datasets
  for (i in 1:(length(tibbles) - 1)) {
    for (j in (i + 1):length(tibbles)) {
      # Get dataset names
      name_i <- dataset_names[i]
      name_j <- dataset_names[j]

      # Find candidate joining fields
      connection <- find_candidate_joining_fields(
        tibbles[[i]], tibbles[[j]], name_i, name_j
      )

      # Store the connection
      connections[[paste(name_i, "to", name_j)]] <- connection

      # Print information if verbose
      if (verbose && length(connection$join_fields) > 0) {
        if (connection$confidence == "high") {
          cat(sprintf("Found high-confidence connection between '%s' and '%s' using field(s): %s\n",
                      name_i, name_j, paste(connection$join_fields, collapse = ", ")))
        } else if (connection$confidence == "medium") {
          cat(sprintf("Found potential connection between '%s' and '%s':\n", name_i, name_j))
          for (match in connection$join_fields) {
            cat(sprintf("  - %s.%s to %s.%s (confidence: %s)\n",
                        name_i, match$field1, name_j, match$field2, match$confidence))
          }
        }
      }
    }
  }

  # If auto_join is TRUE, try to automatically join the datasets
  if (auto_join) {
    if (verbose) {
      cat("\nAttempting to automatically join datasets...\n")
    }

    # Create a graph of dataset connections
    library(igraph)

    # Create a graph with datasets as nodes
    g <- make_graph(character(0), directed = FALSE)
    g <- add_vertices(g, length(dataset_names), name = dataset_names)

    # Add edges for high-confidence connections
    for (conn_name in names(connections)) {
      conn <- connections[[conn_name]]
      if (conn$confidence == "high" && length(conn$join_fields) > 0) {
        g <- add_edges(g, c(conn$dataset1, conn$dataset2))
      }
    }

    # Find a minimum spanning tree to determine joining order
    if (ecount(g) > 0) {
      mst <- minimum.spanning.tree(g)

      # Create a joining plan
      joining_plan <- list()
      for (edge_idx in 1:ecount(mst)) {
        edge <- ends(mst, edge_idx)
        ds1 <- V(mst)$name[edge[1]]
        ds2 <- V(mst)$name[edge[2]]

        # Find the connection for this edge
        conn_key1 <- paste(ds1, "to", ds2)
        conn_key2 <- paste(ds2, "to", ds1)
        conn <- if (conn_key1 %in% names(connections)) connections[[conn_key1]] else connections[[conn_key2]]

        joining_plan[[length(joining_plan) + 1]] <- list(
          left = ds1,
          right = ds2,
          by = if (is.character(conn$join_fields)) conn$join_fields else conn$join_fields[[1]]$field1
        )
      }

      # Execute the joining plan
      result <- tibbles[[1]]
      current_dataset <- dataset_names[1]

      # Keep track of datasets that have been joined
      joined_datasets <- current_dataset

      while (length(joined_datasets) < length(dataset_names)) {
        # Find a plan that connects to the current joined set
        next_plan <- NULL
        for (plan in joining_plan) {
          if ((plan$left %in% joined_datasets && !(plan$right %in% joined_datasets)) ||
              (plan$right %in% joined_datasets && !(plan$left %in% joined_datasets))) {
            next_plan <- plan
            break
          }
        }

        if (is.null(next_plan)) {
          if (verbose) {
            cat("Warning: Could not find a complete joining plan. Some datasets may not be connected.\n")
          }
          break
        }

        # Determine which dataset to join next
        next_dataset <- if (next_plan$left %in% joined_datasets) next_plan$right else next_plan$left

        # Join the datasets
        by_field <- next_plan$by

        if (verbose) {
          cat(sprintf("Joining '%s' to result using field '%s'\n", next_dataset, by_field))
        }

        result <- dplyr::left_join(result, tibbles[[next_dataset]], by = by_field)

        # Mark the dataset as joined
        joined_datasets <- c(joined_datasets, next_dataset)
      }

      return(result)
    } else {
      warning("Could not find high-confidence connections for automatic joining")
      return(connections)
    }
  } else {
    # Just return the detected connections
    return(connections)
  }
}

#' Find connection paths between RIO datasets
#'
#' This function finds all possible connection paths between specified datasets
#' using the stored RIO structure mapping.
#'
#' @param ... Named or unnamed character strings of dataset names to connect.
#' @param structure The RIO structure mapping as returned by rio_load_structure().
#'        If NULL, it will be loaded automatically.
#' @param max_path_length Maximum length of connection paths to consider. Default is 3.
#' @param min_confidence Minimum confidence level for connections. Can be "high" or "medium".
#'        Default is "medium".
#'
#' @return A list of possible connection paths between the datasets.
#'
#' @examples
#' \dontrun{
#' # Find connection paths between three datasets
#' paths <- rio_find_connection_paths("onderwijslocaties", "vestigingserkenningen", "onderwijsaanbieders")
#' }
#'
#' @export
rio_find_connection_paths <- function(..., structure = NULL, max_path_length = 3, min_confidence = "medium") {
  # Load structure if not provided
  if (is.null(structure)) {
    structure <- rio_load_structure(auto_create = TRUE)
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

#' Find candidate joining fields between two datasets
#'
#' @param df1 First dataframe or tibble
#' @param df2 Second dataframe or tibble
#' @param name1 Name of the first dataset
#' @param name2 Name of the second dataset
#'
#' @return A list with details about the potential connection
#'
#' @keywords internal
find_candidate_joining_fields <- function(df1, df2, name1, name2) {
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

  # Get column names
  cols1 <- colnames(df1)
  cols2 <- colnames(df2)

  # Find columns that match common patterns
  id_cols1 <- cols1[grepl(paste(common_id_patterns, collapse = "|"), cols1)]
  id_cols2 <- cols2[grepl(paste(common_id_patterns, collapse = "|"), cols2)]

  # Find exact matches between datasets
  exact_matches <- intersect(id_cols1, id_cols2)

  # If we have exact matches, prioritize them
  if (length(exact_matches) > 0) {
    return(list(
      dataset1 = name1,
      dataset2 = name2,
      join_fields = exact_matches,
      confidence = "high"
    ))
  }

  # Look for similar fields (e.g., CODE in one dataset might match XXX_CODE in another)
  potential_matches <- list()
  for (col1 in id_cols1) {
    for (col2 in id_cols2) {
      # Extract the base part of the field name (removing prefixes)
      base1 <- sub("^.*_", "", col1)
      base2 <- sub("^.*_", "", col2)

      if (base1 == base2) {
        # Check if the data types are compatible
        type1 <- class(df1[[col1]])[1]
        type2 <- class(df2[[col2]])[1]

        compatible <- (type1 == type2) ||
          (type1 %in% c("character", "factor") && type2 %in% c("character", "factor")) ||
          (type1 %in% c("numeric", "integer") && type2 %in% c("numeric", "integer"))

        if (compatible) {
          potential_matches <- c(potential_matches, list(list(
            field1 = col1,
            field2 = col2,
            confidence = "medium"
          )))
        }
      }
    }
  }

  # Check for relational tables (tables with names starting with "relaties_")
  is_relation_table <- function(name) {
    grepl("^relaties_", name, ignore.case = TRUE)
  }

  # If one of the tables is a relation table, handle it specially
  if (is_relation_table(name1) || is_relation_table(name2)) {
    # Determine which is the relation table
    rel_name <- if (is_relation_table(name1)) name1 else name2
    other_name <- if (is_relation_table(name1)) name2 else name1
    rel_df <- if (is_relation_table(name1)) df1 else df2
    other_df <- if (is_relation_table(name1)) df2 else df1

    # Get the potential linking fields by analyzing the relation table name
    # Example: relaties_onderwijsbesturen_onderwijsaanbieders would link to
    # onderwijsbesturen via ONDERWIJSBESTUURID and to onderwijsaanbieders via ONDERWIJSAANBIEDERID
    rel_parts <- strsplit(rel_name, "_")[[1]]
    rel_parts <- rel_parts[rel_parts != "relaties"]

    for (part in rel_parts) {
      # Try to find a corresponding ID field in both tables
      potential_id_field <- paste0(toupper(part), "ID")
      potential_code_field <- paste0(toupper(part), "CODE")

      if (potential_id_field %in% colnames(rel_df) &&
          potential_id_field %in% colnames(other_df)) {
        return(list(
          dataset1 = rel_name,
          dataset2 = other_name,
          join_fields = potential_id_field,
          confidence = "high"
        ))
      } else if (potential_code_field %in% colnames(rel_df) &&
                 potential_code_field %in% colnames(other_df)) {
        return(list(
          dataset1 = rel_name,
          dataset2 = other_name,
          join_fields = potential_code_field,
          confidence = "high"
        ))
      }
    }
  }

  # If we have potential matches, return them
  if (length(potential_matches) > 0) {
    # Sort potential matches by confidence
    potential_matches <- potential_matches[order(sapply(potential_matches, function(x) x$confidence), decreasing = TRUE)]

    return(list(
      dataset1 = name1,
      dataset2 = name2,
      join_fields = potential_matches,
      confidence = "medium"
    ))
  }

  # If no matches were found, return an empty result
  return(list(
    dataset1 = name1,
    dataset2 = name2,
    join_fields = character(0),
    confidence = "low"
  ))
}

#' Execute joins to combine datasets based on a connection path
#'
#' @param tibbles List of tibbles to combine
#' @param path Connection path specifying how to join the tibbles
#' @param verbose Logical indicating whether to show detailed connection information
#' @param reference_date Optional date to filter valid records
#'
#' @return A combined tibble
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

#' Find the best connection path from a list of paths
#'
#' @param paths A list of connection paths as returned by rio_find_connection_paths()
#'
#' @return The best connection path based on length and confidence
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

#' Find date fields in a dataframe
#'
#' @param df A dataframe
#'
#' @return A character vector of date field names
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


#' Get the directory for saving RIO package data
#'
#' @return A character string with the path to the RIO data directory
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
