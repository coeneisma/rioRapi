#' Load RIO relations from a YAML file
#'
#' This function loads the RIO relations between tables from a YAML file.
#' If the file doesn't exist, an empty relation object is returned.
#'
#' @param file_path Path to the YAML file. Default is the package's built-in relations file.
#'
#' @return A list with the RIO relations structure.
#'
#' @keywords internal
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
#' @keywords internal
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

