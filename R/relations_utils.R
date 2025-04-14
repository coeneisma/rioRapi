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

#' Link datasets using defined relations
#'
#' This function links datasets based on predefined relations.
#'
#' @param ... Named tibbles to link.
#' @param relations Optional list of relations. If NULL, relations are loaded from the default file.
#' @param reference_date Optional date to filter valid records. Default is NULL (no filtering).
#' @param verbose Logical indicating whether to display detailed connection information.
#'        Default is TRUE.
#'
#' @return A linked tibble with data from all input tibbles.
#'
#' @examples
#' \dontrun{
#' # Retrieve data from different datasets
#' locations <- rio_get_data(dataset_name = "onderwijslocaties")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#'
#' # Link the datasets with predefined relations
#' linked_data <- rio_link_datasets_with_relations(
#'   locations = locations,
#'   institutions = institutions
#' )
#' }
#'
#' @export
rio_link_datasets_with_relations <- function(..., relations = NULL, reference_date = NULL, verbose = TRUE) {
  # Get the input tibbles
  tibbles <- list(...)

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for linking")
  }

  # Load relations if not provided
  if (is.null(relations)) {
    relations <- rio_load_relations()
    if (length(relations) == 0) {
      stop("No relations defined. Please define relations first or use rio_link_datasets() with method = 'auto'")
    }
  }

  # Get dataset names
  dataset_names <- names(tibbles)

  # Check for unnamed parameters
  if ("" %in% dataset_names) {
    stop("All datasets must be named. Example: rio_link_datasets_with_relations(dataset1 = df1, dataset2 = df2)")
  }

  if (verbose) {
    cli::cli_alert_info("Linking {length(dataset_names)} datasets: {paste(dataset_names, collapse = ', ')}")
  }

  # Create a graph to find the best path through the datasets
  # Note: We create an empty graph first and add vertices/edges later
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

  # Find a spanning tree to avoid cycles
  mst <- igraph::mst(g)

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
    cli::cli_alert_success("All datasets successfully linked. Final result has {nrow(result)} rows and {ncol(result)} columns")
  }

  return(result)
}
