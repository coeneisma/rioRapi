#' Combine RIO datasets
#'
#' This function combines multiple RIO datasets based on a common ID field.
#'
#' @param ... Named tibbles to combine. Each argument should be a tibble with data from RIO.
#' @param by Character vector of columns to join by. If NULL, will join by all columns with the same name.
#' @param suffix Suffixes to add to make non-by column names unique across the joined tables.
#'
#' @return A combined tibble with data from all input tibbles.
#'
#' @examples
#' \dontrun{
#' locations <- rio_get_locations(city = "Rotterdam")
#' institutions <- rio_get_data(dataset_id = "institution-resource-id")
#'
#' # Combine datasets
#' combined_data <- rio_combine(
#'   locations = locations,
#'   institutions = institutions,
#'   by = "INSTELLINGSCODE"
#' )
#' }
#'
#' @export
rio_combine <- function(..., by = NULL, suffix = c("_x", "_y")) {
  # Get the list of tibbles
  tibbles <- list(...)

  # Check if tibbles were provided
  if (length(tibbles) < 2) {
    stop("At least two datasets must be provided for combining")
  }

  # Start with the first tibble
  result <- tibbles[[1]]

  # Combine with each of the remaining tibbles
  for (i in 2:length(tibbles)) {
    result <- dplyr::left_join(result, tibbles[[i]], by = by, suffix = suffix)
  }

  return(result)
}

#' Join RIO datasets with vector lookup
#'
#' This function joins data from a RIO dataset based on a vector of IDs.
#'
#' @param ids A vector of IDs to look up.
#' @param dataset_id The ID of the dataset resource to retrieve.
#' @param id_field The name of the ID field in the dataset.
#' @param reference_date Optional date to filter valid records. Default is the current date.
#'
#' @return A tibble with data for the specified IDs.
#'
#' @keywords internal
rio_lookup <- function(ids, dataset_id, id_field, reference_date = NULL) {
  # Create connection
  conn <- rio_api_connection()

  # Fetch data by IDs
  filters <- list()
  filters[[id_field]] <- ids

  data <- rio_get_data(
    dataset_id = dataset_id,
    quiet = TRUE,
    filters = filters
  )

  # Apply date filtering if a reference date is provided
  if (!is.null(reference_date)) {
    data <- rio_filter_by_date(data, reference_date = reference_date)
  }

  return(data)
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

#' Visualize connections between RIO datasets
#'
#' This function creates a network visualization of the connections between RIO datasets,
#' making it easier to understand how different datasets relate to each other.
#'
#' @param connections A list of connections as returned by rio_auto_connect().
#' @param min_confidence The minimum confidence level to include in the visualization.
#'        Can be "high", "medium", or "low". Default is "medium".
#' @param interactive Logical, whether to create an interactive visualization (requires visNetwork package).
#'        If FALSE, creates a static plot using igraph. Default is TRUE.
#'
#' @return A plot object that visualizes the connections between datasets.
#'
#' @examples
#' \dontrun{
#' # Get data from different datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' # Detect connections between datasets
#' connections <- rio_auto_connect(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen
#' )
#'
#' # Visualize the connections
#' rio_visualize_connections(connections)
#' }
#'
#' @export
rio_visualize_connections <- function(connections, min_confidence = "medium", interactive = TRUE) {
  # Check if required packages are available
  if (interactive && !requireNamespace("visNetwork", quietly = TRUE)) {
    warning("Package 'visNetwork' is required for interactive visualization. Using static visualization instead.")
    interactive <- FALSE
  }

  if (!requireNamespace("igraph", quietly = TRUE)) {
    stop("Package 'igraph' is required for this function")
  }

  # Convert confidence level to numeric
  confidence_levels <- c("low" = 1, "medium" = 2, "high" = 3)
  min_confidence_num <- confidence_levels[min_confidence]

  # Create a list of unique datasets
  datasets <- unique(c(
    sapply(connections, function(x) x$dataset1),
    sapply(connections, function(x) x$dataset2)
  ))

  # Create a graph
  g <- igraph::make_graph(character(0), directed = FALSE)
  g <- igraph::add_vertices(g, length(datasets), name = datasets)

  # Add edges for connections with sufficient confidence
  edge_labels <- character(0)
  edge_colors <- character(0)
  edge_widths <- numeric(0)

  for (conn_name in names(connections)) {
    conn <- connections[[conn_name]]
    conn_confidence <- confidence_levels[conn$confidence]

    if (conn_confidence >= min_confidence_num && length(conn$join_fields) > 0) {
      # Add an edge
      g <- igraph::add_edges(g, c(conn$dataset1, conn$dataset2))

      # Get the joining field(s) for the edge label
      if (is.character(conn$join_fields)) {
        join_fields <- conn$join_fields
      } else {
        join_fields <- sapply(conn$join_fields, function(x) paste(x$field1, "=", x$field2))
      }

      edge_labels <- c(edge_labels, paste(join_fields, collapse = "\n"))

      # Assign color and width based on confidence
      edge_colors <- c(edge_colors, switch(conn$confidence,
                                           "high" = "green",
                                           "medium" = "orange",
                                           "low" = "red"))

      edge_widths <- c(edge_widths, switch(conn$confidence,
                                           "high" = 3,
                                           "medium" = 2,
                                           "low" = 1))
    }
  }

  # Set edge attributes
  igraph::E(g)$label <- edge_labels
  igraph::E(g)$color <- edge_colors
  igraph::E(g)$width <- edge_widths

  # Set vertex attributes
  igraph::V(g)$label <- igraph::V(g)$name

  # Create visualization
  if (interactive) {
    # Create interactive visualization with visNetwork
    nodes <- data.frame(
      id = igraph::V(g)$name,
      label = igraph::V(g)$name,
      title = igraph::V(g)$name,  # Tooltip
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

    visNetwork::visNetwork(nodes, edges) %>%
      visNetwork::visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
      visNetwork::visLayout(randomSeed = 123)  # For consistent layout
  } else {
    # Create static visualization with igraph
    plot(g,
         layout = igraph::layout_with_fr(g),
         vertex.size = 20,
         vertex.color = "lightblue",
         vertex.label.color = "black",
         vertex.label.cex = 0.8,
         edge.label.cex = 0.7,
         edge.curved = 0.2,
         main = "RIO Datasets Connection Network")

    # Add legend
    legend("bottomright",
           legend = c("High Confidence", "Medium Confidence", "Low Confidence"),
           col = c("green", "orange", "red"),
           lwd = c(3, 2, 1),
           cex = 0.8)
  }
}

#' Find optimal joining path between RIO datasets
#'
#' This function determines the optimal way to join multiple RIO datasets,
#' based on the detected connections between them.
#'
#' @param connections A list of connections as returned by rio_auto_connect().
#' @param from The name of the starting dataset.
#' @param to The name of the target dataset.
#' @param min_confidence The minimum confidence level to consider for connections.
#'        Can be "high", "medium", or "low". Default is "medium".
#'
#' @return A list describing the optimal joining path, including the sequence of
#'         datasets to join and the fields to use for each join.
#'
#' @examples
#' \dontrun{
#' # Get data from different datasets
#' locations <- rio_get_locations(city = "Amsterdam")
#' institutions <- rio_get_data(dataset_name = "onderwijsinstellingserkenningen")
#' vestigingen <- rio_get_data(dataset_name = "vestigingserkenningen")
#'
#' # Detect connections between datasets
#' connections <- rio_auto_connect(
#'   locations = locations,
#'   institutions = institutions,
#'   vestigingen = vestigingen
#' )
#'
#' # Find the optimal joining path
#' joining_path <- rio_find_joining_path(
#'   connections,
#'   from = "locations",
#'   to = "institutions"
#' )
#' }
#'
#' @export
rio_find_joining_path <- function(connections, from, to, min_confidence = "medium") {
  # Check if required packages are available
  if (!requireNamespace("igraph", quietly = TRUE)) {
    stop("Package 'igraph' is required for this function")
  }

  # Convert confidence level to numeric
  confidence_levels <- c("low" = 1, "medium" = 2, "high" = 3)
  min_confidence_num <- confidence_levels[min_confidence]

  # Create a list of unique datasets
  datasets <- unique(c(
    sapply(connections, function(x) x$dataset1),
    sapply(connections, function(x) x$dataset2)
  ))

  # Check if from and to datasets exist
  if (!(from %in% datasets)) {
    stop(sprintf("Starting dataset '%s' not found in connections", from))
  }

  if (!(to %in% datasets)) {
    stop(sprintf("Target dataset '%s' not found in connections", to))
  }

  # Create a graph
  g <- igraph::make_graph(character(0), directed = FALSE)
  g <- igraph::add_vertices(g, length(datasets), name = datasets)

  # Add edges for connections with sufficient confidence
  # Store connection details in a separate list
  edge_connection_details <- list()

  for (conn_name in names(connections)) {
    conn <- connections[[conn_name]]
    conn_confidence <- confidence_levels[conn$confidence]

    if (conn_confidence >= min_confidence_num && length(conn$join_fields) > 0) {
      # Add an edge
      edge_id <- length(edge_connection_details) + 1
      g <- igraph::add_edges(g, c(conn$dataset1, conn$dataset2))

      # Store connection details
      edge_connection_details[[edge_id]] <- list(
        dataset1 = conn$dataset1,
        dataset2 = conn$dataset2,
        join_fields = conn$join_fields,
        confidence = conn$confidence
      )

      # Set edge weight based on confidence (higher confidence = lower weight)
      igraph::E(g)$weight[edge_id] <- switch(conn$confidence,
                                             "high" = 1,
                                             "medium" = 3,
                                             "low" = 5)
    }
  }

  # Find the shortest path between from and to
  tryCatch({
    path <- igraph::shortest_paths(g, from = from, to = to, weights = igraph::E(g)$weight)
    path_vertices <- path$vpath[[1]]
    path_datasets <- igraph::V(g)$name[path_vertices]

    # Build the joining instructions
    joining_path <- list(
      path = path_datasets,
      joins = list()
    )

    # For each pair of consecutive datasets in the path, find the connection details
    for (i in 1:(length(path_datasets) - 1)) {
      ds1 <- path_datasets[i]
      ds2 <- path_datasets[i + 1]

      # Find the edge between these two datasets
      edge_index <- NULL
      for (j in 1:length(edge_connection_details)) {
        edge <- edge_connection_details[[j]]
        if ((edge$dataset1 == ds1 && edge$dataset2 == ds2) ||
            (edge$dataset1 == ds2 && edge$dataset2 == ds1)) {
          edge_index <- j
          break
        }
      }

      # Get the connection details
      edge <- edge_connection_details[[edge_index]]

      # Format join fields
      join_by <- if (is.character(edge$join_fields)) {
        edge$join_fields
      } else if (length(edge$join_fields) > 0) {
        # For medium confidence joins, we need to extract the field mappings
        join_mapping <- list()
        for (jf in edge$join_fields) {
          if (edge$dataset1 == ds1) {
            join_mapping[[jf$field1]] <- jf$field2
          } else {
            join_mapping[[jf$field2]] <- jf$field1
          }
        }
        join_mapping
      } else {
        NULL
      }

      joining_path$joins[[i]] <- list(
        from = ds1,
        to = ds2,
        by = join_by,
        confidence = edge$confidence
      )
    }

    return(joining_path)
  }, error = function(e) {
    stop(sprintf("Could not find a path from '%s' to '%s': %s", from, to, e$message))
  })
}
