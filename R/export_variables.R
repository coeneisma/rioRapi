# library(rioRapi)
# library(dplyr)
# library(tidyr)
# library(readr)
#
# # Functie om velden van een dataset op te halen en te formatteren
# get_dataset_fields <- function(dataset_info) {
#   cat(sprintf("Ophalen velden voor dataset: %s (%s)\n", dataset_info$name, dataset_info$id))
#
#   # Velden ophalen met rio_get_fields
#   tryCatch({
#     fields <- rio_get_fields(dataset_id = dataset_info$id)
#
#     # Als we succesvol velden hebben opgehaald, maak een dataframe met de gewenste structuur
#     if (nrow(fields) > 0) {
#       result <- fields %>%
#         select(field_id = id, field_type = type) %>%
#         mutate(
#           dataset_id = dataset_info$id,
#           dataset_name = dataset_info$name,
#           description = dataset_info$description
#         ) %>%
#         select(dataset_id, dataset_name, description, field_id, field_type)
#
#       return(result)
#     } else {
#       cat(sprintf("Geen velden gevonden voor dataset: %s\n", dataset_info$name))
#       return(NULL)
#     }
#   }, error = function(e) {
#     cat(sprintf("Fout bij ophalen velden voor dataset %s: %s\n", dataset_info$name, e$message))
#     return(NULL)
#   })
# }
#
# # Hoofdfunctie om alle datasets en hun velden op te halen
# collect_all_fields <- function() {
#   # Alle beschikbare datasets ophalen
#   cat("Ophalen van lijst met beschikbare datasets...\n")
#   datasets <- rio_list_datasets()
#
#   if (nrow(datasets) == 0) {
#     stop("Geen datasets gevonden!")
#   }
#
#   cat(sprintf("Gevonden datasets: %d\n", nrow(datasets)))
#
#   # Voor elke dataset, haal de velden op
#   all_fields <- list()
#
#   for (i in 1:nrow(datasets)) {
#     dataset_info <- datasets[i, ]
#     fields_data <- get_dataset_fields(dataset_info)
#
#     if (!is.null(fields_data)) {
#       all_fields[[length(all_fields) + 1]] <- fields_data
#     }
#
#     # Kleine pauze om de API niet te overbelasten
#     Sys.sleep(0.5)
#   }
#
#   # Combineer alle resultaten
#   if (length(all_fields) > 0) {
#     combined_fields <- bind_rows(all_fields)
#     return(combined_fields)
#   } else {
#     stop("Geen velden gevonden voor alle datasets!")
#   }
# }
#
# # Voer de hoofdfunctie uit
# cat("Start met verzamelen van alle velden...\n")
# all_fields <- collect_all_fields()
#
# # Schrijf de resultaten naar een CSV-bestand
# output_file <- "rio_all_fields.csv"
# write_csv(all_fields, output_file)
# cat(sprintf("Resultaten opgeslagen in: %s\n", output_file))
