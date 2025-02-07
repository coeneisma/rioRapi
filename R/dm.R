library(httr2)
library(dm)

# Downloaden overzicht datasets

rio_package <- req |>
  req_url_path_append("package_show") |>
  req_headers("Accept" = "application/json") |>
  req_body_json(
    list(
      id = "rio_nfo_po_vo_vavo_mbo_ho"
    )
  ) |>
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)

#' Je kunt een overzicht krijgen van de verschillende variabelen uit
#' object$result$fields. Dit wil ik gebruiken om lege dataframes te genereren
#' die gebruikt kunnen worden om de relaties te definiëren


inlezen_dataset <- function(id, limit = NULL){
  req <- request("https://onderwijsdata.duo.nl/api/3/action/datastore_search")
  batch_size <- 1000
  offset <- 0
  all_records <- list()

  inlezen_dataset <- function(id, limit = NULL, offset = NULL){

    req |>
      req_headers("Accept" = "application/json") |>
      req_body_json(list(
        resource_id = id,
        limit = limit,
        offset = offset
      )) |>
      req_perform() |>
      resp_body_json(simplifyVector = TRUE)
  }

  initial_request <- inlezen_dataset(id, limit = 1)

  total_records <- initial_request$result$total # Totaal aantal records

  while (offset < total_records){
    batch <- inlezen_dataset(id, limit = batch_size, offset <- offset)

    all_records <- append(all_records, list(batch$result$records))

    offset <- offset + batch_size
  }

  return(dplyr::bind_rows(all_records))
#
#   dataset$result$records <- dataset$result$records[0, ]
#
#   return(dataset)


}




inlezen_alle_datasets <- function(rio_package_overzicht){
  # rio_package_overzicht <- rio_package_overzicht |>
  #   dplyr::filter(!is.na(total_record_count)) |>
  #   dplyr::filter(total_record_count != 0)

  rio_package_overzicht$id |>
    purrr::set_names(rio_package_overzicht$name) |>
    purrr::map(inlezen_dataset)

}

test_dataset <- inlezen_dataset("36f8ca3f-4251-4f6a-9109-eadc70b9d227")

datasets <- inlezen_alle_datasets(rio_package$result$resources)

