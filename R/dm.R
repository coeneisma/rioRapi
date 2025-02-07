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


inlezen_dataset <- function(id, limit = 0){
  req <- request("https://onderwijsdata.duo.nl/api/3/action")

  dataset <- req |>
    req_url_path_append("datastore_search") |>
    req_method("POST") |>
    req_headers("Accept" = "application/json") |>
    req_body_json(list(
      resource_id = id,
      limit = limit
    )) |>
    req_perform() |>
    resp_body_json(simplifyVector = TRUE)

  aantal_records <- dataset$result$total
  runs <- ceiling(aantal_records / 1000)


  # dataset$results$records
  dataset
}


# Dit werkt nog niet
inlezen_alle_datasets <- function(rio_package_overzicht){

  rio_package_overzicht$id |>
    purrr::set_names(rio_package_overzicht$name) |>
    purrr::map(inlezen_dataset)

}

test_dataset <- inlezen_dataset(rio_package$result$id[1])

datasets <- inlezen_alle_datasets(rio_package$result$resources)

