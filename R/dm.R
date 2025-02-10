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

  print(glue::glue("Downloaden dataset met ID: {id}"))

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

  print(glue::glue("Aantal te downloaden records: {total_records}"))

  while (offset < total_records){
    print(glue::glue("Downloaden records vanaf {offset} van in totaal {total_records} records"))
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
  rio_package_overzicht <- rio_package_overzicht |>
    dplyr::filter(!is.na(total_record_count)) |>
    dplyr::filter(total_record_count != 0) |>
    dplyr::arrange(desc(total_record_count))

  rio_package_overzicht$id |>
    purrr::set_names(rio_package_overzicht$name) |>
    purrr::map(inlezen_dataset)

}

test_dataset <- inlezen_dataset("36f8ca3f-4251-4f6a-9109-eadc70b9d227")

datasets <- inlezen_alle_datasets(rio_package$result$resources)

# Omzetten naar dm-object

datasets_dm <- dm::as_dm(datasets)

dm_enum_pk_candidates(datasets_dm, "aangeboden_vo_opleidingen")

datasets_dm_keys <- datasets_dm |>
  # Primairy keys
  dm_add_pk(vo_onderwijslicenties, LICENTIECODE) |>
  dm_add_fk(vo_onderwijslicenties, )

  dm_add_pk(aangeboden_nfo_opleidingen, AANGEBODEN_OPLEIDINGCODE) |>
  dm_add_pk(mbo_onderwijslicenties, LICENTIECODE) |>
  dm_add_pk(vestigingserkenningen, VESTIGINGSCODE) |>
  dm_add_pk(onderwijsinstellingserkenningen, OIE_CODE) |>
  dm_add_pk(vestigingslicenties, LICENTIECODE) |>
  dm_add_pk(financieringsbesluiten, OIE_CODE) |>
  dm_add_pk(aangeboden_vo_opleidingen, AANGEBODEN_OPLEIDINGCODE) |>
  dm_add_pk(bevoegd_gezag_erkenningen, BGE_CODE) |>
  dm_add_pk(ho_opleidingserkenningen, UNIEKE_ERKENDEOPLEIDINGSCODE) |>
  # Foreign keys
  dm_add_fk()
datasets


datasets_dm_pk |> dm_draw()
