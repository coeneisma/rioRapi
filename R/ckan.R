library(httr2)

req <- request("https://onderwijsdata.duo.nl/api/3/action")


# list of ckan-objects ----------------------------------------------------

groups <- req |>
  req_url_path_append("group_list") |>
  req_headers("Accept" = "application/json") |>
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)

packages <- req |>
  req_url_path_append("package_list") |>
  req_headers("Accept" = "application/json") |>
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)

tags <- req |>
  req_url_path_append("tag_list") |>
  req_headers("Accept" = "application/json") |>
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)




# JSON representation of objects ------------------------------------------

#' Hier staan alle objecten van RIO in.

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

# Alle RIO-datasets-metadata
rio_package$result$resources |> View()

# Meta-data over RIO zelf
rio_package$result$extras |> View()


# JSON presentatie van dataset --------------------------------------------

onderwijslocaties <- req |>
  req_url_path_append("datastore_search") |>
  req_method("POST") |>
  req_headers("Accept" = "application/json") |>
  req_body_json(list(
    resource_id = "a7e3f323-6e46-4dca-a834-369d9d520aa8",
    # limit = 10,
    # q = "koudum",
    # q = list("PLAATSNAAM" = "Rotterdam")
    # Met de filters kun je dus ook een lijst met id's doorgeven!
    filters = list("PLAATSNAAM" = c("Rotterdam", "Capelle aan den IJssel", "Delft"))
    )) |>  # resource_id en eventueel limit toevoegen
  # req_url()
  # req_dry_run()
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)

onderwijslocaties$result$records |> View()

# Plotten op de kaart
onderwijslocaties$result$records |>
  sf::st_as_sf(coords = c("GPS_LONGITUDE", "GPS_LATITUDE"), crs = 4326) |>
  leaflet() |>
  leaflet::addTiles() |>
  # leaflet::addCircleMarkers()
  leaflet::addCircles()

# Vestigingserkenningen ---------------------------------------------------



vestigingserkenningen <- req |>
  req_url_path_append("datastore_search") |>
  req_method("POST") |>
  req_headers("Accept" = "application/json") |>
  req_body_json(list(
    resource_id = "01fd2a5f-40af-456f-864d-13265a51e5e2",
    limit = 10
    # q = "koudum",
    # q = list("PLAATSNAAM" = "Rotterdam")
    # Met de filters kun je dus ook een lijst met id's doorgeven!
    # filters = list("PLAATSNAAM" = c("Rotterdam", "Capelle aan den IJssel", "Delft"))
  )) |>  # resource_id en eventueel limit toevoegen
  # req_url()
  # req_dry_run()
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)

vestigingserkenningen$result$records |> View()
vestigingserkenningen$result$fields


# aangeboden_ho_opleidingen -----------------------------------------------


aangeboden_ho_opleidingen <- req |>
  req_url_path_append("datastore_search") |>
  req_method("POST") |>
  req_headers("Accept" = "application/json") |>
  req_body_json(list(
    resource_id = "c0e73372-076d-46ac-b5fd-9d27e9f154f6",
    limit = 10
    # q = "koudum",
    # q = list("PLAATSNAAM" = "Rotterdam")
    # Met de filters kun je dus ook een lijst met id's doorgeven!
    # filters = list("PLAATSNAAM" = c("Rotterdam", "Capelle aan den IJssel", "Delft"))
  )) |>  # resource_id en eventueel limit toevoegen
  # req_url()
  # req_dry_run()
  req_perform() |>
  resp_body_json(simplifyVector = TRUE)

aangeboden_ho_opleidingen$result$records |> View()
