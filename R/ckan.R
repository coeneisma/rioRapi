
req <- request("https://onderwijsdata.duo.nl/api/3/action")

groups <- req |>
  req_url_path_append("group_list") |>
  req_headers("Accept" = "application/json") |>
  req_perform()
  # req_dry_run()

groups_data <- groups |>
  resp_body_json(simplifyVector = TRUE)


# JSON presentatie van dataset --------------------------------------------

opleidingen <- req |>
  req_url_path_append("datastore_search") |>
  req_method("POST") |>
  req_headers("Accept" = "application/json") |>
  req_body_json(list(
    resource_id = "a7e3f323-6e46-4dca-a834-369d9d520aa8",
    # limit = 10,
    # q = "koudum",
    q = list("PLAATSNAAM" = "Rotterdam")
    # sort = "PLAATSNAAM"
    # filters = list("PLAATSNAAM" = "Rotterdam")
    )) |>  # resource_id en eventueel limit toevoegen
  # req_url()
  # req_dry_run()
  req_perform()

opleidingen_data <- opleidingen |> resp_body_json(simplifyVector = TRUE)

opleidingen_data$result$records |> View()
