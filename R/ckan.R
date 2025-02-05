
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
  req_url_path_append("package_show") |>
  # req_method("POST") |>
  req_headers("Accept" = "application/json") |>
  # req_headers("resource_id" = "a7e3f323-6e46-4dca-a834-369d9d520aa8") |>
  req_body_json(
    list(
      resource_id = "a7e3f323-6e46-4dca-a834-369d9d520aa8")
    ) |>
  req_dry_run()
  req_perform()

