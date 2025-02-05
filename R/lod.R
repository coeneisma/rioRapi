library(httr2)
library(jsonlite)
library(rapiclient)

req <- request("https://lod.onderwijsregistratie.nl/api/rio/v2/")



opleidingen <- req |>
  req_url_path("opleidingen") |>
  req_headers("Accept" = "application/json") |>
  req_perform()
  # req_dry_run()


opleidingen_data <- opleidingen |>
  resp_body_json()
  # resp_body_json(simplifyVector = TRUE)
  # resp_body_html()
