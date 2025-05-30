% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/api.R
\name{rio_api_connection}
\alias{rio_api_connection}
\title{Create a connection to the RIO CKAN API}
\usage{
rio_api_connection(base_url = "https://onderwijsdata.duo.nl/api/3/action")
}
\arguments{
\item{base_url}{The base URL for the RIO CKAN API.
Default is "https://onderwijsdata.duo.nl/api/3/action".}
}
\value{
A request object configured for the RIO CKAN API.
}
\description{
This function creates a request object to connect to the Dutch Register of
Institutions and Programs (RIO) CKAN API.
}
\examples{
\dontrun{
rio_conn <- rio_api_connection()
}

}
\keyword{internal}
