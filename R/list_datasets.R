
list_datasets <- function(as_table = TRUE){

  if(isTRUE(as_table)){
    ckanr::package_list(as = "table")
  } else

    ckanr::package_list()


}
