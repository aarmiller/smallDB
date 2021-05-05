#' Collect core data
#'
#' collect data from the core table
#'
#' @param setting inpatient or outpatient
#' @param source ccae or mdcr
#' @param year year (as integer value)
#' @param vars a vector of variables to collect. If NULL all variables in a table will be returned
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#' @return A tibble of variables from the respective core table
#'
#' @export
get_core_data <- function(setting,source,year,vars=c(),db_con,collect_n=Inf){
  checkmate::assert_choice(setting, c("inpatient", "outpatient"))
  checkmate::assert_choice(source, c("ccae", "mdcr"))

  tbl_name <- glue::glue("{setting}_core_{source}_{year}")
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars()

  if (is.null(vars)){
    get_vars <- get_vars
  } else {
    get_vars <- vars[vars %in% get_vars]
  }

  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::select(get_vars) %>%
    dplyr::collect(n=collect_n)

  return(out)
}


#' Get RX visits for a set of NDC codes
#'
#' This function collects dates for RX fills
#'
#' @importFrom rlang .data
#'
#' @param source ccae or mdcr
#' @param year year (as integer value)
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
get_rx_dates <- function(source,year,db_con,collect_n=Inf){

  checkmate::assert_choice(source, c("ccae", "mdcr"))

  tbl_name <- glue::glue("rx_core_{source}_{year}")

  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::select("enrolid","svcdate") %>%
    dplyr::distinct() %>%
    dplyr::collect(n=collect_n)

  return(out)
}
