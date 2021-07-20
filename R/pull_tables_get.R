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

#' Get RX visits for a set of NDC codes
#'
#' This function collects records that correspond to a set of NDC codes
#'
#' @importFrom rlang .data
#'
#' @param source ccae or mdcr
#' @param year year (as integer value)
#' @param ndc_codes a vector of ndc codes to lookup
#' @param rx_vars variables to collect from the rx table. If no variable names are provided
#' all variables in the rx table are returned. Default is rx_vars = c()
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
get_rx_data <- function(source,year,ndc_codes,rx_vars=NULL,db_con,collect_n=Inf){

  checkmate::assert_choice(source, c("ccae", "mdcr"))
  checkmate::assertVector(ndc_codes)

  tbl_name <- glue::glue("rx_core_{source}_{year}")
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars()

  if (is.null(rx_vars)){
    get_vars <- get_vars
  } else {
    get_vars <- rx_vars[rx_vars %in% get_vars]
  }

  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::filter(.data$ndcnum %in% ndc_codes) %>%
    dplyr::select(get_vars) %>%
    dplyr::collect(n=collect_n)

  return(out)

}
