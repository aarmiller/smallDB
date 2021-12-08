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
  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))

  tbl_name <- glue::glue("{setting}_core_{source}_{year}")
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars() %>% as.vector()

  if (is.null(vars)){
    get_vars <- get_vars
  } else {
    get_vars <- vars[vars %in% get_vars]
  }

  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::select(all_of(get_vars)) %>%
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

  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))

  tbl_name <- glue::glue("rx_core_{source}_{year}")

  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::select("enrolid","svcdate") %>%
    dplyr::distinct() %>%
    dplyr::collect(n=collect_n)

  return(out)
}


#' Get Procedure visits for a set of procedure codes
#'
#' This function collects dates for procedures
#'
#' @importFrom rlang .data
#'
#' @param source ccae, mdcr or medicaid
#' @param year year
#' @param proc_codes vector of procedure codes to pull visits for
#' @param setting "inpatient" or "outpatient"
#' @param tbl_vars names of variables to pull from corresponding tables. If NULL all tables
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
get_proc_dates <- function(source,year,proc_codes,setting = "inpatient",tbl_vars=NULL,db_con,collect_n=Inf){
  
  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  checkmate::assert_choice(setting, c("inpatient", "outpatient"))
  checkmate::assertVector(proc_codes)
  
  if (setting=="inpatient"){
    tbl_name <- glue::glue("inpatient_proc_{source}_{year}") 
  } else {
    tbl_name <- glue::glue("outpatient_core_{source}_{year}")
  }
  
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars() %>% as.vector()
  
  if (is.null(tbl_vars)){
    get_vars <- get_vars
  } else {
    get_vars <- tbl_vars[tbl_vars %in% get_vars]
  }
  
  out <- db_con %>%
    dplyr::tbl(tbl_name) 
  
  if (setting == "inpatient"){
    out <- out %>% 
      dplyr::filter(.data$proc %in% proc_codes) %>%
      dplyr::select(all_of(get_vars)) %>%
      dplyr::collect(n=collect_n) 
    
  } else {
    out <- out %>% 
      dplyr::filter(.data$proc1 %in% proc_codes) %>%
      dplyr::select(all_of(get_vars)) %>%
      dplyr::collect(n=collect_n) 
  }
  
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
#' all variables in the rx table are returned. Default is rx_vars = NULL
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
get_rx_data <- function(source,year,ndc_codes,rx_vars=NULL,db_con,collect_n=Inf){

  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  checkmate::assertVector(ndc_codes)

  tbl_name <- glue::glue("rx_core_{source}_{year}")
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars() %>% as.vector()

  if (is.null(rx_vars)){
    get_vars <- get_vars
  } else {
    get_vars <- rx_vars[rx_vars %in% get_vars]
  }

  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::filter(.data$ndcnum %in% ndc_codes) %>%
    dplyr::select(all_of(get_vars)) %>%
    dplyr::collect(n=collect_n)

  return(out)

}

#' Collect table data
#'
#' Collect data from a generic table
#'
#' @param table table to collect variables from
#' @param source ccae, mdcr or medicaid
#' @param year year (as integer value)
#' @param vars a vector of variables to collect. If NULL all variables in a table will be returned
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#' @return A tibble of variables from the respective table
#'
#' @export
get_table_data <- function(table,source,year,vars=NULL,db_con,collect_n=Inf){
  
  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  
  tbl_name <- glue::glue("{table}_{source}_{year}")
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars() %>% as.vector()
  
  if (is.null(vars)){
    get_vars <- get_vars
  } else {
    get_vars <- vars[vars %in% get_vars]
  }
  
  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::select(all_of(get_vars)) %>%
    dplyr::collect(n=collect_n)
  
  return(out)
}

#' Collect inpatient diagnosis visits
#'
#' Collect inpatient visits for a particular diagnosis along with the diagnosis order
#'
#' @param source ccae, mdcr or medicaid
#' @param year year (as integer value)
#' @param dx_list A named list of icd9 and icd10 codes, with names of "icd9_codes" and
#' "icd10_codes", respectively
#' @param dx_num a logical indicator of whether to collect diagnosis number. Default is TRUE.
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#' @return A tibble of variables from the respective table
#'
#' @export
get_inpatient_dx_visits <- function(source,year,dx_list,dx_num=TRUE,db_con,collect_n=Inf){

checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))

icd9_codes <- dx_list %>% 
  purrr::map(~.$icd9_codes) %>% 
  unlist(use.names = FALSE) %>% 
  unique()

icd10_codes <- dx_list %>% 
  purrr::map(~.$icd10_codes) %>% 
  unlist(use.names = FALSE) %>% 
  unique()

if (as.integer(year)<=14){
  
  tbl_name <- glue::glue("inpatient_dx_{source}_{year}")
  
  out <- db_con %>%
    dplyr::tbl(tbl_name) %>%
    dplyr::filter(dx %in% icd9_codes) %>% 
    dplyr::collect(n=collect_n)
  
} else {
  
  tbl_name1 <- glue::glue("inpatient_dx9_{source}_{year}")
  tbl_name2 <- glue::glue("inpatient_dx10_{source}_{year}")
  
  out1 <- db_con %>%
    dplyr::tbl(tbl_name1) %>%
    dplyr::filter(dx %in% icd9_codes) %>%
    dplyr::collect(n=collect_n)
  
  out2 <- db_con %>%
    dplyr::tbl(tbl_name2) %>%
    dplyr::filter(dx %in% icd10_codes) %>%
    dplyr::collect(n=collect_n)
  
  out <- rbind(out1,out2)
  
  
}

if (dx_num == FALSE){
  out <- out %>% 
    dplyr::select(caseid,dx) %>% 
    dplyr::distinct()
}

return(out)
}


#' Get Procedure visits from facility tables for a set of procedure codes
#'
#' This function collects dates for procedures
#'
#' @importFrom rlang .data
#'
#' @param source ccae, mdcr or medicaid
#' @param year year
#' @param proc_codes vector of procedure codes to pull visits for
#' @param tbl_vars names of variables to pull from corresponding tables. If NULL date and enrolid will be collected
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
get_facility_procs <- function(source,year,proc_codes,tbl_vars=NULL,db_con,collect_n=Inf){
  
  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  checkmate::assertVector(proc_codes)
  
  tbl_name <- glue::glue("facility_proc_{source}_{year}")
  
  if (year=="01") {return(NULL)}
  
  get_vars <- dplyr::tbl(db_con,tbl_name) %>% dplyr::tbl_vars() %>% as.vector()
  
  if (is.null(tbl_vars)){
    get_vars <- c("enrolid","proc","svcdate")
  } else {
    get_vars <- tbl_vars[tbl_vars %in% get_vars]
  }
  
  out <- db_con %>%
    dplyr::tbl(tbl_name) 
  
    out <- out %>%
      dplyr::filter(.data$proc %in% proc_codes) %>% 
      dplyr::select(all_of(get_vars)) %>%
      dplyr::collect(n=collect_n)
  
  return(out)
  
}



#' Collect all inpatient diagnosis visits
#'
#' Collect all inpatient visits along with the diagnosis order
#'
#' @param source ccae, mdcr or medicaid
#' @param year year (as integer value)
#' @param dx_num a logical indicator of whether to collect diagnosis number. Default is TRUE.
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#' @param keys what keys to return diagnoses for
#' @return A tibble of variables from the respective table
#'
#' @export
get_all_inpatient_dx_visits <- function(source,year,dx_num=TRUE,db_con,collect_n=Inf,keys){
  
  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  
  if (as.integer(year)<=14){
    
    tbl_name <- glue::glue("inpatient_dx_{source}_{year}")
    
    out <- db_con %>%
      dplyr::tbl(tbl_name) %>%
      dplyr::collect(n=collect_n)
    
  } else {
    
    tbl_name1 <- glue::glue("inpatient_dx9_{source}_{year}")
    tbl_name2 <- glue::glue("inpatient_dx10_{source}_{year}")
    
    out1 <- db_con %>%
      dplyr::tbl(tbl_name1) %>%
      dplyr::collect(n=collect_n)
    
    out2 <- db_con %>%
      dplyr::tbl(tbl_name2) %>%
      dplyr::collect(n=collect_n)
    
    out <- rbind(out1,out2)
    
    
  }
  
  if (dx_num == FALSE){
    out <- out %>% 
      dplyr::select(caseid,dx) %>% 
      dplyr::distinct()
  }
  
  return(out)
}






