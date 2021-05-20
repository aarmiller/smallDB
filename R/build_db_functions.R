



#### Get DX Functions ####

#' Get Diagnosis Dates for List of ICD-9 diagnoses
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param dx_list vector of diagnoses to pull visits for
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
get_dx9_dates <- function(setting,source,year,dx_list,con,collect_n=10){
  if (setting=="inpatient") {
    if (as.integer(year)<15){
      if (as.integer(year)==1){
        dat <- dplyr::tbl(con,paste0(setting,"_dx_",db,"_",year)) %>%
          dplyr::filter(dx %in% dx_list) %>%
          dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",db,"_",year)) %>%
                       dplyr::mutate(disdate=admdate+los) %>%
                       dplyr::select(caseid,enrolid,admdate,disdate),
                     by="caseid") %>%
          dplyr::collect(n=collect_n) %>%
          dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
      } else {
        dat <- dplyr::tbl(con,paste0(setting,"_dx_",source,"_",year)) %>%
          dplyr::filter(dx %in% dx_list) %>%
          dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                       dplyr::select(caseid,enrolid,admdate,disdate),
                     by="caseid") %>%
          dplyr::collect(n=collect_n) %>%
          dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
      }
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx9_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                            dplyr::select(caseid,enrolid,admdate,disdate),
                   by="caseid") %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }
  } else {
    if (year<15){
      dat <- dplyr::tbl(con,paste0(setting,"_dx_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx9_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }

  }
  return(dat)
}



#' Get Diagnosis Dates for List of ICD-10 diagnoses
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param dx_list vector of diagnoses to pull visits for
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
get_dx10_dates <- function(setting,source,year,dx_list,con,collect_n=10){
  if (setting=="inpatient") {
    if (year<15){
      dat <- tibble::tibble(caseid = integer(),
                            dx = character(),
                            dx_num = integer(),
                            enrolid = bit64::integer64(),
                            admdate = integer(),
                            disdate = integer())
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx10_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                            dplyr::select(caseid,enrolid,admdate,disdate),
                   by="caseid") %>%
        dplyr::collect(n=collect_n) %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }
  } else {
    if (year<15){
      dat <- tibble::tibble(seqnum_o = integer(),
                            enrolid = bit64::integer64(),
                            svcdate = integer(),
                            dx = character(),
                            dx_num = integer())
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx10_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid = bit64::as.integer64(enrolid))
    }
  }
  return(dat)
}

#' Get Diagnosis Dates for List of ICD-9 and ICD-10 diagnoses
#'
#' This function is a wrapper around `get_dx9_dates()` and `get_dx10_dates()`
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param dx9_list vector of ICD-9 diagnoses to pull visits for
#' @param dx10_list vector of ICD-10 diagnoses to pull visits for
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
get_dx_dates <- function(setting,source,year,dx9_list,dx10_list,con,
                         collect_n=10){

  dplyr::bind_rows(get_dx9_dates(setting = setting,
                                 source = source,
                                 year = year,
                                 dx_list = dx9_list,
                                 con = con,
                                 collect_n = collect_n),
                   get_dx10_dates(setting = setting,
                                  source = source,
                                  year = year,
                                  dx_list = dx10_list,
                                  con = con,
                                  collect_n = collect_n))
}

#### Pull DB functions ####

#' Get Linked Visits (All)
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param enrolid_table a table containing enrolids to pull data for
#' @param con a database connection
#' @param collect_n number of rows to collect
#' @param HCUP a logical indicator of whether collecting data from HCUP (vs Truven)
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'
get_linked_visits_all <- function(setting,source,year,enrolid_table,con,
                                  collect_n=10,HCUP=FALSE){
  if (HCUP==FALSE){
    if (setting=="inpatient"){
      dat <- dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
        dplyr::inner_join(enrolid_table,by="enrolid",copy=TRUE) %>%
        dplyr::collect(n=collect_n) %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
        dplyr::inner_join(enrolid_table,by="enrolid",copy=TRUE) %>%
        dplyr::collect(n=collect_n) %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }
  } else {
    # con <- con[[tolower(setting)]]
    # dat <- tbl(con,paste0(setting,"_",year,"_",db,"_dte")) %>%
    #   inner_join(tbl(con,enrolid_table),by="VisitLink") %>%
    #   left_join(tbl(con,paste0(setting,"_",year,"_",db,"_core")) %>%
    #               select_(.dots=c("KEY","LOS_X",get_vars)),by="KEY") %>%
    #   collect(n=collect_n)
  }
  return(dat)
}


#' Get Linked All
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param table_name name of table to pull
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param join_table a table containing enrolids (or other) to join on
#' @param join_by a vector of variables to join by in the join table
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'
get_linked_all_truven <- function(table_name,source,year,join_table,
                                  join_by="enrolid",con,collect_n=10){
  if (join_by=="enrolid"){
    dat <- dplyr::tbl(con,paste0(table_name,"_",source,"_",year)) %>%
      dplyr::inner_join(join_table,by=join_by,copy=TRUE) %>%
      dplyr::collect(n=collect_n) %>%
      dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
  } else {
    dat <- dplyr::tbl(con,paste0(table_name,"_",source,"_",year)) %>%
      dplyr::inner_join(join_table,by=join_by,copy=TRUE) %>%
      dplyr::collect(n=collect_n)
  }
  return(dat)
}
