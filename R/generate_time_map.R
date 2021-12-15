
#' Build time_map keys
#'
#' This function goes to the database and assembles all of the inpatient an outpatient visits
#' (or just the visits specified by a collection table). It then assembles them into a logintudinal
#' time_map of distinct visits (defined by admission/service date and stdplac). Finally, each
#' visit is given a distinct visit key. The time_map and the distinct inpatient and outpatient visit keys
#' are returned as a list. NOTE: This function should generally only be run once after the small DB is
#' build, and it generally should not be called directly. Use the function `add_time_map_keys()` to add the
#' time_map keys to the database.
#'
#' @importFrom rlang .data
#'
#' @param collect_tab a collection table to build time_map from. By default all years will be used
#' @param db_con a connection to a database
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'

build_time_map_keys <- function(collect_tab=collect_table(), db_con){

  # Collect Inpatient Visits
  if (any(collect_tab$setting=="inpatient")){
    # get inpatient visits
    temp.in <- collect_tab %>%
      dplyr::filter(.data$setting=="inpatient") %>%
      gether_core_data(vars = c("caseid","enrolid","admdate","disdate","los"),db_con = db_con)

    # clean inpatient visits
    temp.in <- temp.in %>%
      dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                         ifelse(source=="mdcr",2L,3L))) %>%
      dplyr::select("year","source_type","core_data") %>%
      tidyr::unnest(cols = c(core_data)) %>%
      dplyr::mutate(disdate=ifelse(is.na(.data$disdate),.data$admdate+.data$los,.data$disdate),
                    setting_type=5L) %>%
      dplyr::select(-.data$los)

  } else {
    temp.in <- tibble::tibble(year = character(),
                              source_type = integer(),
                              caseid = integer(),
                              enrolid = integer(),
                              admdate = integer(),
                              disdate = integer(),
                              setting_type = integer())
  }

  # collect outpatient visits
  if (any(collect_tab$setting=="outpatient")) {
    # get outpatient visits
    temp.out <- collect_tab %>%
      dplyr::filter(.data$setting=="outpatient") %>%
      gether_core_data(vars = c("enrolid", "stdplac", "svcdate", "stdprov",
                                "svcscat", "procgrp", "revcode", "proc1"),
                       db_con = db_con) %>%
      dplyr::mutate(core_data=purrr::map(.data$core_data,~dplyr::distinct(.)))

    # clean outpatient visits
    temp.out <- temp.out %>%
      dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                         ifelse(source=="mdcr",2L,3L))) %>%
      dplyr::select("year","source_type","core_data") %>%
      mutate(core_data = map(core_data, ~mutate(., procgrp = as.character(procgrp),
                                                stdprov = as.character(stdprov)))) %>%
      tidyr::unnest(cols = c(core_data)) %>%
      dplyr::mutate(disdate = .data$svcdate,
                    admdate = .data$svcdate,
                    setting_type = 1L) %>%
      dplyr::select(-.data$svcdate)
    
    # Identify visits
    temp.out <-  add_ed_obs_indicator(temp.out)
    
  } else {
    temp.out <- tibble::tibble(year = character(),
                               source_type = integer(),
                               enrolid = integer(),
                               stdplac = integer(),
                               admdate = integer(),
                               disdate = integer(),
                               setting_type = integer())
  }

  # Collect RX visits
  if (any(collect_tab$setting=="rx")) {

    # get rx visits
    temp.rx <- collect_tab %>%
      dplyr::filter(.data$setting=="rx") %>%
      gether_rx_dates(db_con = db_con)

    # clean rx visits
    temp.rx <- temp.rx %>%
      dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                         ifelse(source=="mdcr",2L,3L))) %>%
      dplyr::select("year","source_type","rx_data") %>%
      tidyr::unnest(cols = c(rx_data)) %>%
      dplyr::mutate(disdate = .data$svcdate,
                    admdate = .data$svcdate,
                    setting_type = 4L) %>%
      dplyr::select(-.data$svcdate)

  } else {

    temp.rx <- tibble::tibble(year = character(),
                              source_type = integer(),
                              enrolid = integer(),
                              stdplac = integer(),
                              admdate = integer(),
                              disdate = integer(),
                              setting_type = integer())
  }

  # assemble time_map
  temp_time_map <- dplyr::bind_rows(temp.in,temp.out,temp.rx) %>%
    dplyr::arrange(.data$enrolid, .data$admdate,.data$setting_type) %>%
    dplyr::distinct() %>% 
    dplyr::mutate(key=dplyr::row_number()) 
  
  
  # get distinct outpatient keys
  out_keys <- temp_time_map %>%
    dplyr::filter(.data$setting_type %in% 1:3) %>%
    dplyr::select("year","source_type","enrolid","stdplac", "setting_type",
                  "svcdate"="admdate","key")
  
  # get distinct inpatient keys
  in_keys <- temp_time_map %>%
    dplyr::filter(.data$setting_type==5) %>%
    dplyr::select("year","source_type","enrolid","admdate",
                  "disdate","caseid","setting_type","key")
  
  # get distinct rx keys
  rx_keys <- temp_time_map %>%
    dplyr::filter(.data$setting_type==4) %>%
    dplyr::select("year","source_type","enrolid","svcdate"="admdate","key", "setting_type")

  return(list(time_map = temp_time_map,
              out_keys = out_keys,
              in_keys = in_keys,
              rx_keys = rx_keys))
}

#' Add time_map keys to remote database
#'
#' This function adds time_map keys, created by `build_time_map_keys()` to a remote database. Keys
#' can be added temporarily and/or overwritten. This function will first check if outpatient or inpatient
#' keys already exist, and if overwite is set to FALSE it will not proceed. NOTE: This function generally
#' only needs to be run once, after the mini database has been constructed.
#'
#' @param collect_tab a collection table to build time_map from. By default all years will be used
#' @param db_con a connection to a database
#' @param overwrite a logical indicator. If TRUE the existing inpatient_keys and/or outpatient_keys tables
#' will be overwritten
#' @param temporary a logical indicator. If TRUE the tables will only be added temporarilly to the database.
#' @return Nothing will be returned, but tables may be added to the remote database
#' @export
#'
add_time_map_keys <- function(collect_tab=collect_table(), db_con, overwrite=FALSE, temporary=TRUE){

  if (any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys","rx_keys")) & overwrite==FALSE){
    warning("Database contains keys and overwrite set to FALSE")
  } else {
    temp_keys <- build_time_map_keys(collect_tab = collect_tab,
                                     db_con = db_con)

    dplyr::copy_to(dest=db_con,
                   df=temp_keys$out_keys,
                   name = "outpatient_keys",
                   temporary = temporary,
                   overwrite = overwrite)

    dplyr::copy_to(dest=db_con,
                   df=temp_keys$in_keys,
                   name = "inpatient_keys",
                   temporary = temporary,
                   overwrite = overwrite)

    dplyr::copy_to(dest=db_con,
                   df=temp_keys$rx_keys,
                   name = "rx_keys",
                   temporary = temporary,
                   overwrite = overwrite)

  }
}

#' Build longitudinal timemap from small db
#'
#' This function makes a time_map from the inpatient and outpatient visit keys,
#' contained in a small database. IF keys are not found in the database, optional
#' arguments allow keys to be generated temporarily (NOTE: this function cannot
#' be used to permanently add keys to the database, to permanently add keys use,
#' `add_time_map_keys()`).
#'
#' @importFrom rlang .data
#'
#' @param db_con connection to the small database
#' @param collect_tab (optional) a collection table. This argument is only used to make temporary,
#' visit keys if no keys are found in the database
#' @return a tibble containing the timemap
#' @export
#'
build_time_map <- function(db_con,collect_tab=collect_table()){
  if (!any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys"))){
    warning("Database contains no visit keys. Temporary visit keys were generated using the collection table specified.")
    add_time_map_keys(collect_tab=collect_tab, db_con = db_con,temporary = TRUE)
  }

  dat <- rbind(db_con %>%
                 dplyr::tbl("outpatient_keys") %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(disdate=.data$svcdate) %>%
                 dplyr::select("key","year","source_type","enrolid","admdate"="svcdate",
                               "disdate","stdplac", "setting_type"),

               db_con %>%
                 dplyr::tbl("rx_keys") %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(disdate=.data$svcdate,
                               stdplac=-2L) %>%
                 dplyr::select("key","year","source_type","enrolid","admdate"="svcdate",
                               "disdate","stdplac","setting_type"),

               db_con %>%
                 dplyr::tbl("inpatient_keys") %>%
                 dplyr::select(-.data$caseid) %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(stdplac=-1L) %>%
                 dplyr::select(.data$key,dplyr::everything())) %>%
    dplyr::arrange(.data$enrolid, .data$admdate, .data$setting_type)

  return(dat)
}
