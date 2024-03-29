
#' Build time_map keys (in parallel)
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
#' @param db_con a connection to a database#' 
#' @param db_path Path to the database
#' @param num_cores The number of worker cores to use. If not specified will use 10. If NULL is specified then it will determine the number of cores based on the which ever
#' is the smallest value between number of rows in for collect_tab or detected number of cores - 1
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'

build_time_map_keys_parallel <- function(collect_tab=collect_table(), db_path,
                                         db_con,
                                         num_cores = NULL){

  # Collect Inpatient Visits
  if (any(collect_tab$setting=="inpatient")){
    # get inpatient visits
    temp.in <- collect_tab %>%
      dplyr::filter(.data$setting=="inpatient") %>%
      gether_core_data_parallel(vars = c("caseid","enrolid","admdate","disdate","los"),
                                db_path = db_path, 
                                num_cores = num_cores)

    # clean inpatient visits
    temp.in <- temp.in %>%
      dplyr::mutate(ccae=ifelse(source=="ccae",1L,0L)) %>%
      dplyr::select("year","ccae","core_data") %>%
      tidyr::unnest(cols = c(core_data)) %>%
      dplyr::mutate(disdate=ifelse(is.na(.data$disdate),.data$admdate+.data$los,.data$disdate),
                    inpatient = 1L,
                    source = "inpatient",
                    rx = 0L) %>%
      dplyr::select(-.data$los)

  } else {
    temp.in <- tibble::tibble(year = character(),
                              ccae = integer(),
                              caseid = integer(),
                              enrolid = integer(),
                              source = character(),
                              admdate = integer(),
                              disdate = integer(),
                              inpatient = integer(),
                              rx = integer())
  }

  # collect outpatient visits
  if (any(collect_tab$setting=="outpatient")) {
    # get outpatient visits
    temp.out <- collect_tab %>%
      dplyr::filter(.data$setting=="outpatient") %>%
      gether_core_data_parallel(vars = c("enrolid", "stdplac", "svcdate", "stdprov",
                                "svcscat", "procgrp", "revcode", "proc1"),
                                db_path = db_path, 
                                num_cores = num_cores) %>%
      dplyr::mutate(core_data=purrr::map(.data$core_data,~dplyr::distinct(.)))

    # clean outpatient visits
    temp.out <- temp.out %>%
      dplyr::mutate(ccae=ifelse(source=="ccae",1L,0L)) %>%
      dplyr::select("year","ccae","core_data") %>%
      mutate(core_data = map(core_data, ~mutate(., procgrp = as.character(procgrp),
                                                stdprov = as.character(stdprov)))) %>%
      tidyr::unnest(cols = c(core_data)) %>%
      dplyr::mutate(disdate = .data$svcdate,
                    admdate = .data$svcdate,
                    inpatient = 0L,
                    rx = 0L) %>%
      dplyr::select(-.data$svcdate)
    
    # Identify visits
    temp.out <-  add_ed_indicator(temp.out)
    
  } else {
    temp.out <- tibble::tibble(year = character(),
                               ccae = integer(),
                               enrolid = integer(),
                               stdplac = integer(),
                               source = character(),
                               admdate = integer(),
                               disdate = integer(),
                               inpatient = integer(),
                               rx = integer())
  }

  # Collect RX visits
  if (any(collect_tab$setting=="rx")) {

    # get rx visits
    temp.rx <- collect_tab %>%
      dplyr::filter(.data$setting=="rx") %>%
      gether_rx_dates(db_con = db_con)

    # clean rx visits
    temp.rx <- temp.rx %>%
      dplyr::mutate(ccae=ifelse(source=="ccae",1L,0L)) %>%
      dplyr::select("year","ccae","rx_data") %>%
      tidyr::unnest(cols = c(rx_data)) %>%
      dplyr::mutate(disdate = .data$svcdate,
                    admdate = .data$svcdate,
                    source = "rx",
                    inpatient = 0L,
                    rx = 1L) %>%
      dplyr::select(-.data$svcdate)

  } else {

    temp.rx <- tibble::tibble(year = character(),
                              ccae = integer(),
                              enrolid = integer(),
                              stdplac = integer(),
                              source = character(),
                              admdate = integer(),
                              disdate = integer(),
                              inpatient = integer(),
                              rx = integer())
  }

  # assemble time_map
  temp_time_map <- dplyr::bind_rows(temp.in,temp.out,temp.rx) %>%
    dplyr::arrange(.data$enrolid, .data$admdate,.data$inpatient, .data$rx) %>%
    dplyr::mutate(key=dplyr::row_number()) %>% 
    dplyr::mutate_at(vars(inpatient, rx),~ifelse(is.na(.),0L,.))

  # get distinct outpatient keys
  out_keys <- temp_time_map %>%
    dplyr::filter(.data$inpatient==0 & .data$rx==0) %>%
    dplyr::select("year","ccae","enrolid","stdplac", "source",
                  "svcdate"="admdate","key")

  # get distinct inpatient keys
  in_keys <- temp_time_map %>%
    dplyr::filter(.data$inpatient==1) %>%
    dplyr::select("year","ccae","enrolid","admdate", "source",
                  "disdate","caseid","key")

  # get distinct rx keys
  rx_keys <- temp_time_map %>%
    dplyr::filter(.data$inpatient==0 & .data$rx==1) %>%
    dplyr::select("year","ccae","enrolid","svcdate"="admdate","key", "source",)

  return(list(time_map = temp_time_map,
              out_keys = out_keys,
              in_keys = in_keys,
              rx_keys = rx_keys))
}

#' Add time_map keys to remote database (in parallel)
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
#' @param db_path Path to the database
#' @param num_cores The number of worker cores to use. If not specified will use 10. If NULL is specified then it will determine the number of cores based on the which ever
#' is the smallest value between number of rows in for collect_tab or detected number of cores - 1
#' @param temporary a logical indicator. If TRUE the tables will only be added temporarilly to the database.
#' @return Nothing will be returned, but tables may be added to the remote database
#' @export
#'
add_time_map_keys_parallel <- function(collect_tab=collect_table(), db_path, db_con,
                              num_cores = NULL, overwrite=FALSE, temporary=TRUE){

  if (any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys","rx_keys")) & overwrite==FALSE){
    warning("Database contains keys and overwrite set to FALSE")
  } else {
    temp_keys <- build_time_map_keys_parallel(collect_tab = collect_tab,
                                              db_path = db_path,
                                              num_cores = num_cores,
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

#' Build longitudinal timemap from small db (in parallel)
#'
#' This function makes a time_map from the inpatient and outpatient visit keys,
#' contained in a small database. IF keys are not found in the database, optional
#' arguments allow keys to be generated temporarily (NOTE: this function cannot
#' be used to permanently add keys to the database, to permanently add keys use,
#' `add_time_map_keys_parallel()`).
#'
#' @importFrom rlang .data
#'
#' @param db_con connection to the small database
#' @param collect_tab (optional) a collection table. This argument is only used to make temporary,
#' visit keys if no keys are found in the database
#' @param db_path Path to the database
#' @param num_cores The number of worker cores to use. If not specified will use 10. If NULL is specified then it will determine the number of cores based on the which ever
#' is the smallest value between number of rows in for collect_tab or detected number of cores - 1
#' @return a tibble containing the timemap
#' @export
#'
build_time_map_parallel <- function(collect_tab=collect_table(), db_path, db_con,
                           num_cores = 10){
  if (!any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys"))){
    warning("Database contains no visit keys. Temporary visit keys were generated using the collection table specified.")
    add_time_map_keys_parallel(collect_tab=collect_tab,db_con = db_con,
                               db_path = db_path, 
                               num_cores = num_cores,
                               temporary = TRUE)
  }

  dat <- rbind(db_con %>%
                 dplyr::tbl("outpatient_keys") %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(disdate=.data$svcdate,
                               inpatient=0L,
                               rx=0L) %>%
                 dplyr::select("key","year","ccae","enrolid","admdate"="svcdate",
                               "disdate","inpatient","rx","stdplac", "source"),

               db_con %>%
                 dplyr::tbl("rx_keys") %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(disdate=.data$svcdate,
                               inpatient=0L,
                               stdplac=-2L,
                               rx=1L) %>%
                 dplyr::select("key","year","ccae","enrolid","admdate"="svcdate",
                               "disdate","inpatient","rx","stdplac", "source"),

               db_con %>%
                 dplyr::tbl("inpatient_keys") %>%
                 dplyr::select(-.data$caseid) %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(inpatient=1L,
                               stdplac=-1L,
                               rx=0L) %>%
                 dplyr::select(.data$key,dplyr::everything())) %>%
    dplyr::arrange(.data$enrolid, .data$admdate, .data$inpatient, .data$rx)

  return(dat)
}

