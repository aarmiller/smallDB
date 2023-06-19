

#' Build collection plan
#'
#'
#' `collect_plan` scans a small db and sets up a collection tibble for holding 
#' and distributing queries across a rand of tables in the database
#'
#' @importFrom rlang .data
#'
#' @param db_con a database connection
#' 
#' @return a tibble
#' @export
collect_plan <- function(db_con){
  
  avail_data <- tibble::tibble(table = DBI::dbListTables(db_con)) %>%
    dplyr::filter(stringr::str_detect(table,"inpatient_core")) %>%
    dplyr::mutate(tmp = stringr::str_remove(table,"inpatient_core_")) %>%
    dplyr::mutate(year = as.integer(str_sub(tmp,start = -2, end = -1))) %>%
    dplyr::mutate(source = stringr::str_remove(tmp,"_[0-9]*")) %>%
    dplyr::select(year,source)
  
  medicaid_years <- dplyr::filter(avail_data,source == "medicaid") %>%
    dplyr::distinct(year) %>%
    dplyr::arrange(year) %>%
    .$year
  
  years <- dplyr::filter(avail_data,source == "ccae") %>%
    dplyr::distinct(year) %>%
    dplyr::arrange(year) %>%
    .$year
  
  dplyr::bind_rows(tibble::tibble(source = c("mdcr","ccae")) %>% 
                     dplyr::mutate(year=purrr::map(source,~years)) %>%
                     tidyr::unnest(cols = c(year)),
                   tibble::tibble(source = c("medicaid")) %>% 
                     dplyr::mutate(year=purrr::map(source,~medicaid_years)) %>%
                     tidyr::unnest(cols = c(year))) %>% 
    dplyr::mutate(year = stringr::str_pad(year,width = 2, pad = 0))
}


#' Add indicators for ED or observational stay (New for patient_id)
#'
#' This function  identifies ED visits and observational stays from outpatient 
#' visits based on specific criteria.
#'
#' @param data A tibble of outpatient keys
#' @return A tibble of outpatient key including a source indicator for ED, 
#' observational stay or outpatient
#'
#' @export
#'
add_ed_obs_indicator_new <- function(data){
  out <- data %>% 
    # ED Stay
    dplyr::mutate(setting_type = ifelse((stdplac %in% c(23) |
                                           ((stdplac %in% c(19,21,22,28)) &
                                              (stdprov %in% c("220","428"))) |
                                           ((stdplac %in% c(19,21,22,28)) &
                                              svcscat %in% c("10120","10220","10320","10420","10520",
                                                             "12220","20120","20220","21120","21220",
                                                             "22120","22320","30120","30220","30320",
                                                             "30420","30520","30620","31120","31220",
                                                             "31320","31420","31520","31620")) |
                                           (procgrp %in% c("110","111","114")) |
                                           (revcode %in% c("450","451","452","453","454",
                                                           "455","456","457","458","459")) |
                                           (proc1 %in% c("99281","99282","99283","99284","99285"))), 2L, setting_type)) %>%
    # Observational Stay
    dplyr::mutate(setting_type = ifelse(proc1 %in% c("99218", "99219", "99220", "99224", "99225", 
                                                     "99226", "99234", "99235", "99236"), 3L, setting_type)) %>% 
    dplyr::select(year, source_type, patient_id, admdate, disdate, setting_type, stdplac)
  
  return(out)
}


#' Build tm (time_map) keys (new)
#'
#' This function goes to the database and assembles all of the inpatient an outpatient visits
#' (or just the visits specified by a collection table). It then assembles them into a logintudinal
#' time_map of distinct visits (defined by admission/service date and stdplac). Finally, each
#' visit is given a distinct visit key. The time_map and the distinct inpatient and outpatient visit keys
#' are returned as a list. NOTE: This function should generally only be run once after the small DB is
#' build, and it generally should not be called directly. Use the function `add_tm_keys()` to add the
#' time_map keys to the database.
#'
#' @importFrom rlang .data
#'
#' @param db_con a connection to a database
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'
build_tm_keys <- function(db_con){
  
  # setup extraction plan
  plan <- collect_plan(db_con)
  
  ### get inpatient visits -----------------------------------------------------
  temp.in <- plan %>%
    dplyr::mutate(setting="inpatient") %>%
    gether_core_data(vars = c("patient_id","caseid","admdate","disdate","los"),db_con = db_con)
  
  temp.in <- temp.in %>%
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",0L,2L))) %>%
    dplyr::select("year","source_type","core_data") %>%
    tidyr::unnest(cols = c(core_data)) %>%
    dplyr::mutate(disdate=ifelse(is.na(disdate),admdate+los,disdate),
                  setting_type=5L) %>%
    dplyr::select(-los) %>% 
    distinct()
  
  ### get outpatient visits ----------------------------------------------------
  temp.out <- plan %>%
    dplyr::mutate(setting = "outpatient") %>% 
    gether_core_data(vars = c("patient_id", "stdplac", "svcdate", "stdprov",
                              "svcscat", "procgrp", "revcode", "proc1"),
                     db_con = db_con) %>%
    dplyr::mutate(core_data=purrr::map(.data$core_data,~dplyr::distinct(.)))
  
  # clean outpatient visits
  temp.out <- temp.out %>%
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",0L,2L))) %>%
    dplyr::select("year","source_type","core_data") %>%
    mutate(core_data = map(core_data, ~mutate(., procgrp = as.character(procgrp),
                                              stdprov = as.character(stdprov)))) %>%
    tidyr::unnest(cols = c(core_data)) %>%
    dplyr::mutate(disdate = .data$svcdate,
                  admdate = .data$svcdate,
                  setting_type = 1L) %>%
    dplyr::select(-svcdate) %>% 
    distinct()
  
  # Identify ED visits
  temp.out <-  add_ed_obs_indicator_new(temp.out) %>% distinct()
  
  ### get facility header visits -----------------------------------------------
  temp.fac <- plan %>%
    filter(year != "01") %>% 
    dplyr::mutate(setting = "facility") %>% 
    gether_core_data(vars = c("patient_id", "stdplac", "svcdate", "stdprov","caseid",
                              "svcscat", "procgrp", "revcode","fachdid"),
                     db_con = db_con)
  
  temp.fac <- temp.fac %>% 
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",0L,2L))) %>%
    dplyr::select("year","source_type","core_data") %>%
    mutate(core_data = map(core_data, ~mutate(.,stdprov = as.character(stdprov),
                                              caseid = as.integer(caseid)))) %>%
    tidyr::unnest(cols = c(core_data)) %>%
    dplyr::mutate(disdate = .data$svcdate,
                  admdate = .data$svcdate) %>%
    dplyr::select(-svcdate)
  
  # filter to outpatient not in services
  temp.fac <- temp.fac %>% 
    filter(caseid==0 | is.na(caseid)) %>%
    mutate(setting_type = 1L) %>% 
    mutate(svcscat=NA,procgrp=NA,proc1=NA,revcode=NA) %>% 
    add_ed_obs_indicator_new() %>% 
    distinct()
  
  temp.fac.add <- temp.fac %>% 
    anti_join(temp.out)
  
  # combine with outpatient
  temp.out <- bind_rows(temp.out,temp.fac.add)
  
  ### get rx visits ------------------------------------------------------------
  temp.rx <- plan %>%
    dplyr::mutate(setting="rx") %>%
    gether_rx_dates(db_con = db_con)
  
  # clean rx visits
  temp.rx <- temp.rx %>%
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",0L,2L))) %>%
    dplyr::select("year","source_type","rx_data") %>%
    tidyr::unnest(cols = c(rx_data)) %>%
    dplyr::mutate(disdate = svcdate,
                  admdate = svcdate,
                  setting_type = 4L) %>%
    dplyr::select(-svcdate) %>% 
    dplyr::distinct()
  
  ### assemble time_map --------------------------------------------------------
  temp_time_map <- dplyr::bind_rows(temp.in,temp.out,temp.rx) %>%
    dplyr::arrange(patient_id, admdate,setting_type) %>%
    dplyr::distinct() %>% 
    dplyr::mutate(key=dplyr::row_number()) 
  
  
  # get distinct outpatient keys
  out_keys <- temp_time_map %>%
    dplyr::filter(.data$setting_type %in% 1:3) %>%
    dplyr::select("year","source_type","patient_id","stdplac", "setting_type",
                  "svcdate"="admdate","key")
  
  # get distinct inpatient keys
  in_keys <- temp_time_map %>%
    dplyr::filter(.data$setting_type==5) %>%
    dplyr::select("year","source_type","patient_id","admdate",
                  "disdate","caseid","setting_type","key")
  
  # get distinct rx keys
  rx_keys <- temp_time_map %>%
    dplyr::filter(.data$setting_type==4) %>%
    dplyr::select("year","source_type","patient_id","svcdate"="admdate","key", "setting_type")
  
  return(list(time_map = temp_time_map,
              out_keys = out_keys,
              in_keys = in_keys,
              rx_keys = rx_keys))
}


#' Add tm (time_map) keys to remote database (new)
#'
#' This function adds time_map keys, created by `build_tm_keys()` to a remote database. Keys
#' can be added temporarily and/or overwritten. This function will first check if outpatient or inpatient
#' keys already exist, and if overwite is set to FALSE it will not proceed. NOTE: This function generally
#' only needs to be run once, after the mini database has been constructed.
#'
#' @param db_con a connection to a database
#' @param overwrite a logical indicator. If TRUE the existing inpatient_keys and/or outpatient_keys tables
#' will be overwritten
#' @param temporary a logical indicator. If TRUE the tables will only be added temporarilly to the database.
#' @return Nothing will be returned, but tables may be added to the remote database
#' @export
#'
add_tm_keys <- function(db_con, overwrite=FALSE, temporary=TRUE){
  
  # .Deprecated("assemble_time_map")
  
  if (any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys","rx_keys")) & overwrite==FALSE){
    warning("Database contains keys and overwrite set to FALSE")
  } else {
    temp_keys <- build_tm_keys(db_con = db_con)
    
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
#' @return a tibble containing the timemap
#' @export
#'
build_tm <- function(db_con){
  if (!any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys"))){
    warning("Database contains no visit keys. Temporary visit keys were generated using the collection table specified.")
    add_tm_keys(collect_tab=collect_tab, db_con = db_con,temporary = TRUE)
  }
  
  dat <- rbind(db_con %>%
                 dplyr::tbl("outpatient_keys") %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(disdate=.data$svcdate) %>%
                 dplyr::select("key","year","source_type","patient_id","admdate"="svcdate",
                               "disdate","stdplac", "setting_type"),
               
               db_con %>%
                 dplyr::tbl("rx_keys") %>%
                 dplyr::collect(n=Inf) %>%
                 dplyr::mutate(disdate=.data$svcdate,
                               stdplac=-2L) %>%
                 dplyr::select("key","year","source_type","patient_id","admdate"="svcdate",
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