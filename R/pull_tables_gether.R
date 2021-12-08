#' Gather (gether) core data from all tables defined in a collection tab
#'
#' @importFrom rlang .data
#'
#' @param collect_tab a collection table
#' @param vars variables to collect
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
gether_core_data <- function(collect_tab=collect_table(),vars=c(),db_con,collect_n=Inf){
  collect_tab %>%
    dplyr::mutate(core_data=purrr::pmap(list(.data$setting,.data$source,.data$year),
                                        ~get_core_data(setting = ..1,
                                                       source = ..2,
                                                       year = ..3,
                                                       vars = vars,
                                                       db_con = db_con,
                                                       collect_n = collect_n)))

}



#' Gather (gether) rx dates from all rx tables defined in a collection tab
#'
#' @importFrom rlang .data
#'
#' @param collect_tab a collection table
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
gether_rx_dates <- function(collect_tab=collect_table(),db_con,collect_n=Inf){

  out <- collect_tab %>%
    filter(setting == "rx") %>%
    dplyr::mutate(rx_data=purrr::pmap(list(.data$source,.data$year),
                                      ~get_rx_dates(source = ..1,
                                                    year = ..2,
                                                    db_con = db_con,
                                                    collect_n = collect_n)))

  return(out)
}

#' Gather visit keys associated with a set of visits for a particular diagnosis
#'
#' This function return a tibble of
#'
#' @importFrom rlang .data
#'
#' @param collect_tab A collection table
#' @param dx_list A named list of icd9 and icd10 codes, with names of "icd9_codes" and
#' "icd10_codes", respectively
#' @param db_con A connection to the database
#'
#' @export
gether_dx_keys <- function(collect_tab=collect_table(),dx_list,db_con){
  
  
  if (!("icd9_codes" %in% names(dx_list))) {
    dx_list <- list(icd9_codes=purrr::map(dx_list,~.$icd9_codes) %>% unlist(use.names = F),
                    icd10_codes=purrr::map(dx_list,~.$icd10_codes) %>% unlist(use.names = F))
  }

  icd_9_codes <- dx_list$icd9_codes
  icd_10_codes <- dx_list$icd10_codes

  ##### get inpatient diagnoses ####
  in_temp1 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)<15,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(.data$source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                     dplyr::distinct(.data$caseid,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(.data$caseid,.data$enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))

  in_temp2 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(.data$source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                     dplyr::distinct(.data$caseid,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(.data$caseid,.data$enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))

  in_temp3 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_10_codes) %>%
                                     dplyr::distinct(.data$caseid,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(.data$caseid,.data$enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))

  in_temp <- dplyr::bind_rows(in_temp1,in_temp2,in_temp3) %>%
    dplyr::select(-.data$setting) %>%
    tidyr::unnest(cols = c(data)) %>%
    dplyr::group_by(source,.data$year) %>%
    tidyr::nest()

  rm(in_temp1,in_temp2,in_temp3)

  #### get outpatient diagnoses ####
  out_temp1 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)<15,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))

  out_temp2 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))

  out_temp3 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_10_codes) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))


  out_temp <- dplyr::bind_rows(out_temp1,out_temp2,out_temp3) %>%
    dplyr::select(-.data$setting) %>%
    tidyr::unnest(cols = c(data)) %>%
    dplyr::group_by(source,.data$year) %>%
    tidyr::nest()
  rm(out_temp1,out_temp2,out_temp3)
  ### Merge in the keys ####
  # Merge inpatient keys
  in_dx_keys <- db_con %>%
    dplyr::tbl("inpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$source_type,.data$year,.data$caseid,.data$key) %>%
    dplyr::inner_join(in_temp %>%
                        dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                                           ifelse(source=="mdcr",2L,3L))) %>% 
                        tidyr::unnest(cols = c(data)),
                      by = c("source_type", "year", "caseid")) %>%
    dplyr::select(.data$dx,.data$key)

  ## Merge outpatient keys
  out_dx_keys <- db_con %>%
    dplyr::tbl("outpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$enrolid,.data$stdplac,.data$svcdate,.data$key) %>%
    dplyr::inner_join(out_temp %>%
                        ungroup() %>%
                        dplyr::select(.data$data) %>%
                        tidyr::unnest(cols = c(data)),
                      by = c("enrolid", "stdplac", "svcdate")) %>%
    dplyr::select(.data$key,.data$dx)

  dx_keys <- dplyr::bind_rows(in_dx_keys,out_dx_keys) %>%
    dplyr::distinct()

  #### Return ####
  return(dx_keys)
}


#' Gather (gether) rx data from all tables defined in a collection tab
#'
#' @importFrom rlang .data
#'
#' @param collect_tab a collection table
#' @param ndc_codes a vector of ndc_codes to lookup
#' @param rx_vars variables to collect. Default is rx_vars = c("enrolid","ndcnum","svcdate")
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#' @param ignore_keys a logical indicator of whether to ignore RX keys when returning the data
#'
#' @export
gether_rx_keys <- function(collect_tab=collect_table(), ndc_codes,
                           rx_vars=c("enrolid","ndcnum","svcdate"), db_con,
                           collect_n=Inf, ignore_keys=FALSE){

  tmp_ndc_data <- collect_tab %>%
    filter(setting == "rx") %>%
    dplyr::mutate(rx_data=purrr::pmap(list(.data$source,.data$year),
                                      ~get_rx_data(source = ..1,
                                                   year = ..2,
                                                   ndc_codes = ndc_codes,
                                                   rx_vars = rx_vars,
                                                   db_con = db_con,
                                                   collect_n = collect_n))) %>%
    dplyr::select(.data$rx_data) %>%
    tidyr::unnest(cols = c("rx_data"))
  
  if (ignore_keys == TRUE) {
    return(tmp_ndc_data)
  }

  tmp_rx_keys <- db_con %>%
    dplyr::tbl("rx_keys") %>%
    dplyr::collect()

  out <- tmp_ndc_data %>%
    dplyr::inner_join(select(tmp_rx_keys,
                             .data$enrolid, .data$svcdate, .data$key),
                      by = c("enrolid", "svcdate"))

  return(out)
}


#' Gather (gether) data from all tables defined in a collection tab
#'
#' @importFrom rlang .data
#'
#' @param collect_tab a collection table
#' @param table name of table to collect data from
#' @param vars variables to collect
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
gether_table_data <- function(collect_tab=collect_table(),table,vars=c(),db_con,collect_n=Inf){
  
  # correct the collection table
  collect_tab <- collect_tab %>% 
    mutate(table = table) %>% 
    distinct(table,source,year)
  
  collect_tab %>%
    dplyr::mutate(table_data=purrr::pmap(list(.data$table,.data$source,.data$year),
                                         ~get_table_data(table = ..1,
                                                         source = ..2,
                                                         year = ..3,
                                                         vars = vars,
                                                         db_con = db_con,
                                                         collect_n = collect_n)))
  
}


#' Gather visit keys associated with a set of visits for a particular procedure
#'
#' This function return a tibble of
#'
#' @importFrom rlang .data
#'
#' @param collect_tab A collection table
#' @param proc_list A named list of icd9, icd10 and CPT procedure codes, with names of "icd9_codes" and
#' "icd10_codes", respectively
#' @param db_con A connection to the database
#'
#' @export
gether_proc_keys <- function(collect_tab=collect_table(),proc_list,db_con){
  
  # combine procedure codes
  proc_codes <- c(proc_list$icd9pcs_codes,
                  proc_list$icd10pcs_codes,
                  proc_list$cpt_codes)
  
  # pull inpatient procedures
  tmp_in <- collect_tab %>% 
    dplyr::filter(setting=="inpatient") %>% 
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~get_proc_dates(source = .x,
                                                  year = .y,
                                                  proc_codes = proc_codes,
                                                  setting = "inpatient",
                                                  db_con = con,
                                                  tbl_vars = c("caseid","proc"))))
  
  # merge in inpatient keys
  in_keys <- tmp_in %>% 
    tidyr::unnest(data) %>% 
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",2L,3L))) %>% 
    dplyr::select(year,source_type,caseid,proc) %>% 
    dplyr::inner_join(tbl(db_con,"inpatient_keys") %>% 
                        dplyr::select(.data$year,.data$source_type,.data$caseid,.data$key) %>% 
                        dplyr::collect(n = Inf) %>% 
                        dplyr::distinct(),
               by = c("year", "source_type", "caseid")) %>% 
    dplyr::select(.data$key,.data$proc)
  
  # pull outpatient procedures
  tmp_out <- collect_table() %>% 
    dplyr::filter(setting=="outpatient") %>% 
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~get_proc_dates(source = .x,
                                                  year = .y,
                                                  proc_codes = proc_codes,
                                                  setting = "outpatient",
                                                  db_con = db_con,
                                                  tbl_vars = c("enrolid","svcdate","stdplac","proc1"))))
  
  
  # merge in outpatient keys
  out_keys <- tmp_out %>% 
    tidyr::unnest(data) %>% 
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",2L,3L))) %>% 
    dplyr::select(.data$enrolid,.data$source_type,.data$stdplac,.data$svcdate,proc=.data$proc1) %>% 
    dplyr::inner_join(tbl(db_con,"outpatient_keys") %>% 
                        dplyr::select(.data$enrolid,.data$source_type,.data$stdplac,.data$svcdate,.data$key) %>% 
                        dplyr::collect(n=Inf),
                      by = c("enrolid", "source_type", "stdplac", "svcdate")) %>% 
    dplyr::select(.data$key,.data$proc)
  
  # combine keys
  proc_keys <- dplyr::bind_rows(in_keys,
                                out_keys)
  
  #### Return ####
  return(proc_keys)
}


#' Gather visit keys associated with a set of visits for a particular inpatient diagnosis
#'
#' This function return a tibble of
#'
#' @importFrom rlang .data
#'
#' @param collect_tab A collection table
#' @param dx_list A named list of icd9 and icd10 codes, with names of "icd9_codes" and
#' "icd10_codes", respectively
#' @param dx_num a logical indicator of whether to collect diagnosis number. Default is TRUE.
#' @param db_con A connection to the database
#'
#' @export
gether_inpatient_dx_keys <- function(collect_tab=collect_table(),dx_list,dx_num=TRUE,db_con){
  
  # collect inpatient data
  tmp_out <- collect_tab %>% 
    dplyr::filter(setting=="inpatient") %>% 
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~get_inpatient_dx_visits(source = .x,
                                                            year = .y,
                                                            dx_list = dx_list,
                                                            dx_num = dx_num,
                                                            db_con = db_con))) %>% 
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",2L,3L))) %>% 
    dplyr::select(source_type,year,data) %>% 
    tidyr::unnest(data)
  
  # pull in inpatient keys
  in_keys <- tbl(db_con,"inpatient_keys") %>% 
    dplyr::select(year,source_type,caseid,key) %>% 
    dplyr::collect(n = Inf)
  
  # merge inpatient keys and the visit data
  if (dx_num == TRUE){
    out <- tmp_out %>% 
      dplyr::inner_join(in_keys,by = c("source_type", "year", "caseid")) %>% 
      dplyr::select(dx,dx_num,key)
  } else {
    out <- tmp_out %>% 
      dplyr::inner_join(in_keys,by = c("source_type", "year", "caseid")) %>% 
      dplyr::select(dx,key)
  }
  
  return(out)
}

#' Gather facility visits associated with a set of procedures
#'
#' This function return a tibble of
#'
#' @importFrom rlang .data
#'
#' @param collect_tab A collection table
#' @param proc_codes A named list of icd9, icd10 and CPT procedure codes, with names of "icd9pcs_codes" and
#' "icd10pcs_codes", and "cpt_codes" respectively
#' @param tbl_vars names of variables to pull from corresponding tables. If NULL date and enrolid will be collected
#' @param db_con A connection to the database
#'
#' @export
gether_facility_procs <- function(collect_tab=collect_table(),proc_list,tbl_vars=NULL,db_con){
  
  # combine procedure codes
  proc_codes <- c(proc_list$icd9pcs_codes,
                  proc_list$icd10pcs_codes,
                  proc_list$cpt_codes)
  
  # pull procedures
  out <- collect_tab %>% 
    dplyr::distinct(source,year) %>% 
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~get_facility_procs(source = .x,
                                                       year = .y,
                                                       proc_codes = proc_codes,
                                                       db_con = con,
                                                       tbl_vars = tbl_vars)))
  
  # combine output
  out <- dplyr::bind_rows(out$data) %>% 
    dplyr::distinct()
 
  
  #### Return ####
  return(out)
}

#' Gather all visit keys associated with a set of visits
#'
#' This function return a tibble of
#'
#' @importFrom rlang .data
#'
#' @param collect_tab A collection table
#' @param dx_num a logical indicator of whether to collect diagnosis number. Default is TRUE.
#' @param db_con A connection to the database
#' @param primary whether or not to only return primary diagnoses, if TRUE forces dx_num to TRUE
#'
#' @export
gether_all_inpatient_dx_keys <- function(collect_tab=collect_table(),dx_num=TRUE,db_con,keys,primary=FALSE){
  
  # collect inpatient data
  tmp_out <- collect_tab %>% 
    dplyr::filter(setting=="inpatient") %>% 
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~get_all_inpatient_dx_visits(source = .x,
                                                            year = .y,
                                                            dx_num = dx_num,
                                                            db_con = db_con))) %>% 
    dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                       ifelse(source=="mdcr",2L,3L))) %>% 
    dplyr::select(source_type,year,data) %>% 
    tidyr::unnest(data)
  
  # pull in inpatient keys
  in_keys <- tbl(db_con,"inpatient_keys") %>% 
    dplyr::select(year,source_type,caseid,key) %>% 
    dplyr::collect(n = Inf) %>%
    filter(key %in% keys)
  
  # if primary true then force dx_num true
  if (primary) {
    dx_num <- TRUE
  }
  
  # merge inpatient keys and the visit data
  if (dx_num == TRUE){
    out <- tmp_out %>% 
      dplyr::inner_join(in_keys,by = c("source_type", "year", "caseid")) %>% 
      dplyr::select(dx,dx_num,key)
  } else {
    out <- tmp_out %>% 
      dplyr::inner_join(in_keys,by = c("source_type", "year", "caseid")) %>% 
      dplyr::select(dx,key)
  }
  
  if (primary) {
    out <- out %>%
      filter(dx_num == 1)
  }
  
  return(out)
}

#' Gather all visit keys associated with a set of visits for a particular diagnosis
#'
#' This function return a tibble of
#'
#' @importFrom rlang .data
#'
#' @param collect_tab A collection table
#' @param db_con A connection to the database
#'
#' @export
gether_all_dx_keys <- function(collect_tab=collect_table(),db_con, keys){
  
  
  ##### get inpatient diagnoses ####
  in_temp1 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)<15,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(.data$source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::distinct(.data$caseid,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(.data$caseid,.data$enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  in_temp2 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(.data$source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::distinct(.data$caseid,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(.data$caseid,.data$enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  in_temp3 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::distinct(.data$caseid,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(.data$caseid,.data$enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  in_temp <- dplyr::bind_rows(in_temp1,in_temp2,in_temp3) %>%
    dplyr::select(-.data$setting) %>%
    tidyr::unnest(cols = c(data)) %>%
    dplyr::group_by(source,.data$year) %>%
    tidyr::nest()
  
  rm(in_temp1,in_temp2,in_temp3)
  
  #### get outpatient diagnoses ####
  out_temp1 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)<15,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  out_temp2 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  out_temp3 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  
  out_temp <- dplyr::bind_rows(out_temp1,out_temp2,out_temp3) %>%
    dplyr::select(-.data$setting) %>%
    tidyr::unnest(cols = c(data)) %>%
    dplyr::group_by(source,.data$year) %>%
    tidyr::nest()
  rm(out_temp1,out_temp2,out_temp3)
  ### Merge in the keys ####
  # Merge inpatient keys
  in_dx_keys <- db_con %>%
    dplyr::tbl("inpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$source_type,.data$year,.data$caseid,.data$key) %>%
    dplyr::inner_join(in_temp %>%
                        dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                                           ifelse(source=="mdcr",2L,3L))) %>% 
                        tidyr::unnest(cols = c(data)),
                      by = c("source_type", "year", "caseid")) %>%
    dplyr::select(.data$dx,.data$key)
  
  ## Merge outpatient keys
  out_dx_keys <- db_con %>%
    dplyr::tbl("outpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$enrolid,.data$stdplac,.data$svcdate,.data$key) %>%
    dplyr::inner_join(out_temp %>%
                        ungroup() %>%
                        dplyr::select(.data$data) %>%
                        tidyr::unnest(cols = c(data)),
                      by = c("enrolid", "stdplac", "svcdate")) %>%
    dplyr::select(.data$key,.data$dx)
  
  dx_keys <- dplyr::bind_rows(in_dx_keys,out_dx_keys) %>%
    dplyr::distinct() %>%
    dplyr::filter(key %in% keys)
  
  #### Return ####
  return(dx_keys)
}


