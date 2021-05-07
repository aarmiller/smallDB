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

#' Gather visit keys associated with a set of visits
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
    dplyr::select(.data$ccae,.data$year,.data$caseid,.data$key) %>%
    dplyr::inner_join(in_temp %>%
                        dplyr::mutate(ccae=ifelse(source=="ccae",1L,0L)) %>%
                        tidyr::unnest(cols = c(data)),
                      by = c("ccae", "year", "caseid")) %>%
    dplyr::select(.data$dx,.data$key)

  ## Merge outpatient keys
  out_dx_keys <- db_con %>%
    dplyr::tbl("outpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$enrolid,.data$stdplac,.data$svcdate,.data$key) %>%
    dplyr::inner_join(out_temp %>%
                        dplyr::select(.data$data) %>%
                        tidyr::unnest(cols = c(data)),
                      by = c("enrolid", "stdplac", "svcdate")) %>%
    dplyr::select(.data$key,.data$dx)

  dx_keys <- dplyr::bind_rows(in_dx_keys,out_dx_keys) %>%
    dplyr::distinct()

  #### Return ####
  return(dx_keys)
}

