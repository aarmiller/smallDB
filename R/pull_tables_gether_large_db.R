
#' Gather all visit keys containing specific diagnosis codes for large databases
#' @name gether_dx_keys_delay_large_DB
#' @param collect_tab A tibble with the specific setting (i.e. inpatient or outpatient), source (i.e. ccae or mdcr), and year to access.
#' Default is all possible combinations of setting, source, and year
#' @param dx_list A list of specific diagnosis codes that are of interest. The diagnosis codes need to be seperated into into ICD 9 and
#' ICD 10 specific codes. The list elements need to be labled as icd9_codes and icd10_codes
#' @param db_path Path to the database
#' @return A tibble with all the specified diagnosis codes and the corresponding visit key where the diagnosis codes appeared
#' @export
#'

require(parallel)

gether_dx_keys_delay_large_DB <- function (collect_tab = collect_table(), dx_list, db_path) {
  db_con <- src_sqlite(db_path)
  icd_9_codes <- dx_list$icd9_codes
  icd_10_codes <- dx_list$icd10_codes
  if(collect_tab$setting == "inpatient"){
    if(as.integer(collect_tab$year) < 15){
      in_temp <- collect_tab %>% dplyr::filter(as.integer(.data$year) <
                                                 15) %>% dplyr::mutate(data = purrr::map2(.data$source,
                                                                                          .data$year, ~dplyr::tbl(db_con, paste0("inpatient_dx_",
                                                                                                                                 .x, "_", .y)) %>% dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                                                                            dplyr::distinct(.data$caseid, .data$dx) %>% dplyr::inner_join(dplyr::tbl(db_con,
                                                                                                                                                                     paste0("inpatient_core_", .x, "_", .y)) %>% dplyr::select(.data$caseid,
                                                                                                                                                                                                                               .data$enrolid), by = "caseid") %>% dplyr::distinct(.data$enrolid,
                                                                                                                                                                                                                                                                                  .data$caseid, .data$dx) %>% dplyr::collect(n = Inf) %>%
                                                                                            dplyr::mutate(enrolid = as.integer(.data$enrolid))))
    } else {
      in_temp1 <- collect_tab %>% dplyr::filter(as.integer(.data$year) >
                                                  14) %>% dplyr::mutate(data = purrr::map2(.data$source,
                                                                                           .data$year, ~dplyr::tbl(db_con, paste0("inpatient_dx9_",
                                                                                                                                  .x, "_", .y)) %>% dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                                                                             dplyr::distinct(.data$caseid, .data$dx) %>% dplyr::inner_join(dplyr::tbl(db_con,
                                                                                                                                                                      paste0("inpatient_core_", .x, "_", .y)) %>% dplyr::select(.data$caseid,
                                                                                                                                                                                                                                .data$enrolid), by = "caseid") %>% dplyr::distinct(.data$enrolid,
                                                                                                                                                                                                                                                                                   .data$caseid, .data$dx) %>% dplyr::collect(n = Inf) %>%
                                                                                             dplyr::mutate(enrolid = as.integer(.data$enrolid))))
      
      in_temp2 <- collect_tab %>% dplyr::filter(as.integer(.data$year) >
                                                  14) %>% dplyr::mutate(data = purrr::map2(source, .data$year,
                                                                                           ~dplyr::tbl(db_con, paste0("inpatient_dx10_", .x, "_",
                                                                                                                      .y)) %>% dplyr::filter(.data$dx %in% icd_10_codes) %>%
                                                                                             dplyr::distinct(.data$caseid, .data$dx) %>% dplyr::inner_join(dplyr::tbl(db_con,
                                                                                                                                                                      paste0("inpatient_core_", .x, "_", .y)) %>% dplyr::select(.data$caseid,
                                                                                                                                                                                                                                .data$enrolid), by = "caseid") %>% dplyr::distinct(.data$enrolid,
                                                                                                                                                                                                                                                                                   .data$caseid, .data$dx) %>% dplyr::collect(n = Inf) %>%
                                                                                             dplyr::mutate(enrolid = as.integer(.data$enrolid))))
      in_temp <- dplyr::bind_rows(in_temp1, in_temp2)
      rm(in_temp1, in_temp2)
    }
    
    if (nrow(in_temp %>% tidyr::unnest()) > 0){
      in_temp <- in_temp %>%
        dplyr::select(-.data$setting) %>% dplyr::mutate(ccae = ifelse(source == "ccae", 1L, 0L)) %>% tidyr::unnest()
      
      in_temp_caseid <- in_temp$caseid %>% unique()
      
      dx_keys <- db_con %>% dplyr::tbl("inpatient_keys") %>% dplyr::mutate(enrolid = as.integer(.data$enrolid)) %>%
        dplyr::select(.data$ccae, .data$year, .data$caseid, .data$key) %>%
        filter(caseid %in% in_temp_caseid) %>% collect(n = Inf) %>%
        dplyr::inner_join(in_temp , by = c("ccae", "year", "caseid")) %>%
        dplyr::select(.data$dx, .data$key)
      
      dx_keys <- dx_keys %>%
        dplyr::distinct()
      
    } else {
      dx_keys <- NULL
    }
  }
  if(collect_tab$setting == "outpatient"){
    if(as.integer(collect_tab$year) < 15){
      out_temp <- collect_tab %>% dplyr::filter(as.integer(.data$year) <
                                                  15) %>% dplyr::mutate(data = purrr::map2(source, .data$year,
                                                                                           ~dplyr::tbl(db_con, paste0("outpatient_dx_", .x, "_",
                                                                                                                      .y)) %>% dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                                                                             dplyr::distinct(seqnum_o, .data$enrolid, .data$svcdate,
                                                                                                             .data$dx) %>% dplyr::inner_join(dplyr::tbl(db_con,
                                                                                                                                                        paste0("outpatient_core_", .x, "_", .y)) %>% dplyr::select(seqnum_o,
                                                                                                                                                                                                                   .data$stdplac), by = "seqnum_o") %>% dplyr::distinct(.data$enrolid,
                                                                                                                                                                                                                                                                        .data$svcdate, .data$stdplac, .data$dx) %>% dplyr::collect(n = Inf) %>%
                                                                                             dplyr::mutate(enrolid = as.integer(.data$enrolid))))
    } else {
      out_temp1 <- collect_tab %>% dplyr::filter(as.integer(.data$year) >
                                                   14) %>% dplyr::mutate(data = purrr::map2(source, .data$year,
                                                                                            ~dplyr::tbl(db_con, paste0("outpatient_dx9_", .x, "_",
                                                                                                                       .y)) %>% dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                                                                              dplyr::distinct(seqnum_o, .data$enrolid, .data$svcdate,
                                                                                                              .data$dx) %>% dplyr::inner_join(dplyr::tbl(db_con,
                                                                                                                                                         paste0("outpatient_core_", .x, "_", .y)) %>% dplyr::select(seqnum_o,
                                                                                                                                                                                                                    .data$stdplac), by = "seqnum_o") %>% dplyr::distinct(.data$enrolid,
                                                                                                                                                                                                                                                                         .data$svcdate, .data$stdplac, .data$dx) %>% dplyr::collect(n = Inf) %>%
                                                                                              dplyr::mutate(enrolid = as.integer(.data$enrolid))))
      out_temp2<- collect_tab %>% dplyr::filter(as.integer(.data$year) >
                                                  14) %>% dplyr::mutate(data = purrr::map2(source, .data$year,
                                                                                           ~dplyr::tbl(db_con, paste0("outpatient_dx10_", .x, "_",
                                                                                                                      .y)) %>% dplyr::filter(.data$dx %in% icd_10_codes) %>%
                                                                                             dplyr::distinct(seqnum_o, .data$enrolid, .data$svcdate,
                                                                                                             .data$dx) %>% dplyr::inner_join(dplyr::tbl(db_con,
                                                                                                                                                        paste0("outpatient_core_", .x, "_", .y)) %>% dplyr::select(seqnum_o,
                                                                                                                                                                                                                   .data$stdplac), by = "seqnum_o") %>% dplyr::distinct(.data$enrolid,
                                                                                                                                                                                                                                                                        .data$svcdate, .data$stdplac, .data$dx) %>% dplyr::collect(n = Inf) %>%
                                                                                             dplyr::mutate(enrolid = as.integer(.data$enrolid))))
      out_temp <- dplyr::bind_rows(out_temp1, out_temp2)
      rm(out_temp1, out_temp2)
    }
    
    if (nrow(out_temp %>% tidyr::unnest()) > 0){
      out_temp <- out_temp%>%
        dplyr::select(-.data$setting)  %>% dplyr::select(.data$data) %>%
        tidyr::unnest()
      
      out_temp_enrolid <- out_temp$enrolid %>% unique()
      
      dx_keys <- db_con %>% dplyr::tbl("outpatient_keys") %>% dplyr::mutate(enrolid = as.integer(.data$enrolid)) %>%
        dplyr::select(.data$enrolid, .data$stdplac, .data$svcdate,.data$key) %>%
        filter(enrolid %in% out_temp_enrolid) %>% collect(n = Inf) %>%
        dplyr::inner_join(out_temp, by = c("enrolid", "stdplac", "svcdate")) %>%
        dplyr::select(.data$key, .data$dx)
      
      dx_keys <- dx_keys %>%
        dplyr::distinct()
      
    } else {
      dx_keys <- NULL
    }
  }
  return(dx_keys)
}