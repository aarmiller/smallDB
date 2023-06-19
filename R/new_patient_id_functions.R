

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