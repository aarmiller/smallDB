#' Add indicators for ED or observational stay
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
add_ed_obs_indicator <- function(data){
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
    dplyr::select(year, source_type, enrolid, admdate, disdate, setting_type, stdplac)
  
  return(out)
}