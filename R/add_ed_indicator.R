#' Add ED indicator
#'
#' This function  identifies ED visits from outpatient visits based on specific criteria.
#'
#' @param data A tibble of outpatient keys
#' @return A tibble of outpatient key including a source indicator for ED or outpatient
#'
#' @export
#'
add_ed_indicator <- function(data){
  out <- data %>% mutate(source = ifelse((stdplac==23 |
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
                                                (proc1 %in% c("99281","99282","99283","99284","99285"))), "ed", "outpatient")) %>%
    dplyr::select(year, ccae, enrolid, admdate, disdate, source, stdplac, inpatient)
  return(out)
}