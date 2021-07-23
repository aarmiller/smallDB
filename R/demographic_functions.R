
#' Get enrollment data from one specific multiple "enrollees" table
#'
#' @name get_enroll_data
#' @param source ccae, mdcr, or medicaid
#' @param year year
#' @param enrolid_list A list of enrolids for which enrollment data will be collected
#' @param db_con A connection to a database
#' @param vars Vector of specific variables of interest in the "enrollees" tables (e.g. c("dobyr", "sex")) 
#' @param collect_n The number of observations to return
#' 
#' @return A tibble with information on each enrolid in enrolid_list. The number of columns in the tibble depends on
#' the number of vars selected
#' 
#' @export
#' 
get_enroll_data <- function (source, year, 
                             enrolid_list, collect_n = Inf,
                             vars = c("dobyr", "sex"), db_con) {

  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  
  tbl_name <- glue::glue("enrollees_{source}_{year}")
  
   out <- dplyr::tbl(db_con,tbl_name) %>% 
           dplyr::filter(enrolid %in% enrolid_list) %>% 
           dplyr::select(c("enrolid", dplyr::all_of(vars))) %>% 
           dplyr::collect(n = collect_n) %>% dplyr::mutate(enrolid = as.integer(enrolid)) %>% 
           dplyr::distinct()
   
   return(out)
}



#' Get enrollment data over multiple "enrollees" tables 
#'
#' @name gather_enroll_data
#' @param collect_tab A tibble with the specific setting (i.e. inpatient or outpatient), source (i.e. ccae or mdcr), and year to access
#' Default is all possible combinations of setting, source, and year
#' @param enrolid_list A list of enrolids for which enrollment data will be collected
#' @param db_con A connection to a database
#' @param vars Vector of specific variables of interest in the "enrollees" tables (e.g. c("dobyr", "sex")) 
#' @param collect_n The number of observations to return
#' is the smallest value between number of rows in for collect_tab or detected number of cores - 1
#' 
#' @return A tibble with information on each enrolid in enrolid_list. The number of columns in the tibble depends on
#' the number of vars selected
#' 
#' @examples
#' 
#' out <- gather_enroll_data(enrolid_list = final_cohort$enrolid, db_con = db_con, vars = c("dobyr", "sex"))
#' 
#' @export

gather_enroll_data <- function (collect_tab = collect_table(), enrolid_list, collect_n = Inf,
                                vars = c("dobyr", "sex"), db_con) {
    out <- collect_tab %>%
      dplyr::mutate(core_data=purrr::pmap(list(.data$source,.data$year),
                                          ~get_enroll_data(source = ..1,
                                                           year = ..2,
                                                           vars = vars,
                                                           enrolid_list = enrolid_list,
                                                           db_con = db_con,
                                                           collect_n = collect_n)))
    
    out <- out %>%  dplyr::select(core_data) %>%
      tidyr::unnest(c(core_data)) %>% 
      dplyr::distinct()
    
    return(out)
}

#' Get collapsed enrollment data from one specific multiple "enrollment_detail" table
#'
#' @name get_collapse_enrollment
#' @param source ccae, mdcr, or medicaid
#' @param year year
#' @param enrolid_list A list of enrolids for which enrollment data will be collected
#' @param db_con A connection to a database
#' @param vars Vector of specific variables of interest in the "enrollees" tables (e.g. c("egeoloc", "msa", "plantyp" ,"indstry")) 
#' @param collect_n The number of observations to return
#' 
#' @return A tibble with information on each enrolid in enrolid_list. The number of columns in the tibble depends on
#' the number of vars selected
#' 

#' @export
#' 

get_collapse_enrollment <- function (source, year, enrolid_list, collect_n = Inf,
                             vars = c("egeoloc", "msa", "plantyp" ,"indstry"), db_con) {

  checkmate::assert_choice(source, c("ccae", "mdcr","medicaid"))
  
  tbl_name <- glue::glue("enrollment_detail_{source}_{year}")
  
  temp <- dplyr::tbl(db_con,tbl_name)  %>%
    dplyr::filter(enrolid %in% enrolid_list) %>% 
    dplyr::select(c("enrolid", "dtstart", "dtend", dplyr::all_of(vars))) %>% 
    dplyr::collect(n = collect_n) %>% 
    dplyr::mutate(enrolid = as.integer(.data$enrolid))

  return(temp)
}


#' Get collapsed enrollment data over multiple "enrollment_detail" tables (in parallel)
#'
#' @name gather_collapse_enrollment
#' @param collect_tab A tibble with the specific setting (i.e. inpatient or outpatient), source (i.e. ccae or mdcr), and year to access
#' Default is all possible combinations of setting, source, and year
#' @param enrolid_list A list of enrolids for which enrollment data will be collected
#' @param db_con A connection to a database
#' @param vars Vector of specific variables of interest in the "enrollees" tables (e.g. c("dobyr", "sex")) 
#' @param collect_n The number of observations to return
#' 
#' @return A tibble with information on each enrolid in enrolid_list. The number of columns in the tibble depends on
#' the number of vars selected
#' 
#' @examples
#' 
#' out <- gather_collapse_enrollment(enrolid_list = final_cohort$enrolid, db_con = db_con, vars = c("egeoloc", "msa", "plantyp" ,"indstry"))
#' 
#' @export

gather_collapse_enrollment <- function (collect_tab = collect_table(), enrolid_list, collect_n = Inf,
                                        vars = c("egeoloc", "msa", "plantyp" ,"indstry"), 
                                        db_con) {
  out <- collect_tab %>%
    dplyr::mutate(enroll_data=purrr::pmap(list(.data$source,.data$year),
                                        ~get_collapse_enrollment(source = ..1,
                                                                 year = ..2,
                                                                 vars = vars,
                                                                 enrolid_list = enrolid_list,
                                                                 db_con = db_con,
                                                                 collect_n = collect_n)))
  
  temp <- out %>% 
    dplyr::select(enroll_data) %>% 
    tidyr::unnest(c(enroll_data))
  
  temp_strata <- temp  %>% 
    dplyr::select(c("enrolid", dplyr::all_of(vars))) %>% 
    dplyr::distinct() %>% 
    dplyr::mutate(strata = dplyr::row_number())
  
  temp <- temp %>% 
    dplyr::inner_join(temp_strata, by = c("enrolid", vars))
  
  out1 <- temp %>% dplyr::arrange(.data$enrolid, .data$dtstart) %>% 
    dplyr::group_by(.data$enrolid) %>% 
    dplyr::mutate(gap =((.data$dtstart - dplyr::lag(.data$dtend)) > 1) | .data$strata != dplyr::lag(.data$strata), 
                  gap = ifelse(is.na(.data$gap), FALSE, .data$gap)) %>% 
    dplyr::mutate(period = cumsum(.data$gap)) %>% 
    dplyr::group_by_at(c("enrolid", "period", vars)) %>%
    dplyr::summarise(dtstart = min(.data$dtstart), 
                     dtend = max(.data$dtend)) %>% 
    dplyr::ungroup()
  return(out1)
}
