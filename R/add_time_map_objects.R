#' Build diagnosis indicators corresponding to a list of diagnoses
#'
#' This function makes indicators corresponding to a list of diagnosis codes,
#' for visits identifiedby timemap keys. IF keys are not found in the database, optional
#' arguments allow keys to be generated temporarily (NOTE: this function cannot
#' be used to permanently add keys to the database, to permanently add keys use,
#' `add_time_map_keys()`).
#'
#' @importFrom rlang .data
#'
#' @param condition_dx_list a named list of icd9 and icd10 codes. Each condition should be named
#' and icd9_codes and icd10_codes should be stored as a named sub-list for each condition
#' @param db_con connection to the small database
#' @param collect_tab (optional) a collection table. This argument is only used to make temporary,
#' visit keys if no keys are found in the database
#' @return a tibble containing keys and indicators for each condition
#' @export
build_dx_indicators <- function(condition_dx_list,db_con,collect_tab=collect_table()){

  if (!any(DBI::dbListTables(db_con) %in% c("outpatient_keys","inpatient_keys"))){
    warning("Database contains no visit keys. Temporary visit keys were generated using the collection table specified.")
    add_time_map_keys(collect_tab=collect_tab,db_con = db_con,temporary = TRUE)
  }

  all_cond_codes <- list(icd9_codes=purrr::map(condition_dx_list,~.$icd9_codes) %>% unlist(use.names = F),
                         icd10_codes=purrr::map(condition_dx_list,~.$icd10_codes) %>% unlist(use.names = F))

  cond_keys <- gether_dx_keys(collect_tab = collect_tab,dx_list = all_cond_codes,db_con = db_con)

  cond_keys_name <- tibble::tibble(name=names(condition_dx_list)) %>%
    dplyr::mutate(dx=purrr::map(.data$name,~condition_dx_list[[.]] %>% unlist())) %>%
    tidyr::unnest(cols = c(dx)) %>%
    dplyr::inner_join(cond_keys,by = "dx")

  cond_inds <- cond_keys_name %>%
    dplyr::distinct(.data$name,.data$key) %>%
    dplyr::mutate(dx_ind=1L) %>%
    tidyr::spread(key=.data$name,value=.data$dx_ind) %>%
    dplyr::inner_join(cond_keys_name %>%
                        dplyr::distinct(.data$key),
                      by = "key") %>%
    dplyr::mutate_at(.vars=dplyr::vars(-.data$key),
                     .funs=list(~ifelse(is.na(.),0L,.)))

  return(cond_inds)
}



#' Build rx indicators corresponding to a list of medications
#'
#' This function makes indicators corresponding to a list of ndc codes,
#' for visits identified by timemap keys. If keys are not found in the database,
#' this function will not run
#' `add_time_map_keys()`).
#'
#' @importFrom rlang .data
#'
#' @param rx_list a named list of ndc codes. Each condition should be named and
#' the ndc codes should be stored as a named sub-list for each condition
#' @param db_con connection to the small database
#' @param collect_tab (optional) a collection table. This argument is only used to make temporary,
#' visit keys if no keys are found in the database
#' @return a tibble containing keys and indicators for each condition
#' @export
build_rx_indicators <- function(condition_dx_list,db_con,collect_tab=collect_table()){

  if (!any(DBI::dbListTables(db_con) %in% c("rx_keys"))){
    stop("Database contains no rx keys.")
  }

  # all_cond_codes <- list(icd9_codes=purrr::map(condition_dx_list,~.$icd9_codes) %>% unlist(use.names = F),
  #                        icd10_codes=purrr::map(condition_dx_list,~.$icd10_codes) %>% unlist(use.names = F))
  #
  # cond_keys <- gether_dx_keys(collect_tab = collect_tab,dx_list = all_cond_codes,db_con = db_con)
  #
  # cond_keys_name <- tibble::tibble(name=names(condition_dx_list)) %>%
  #   dplyr::mutate(dx=purrr::map(.data$name,~condition_dx_list[[.]] %>% unlist())) %>%
  #   tidyr::unnest(cols = c(dx)) %>%
  #   dplyr::inner_join(cond_keys,by = "dx")
  #
  # cond_inds <- cond_keys_name %>%
  #   dplyr::distinct(.data$name,.data$key) %>%
  #   dplyr::mutate(dx_ind=1L) %>%
  #   tidyr::spread(key=.data$name,value=.data$dx_ind) %>%
  #   dplyr::inner_join(cond_keys_name %>%
  #                       dplyr::distinct(.data$key),
  #                     by = "key") %>%
  #   dplyr::mutate_at(.vars=dplyr::vars(-.data$key),
  #                    .funs=list(~ifelse(is.na(.),0L,.)))

  return(cond_inds)
}
