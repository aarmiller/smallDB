
#' Gather all visit keys containing specific diagnosis codes over multiple combinations of setting, source, and year (in parallel) for large databases
#' @name build_dx_indicators_delay_large_DB
#' @param condition_dx_list A list of specific diagnosis codes that are of interest. The diagnosis codes need to be seperated into
#' diagnosis categories (e.g. cough, fever, ect.) and within the categories diagnosis codes should be seperated into ICD 9 and
#' ICD 10 specific codes, with list elements labled as icd9_codes and icd10_codes
#' @param db_path Path to the database
#' @param db_con The database connection
#' @param collect_tab A tibble with the specific setting (i.e. inpatient or outpatient), source (i.e. ccae or mdcr), and year to access.
#' Default is all possible combinations of setting, source, and year
#' @param num_cores The number of worker cores to use. If not specified will determined the number of cores based on the which ever
#' is the smallest value between number of rows in for collect_tab or detected number of cores - 1
#' @param return_keys_only Logical to return only the  visit keys containing specific diagnosis codes
#' @return A tibble with visit keys and indicators for the diagnosis codes categories supplied to the condition_dx_list argument
#' @export
#'

build_dx_indicators_delay_large_DB <- function (condition_dx_list, db_con, db_path, collect_tab = collect_table(), num_cores = NULL,
                                       return_keys_only = FALSE) {
  if (!any(dplyr::src_tbls(db_con) %in% c("outpatient_keys",
                                          "inpatient_keys"))) {
    warning("Database contains no visit keys. Temporary visit keys were generated using the collection table specified.")
    add_time_map_keys_delay(collect_tab = collect_tab, db_con = db_con, db_path = db_path,
                      temporary = TRUE)
  }

  if (return_keys_only == FALSE){
  all_cond_codes <- list(icd9_codes = purrr::map(condition_dx_list,
                                                 ~.$icd9_codes) %>% unlist(use.names = F),
                         icd10_codes = purrr::map(condition_dx_list, ~.$icd10_codes) %>% unlist(use.names = F))
  } else {
    all_cond_codes <- condition_dx_list
  }

  db_path2 <- db_path
  collect_tab2 <- collect_tab

  # set up clusters
  if (is.null(num_cores)) {
    num_cores <- min(nrow(collect_tab2), parallel::detectCores() - 1)
  } else {
    num_cores <- num_cores
  }

  cluster <- parallel::makeCluster(num_cores)
  parallel::clusterExport(cluster, varlist = c("gether_dx_keys_delay_large_DB"))
  parallel::clusterCall(cluster, function() library(tidyverse))
  parallel::clusterCall(cluster, function() library(dplyr))

  #give each worker only a row of the collect_tab
  #gether_dx_keys_delay will evaluate specificrow of the collect_tab for the worker
  tmp <- parallel::parLapply(cl = cluster,
                             1:nrow(collect_tab2),
                             function(x){gether_dx_keys_delay_large_DB(collect_tab = collect_tab2[x, ],
                                                         dx_list = all_cond_codes,
                                                         db_path = db_path2)})
  parallel::stopCluster(cluster)
  gc()

  cond_keys <- tibble()
  for (i in 1:length(tmp)){
    x <- tmp[[i]]
    if (!is.null(x)){
      cond_keys <- bind_rows(cond_keys, x)
    }
  }

  if (return_keys_only == TRUE){
    return(cond_keys)
  }

  cond_keys_name <- tibble::tibble(name = names(condition_dx_list)) %>%
    dplyr::mutate(dx = purrr::map(.data$name, ~condition_dx_list[[.]] %>%
                                    unlist())) %>% tidyr::unnest() %>% dplyr::inner_join(cond_keys,
                                                                                         by = "dx")
  cond_inds <- cond_keys_name %>% dplyr::distinct(.data$name,
                                                  .data$key) %>%
    dplyr::mutate(dx_ind = 1L) %>% tidyr::spread(key = .data$name, value = .data$dx_ind) %>%
    dplyr::inner_join(cond_keys_name %>%dplyr::distinct(.data$key) %>%
                        dplyr::mutate(any_ind = 1L), by = "key") %>%
    dplyr::mutate_at(.vars = dplyr::vars(-.data$key), .funs = list(~ifelse(is.na(.), 0L, .)))
  return(cond_inds)
}
