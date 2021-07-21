#' Gather variables from core information for a specific table (in parallel)
#' @name get_core_data_parallel
#' @param table_name The specific core table name in the database to access
#' @param vars Vector of variables it the core table to return
#' @param db_path Path to the database
#' @param collect_n The number of observations to return
#' @return A tibble with all the specified variables to return. Number of rows of tibble returned is determined by the collect_n argument
#' @export
#'

get_core_data_parallel <- function (table_name, vars = c(), db_path, collect_n = Inf) {
  tbl_name <- table_name
  db_con <- src_sqlite(db_path)
  
  get_vars <- dplyr::tbl(db_con, tbl_name) %>% dplyr::tbl_vars()
  if (is.null(vars)) {
    get_vars <- get_vars
  }
  else {
    get_vars <- vars[vars %in% get_vars]
  }
  out <- db_con %>% dplyr::tbl(tbl_name) %>% dplyr::select(get_vars) %>%
    dplyr::collect(n = collect_n)
  return(out)
}