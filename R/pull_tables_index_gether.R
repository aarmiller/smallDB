
#' Gather (gether) dx data in proximity to index
#'
#' @importFrom rlang .data
#'
#' @param collect_tab a collection table
#' @param vars variables to collect
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#'
#' @export
gether_index_dx_visits <- function(collect_tab=collect_table(),enrolid_table,days_before,days_after,db_con,collect_n=Inf){
  collect_tab %>%
    dplyr::distinct(source,year) %>% 
    dplyr::mutate(dx_data=purrr::pmap(list(.data$source,
                                           .data$year),
                                      ~get_index_dx_visits(source = ..1,
                                                           year = ..2,
                                                           enrolid_table=enrolid_table,
                                                           days_before = days_before,
                                                           days_after =days_after,
                                                           db_con = db_con,
                                                           collect_n = collect_n)))
}