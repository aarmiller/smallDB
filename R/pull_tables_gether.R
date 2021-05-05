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
