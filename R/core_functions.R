#' Build collection tibble
#'
#'
#' `collect_table` sets up a collection tibble for holding and distributing
#' queries across a rand of tables in the database
#'
#' @importFrom rlang .data
#'
#' @param settings "inpatient", "outpatient" or c("inpatient","outpatient")
#' @param sources "ccae" or "mdcr" or c("ccae","mdcr")
#' @param years vector of years to collect. Note: if no argument is provided
#' all years will be selected
#'
#' @return a tibble
#' @examples
#' collect_table()
#' @export
collect_table <- function (settings = c("inpatient", "outpatient"),
                           sources = c("ccae", "mdcr"),
                           years = NULL)
{
  if (is.null(years)) {
    years <- stringr::str_pad(c(1:17), 2, pad = "0")
  } else {
    years <- stringr::str_pad(years, 2, pad = "0")
  }
  tibble::tibble(setting = settings) %>% 
    dplyr::mutate(source = purrr::map(.data$setting,~sources)) %>% 
    tidyr::unnest(c(source)) %>% 
    dplyr::mutate(year = purrr::map(source,~years)) %>% 
    tidyr::unnest(c(year))
}
