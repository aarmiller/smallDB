#' Build collection tibble
#'
#'
#' `collect_table` sets up a collection tibble for holding and distributing
#' queries across a rand of tables in the database
#'
#' @importFrom rlang .data
#'
#' @param settings "inpatient", "outpatient", "rx"
#' @param sources "ccae" or "mdcr" or c("ccae","mdcr")
#' @param years vector of years to collect. Note: if no argument is provided
#' all years will be selected
#'
#' @return a tibble
#' @examples
#' collect_table()
#' @export
collect_table <- function(settings=c("inpatient","outpatient","rx"),sources=c("ccae","mdcr"),years=1:20) {

  # convert years to stings
  years <- stringr::str_pad(c(1:20),2,pad="0")

  tibble::tibble(setting=settings) %>%
    dplyr::mutate(source=purrr::map(.data$setting,~sources)) %>%
    tidyr::unnest(cols = c(source)) %>%
    dplyr::mutate(year=purrr::map(source,~years)) %>%
    tidyr::unnest(cols = c(year))
}
