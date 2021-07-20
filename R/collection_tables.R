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
#' @param medicaid_years years of medicaid data to collect. Default is NULL
#'
#'
#' @return a tibble
#' @examples
#' collect_table()
#' @export
collect_table <- function(settings = c("inpatient","outpatient","rx"),sources = c("ccae","mdcr"),
                          years=1:20, medicaid_years = NULL) {

  # convert years to stings
  years <- stringr::str_pad(years,2,pad="0")

  out <- tibble::tibble(setting=settings) %>%
    dplyr::mutate(source=purrr::map(.data$setting,~sources)) %>%
    tidyr::unnest(cols = c(source)) %>%
    dplyr::mutate(year=purrr::map(source,~years)) %>%
    tidyr::unnest(cols = c(year))

  if (!is.null(medicaid_years)) {
    medicaid_rows <- tibble::tibble(setting=settings) %>%
      dplyr::mutate(source="medicaid") %>%
      tidyr::unnest(cols = c(source)) %>%
      dplyr::mutate(year=purrr::map(source,~medicaid_years)) %>%
      tidyr::unnest(cols = c(year))

    out <- rbind(out,medicaid_rows)
  }

  return(out)
}
