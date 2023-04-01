#' Place of Service Labels (STDPLAC)
#'
#' A dataset containing labels for Place of Service (STDPLAC)
#'
#' @format A data frame with 55 rows and 2 variables:
#' \describe{
#'   \item{stdplac}{Integer Value for stdplac}
#'   \item{label}{Place of Servce Label}
#' }
#' @source Marketscan Documentation
"stdplac_labels"


#' Geographic Location of Employee (EGEOLOC)
#'
#' A dataset containing labels for geographic location of employee
#'
#' @format A data frame with 66 rows and 3 variables:
#' \describe{
#'   \item{egeoloc}{Integer Value for geographic location}
#'   \item{label}{Geographic location Label}
#'   \item{level}{Indicator of label level, options include: `nation`, `region`, `state`, `city`}
#' }
#' @source Marketscan Documentation
"egeoloc_labels"