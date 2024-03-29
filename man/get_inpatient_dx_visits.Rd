% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pull_tables_get.R
\name{get_inpatient_dx_visits}
\alias{get_inpatient_dx_visits}
\title{Collect inpatient diagnosis visits}
\usage{
get_inpatient_dx_visits(
  source,
  year,
  dx_list,
  dx_num = TRUE,
  db_con,
  collect_n = Inf
)
}
\arguments{
\item{source}{ccae, mdcr or medicaid}

\item{year}{year (as integer value)}

\item{dx_list}{A named list of icd9 and icd10 codes, with names of "icd9_codes" and
"icd10_codes", respectively}

\item{dx_num}{a logical indicator of whether to collect diagnosis number. Default is TRUE.}

\item{db_con}{a connection to a database,}

\item{collect_n}{the number of rows to collect}
}
\value{
A tibble of variables from the respective table
}
\description{
Collect inpatient visits for a particular diagnosis along with the diagnosis order
}
