% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/demographic_functions.R
\name{get_collapse_enrollment}
\alias{get_collapse_enrollment}
\title{Get collapsed enrollment data from one specific multiple "enrollment_detail" table}
\usage{
get_collapse_enrollment(
  source,
  year,
  enrolid_list,
  collect_n = Inf,
  vars = c("egeoloc", "msa", "plantyp", "indstry"),
  db_con
)
}
\arguments{
\item{source}{ccae, mdcr, or medicaid}

\item{year}{year}

\item{enrolid_list}{A list of enrolids for which enrollment data will be collected}

\item{collect_n}{The number of observations to return}

\item{vars}{Vector of specific variables of interest in the "enrollees" tables (e.g. c("egeoloc", "msa", "plantyp" ,"indstry"))}

\item{db_con}{A connection to a database}
}
\value{
A tibble with information on each enrolid in enrolid_list. The number of columns in the tibble depends on
the number of vars selected
}
\description{
Get collapsed enrollment data from one specific multiple "enrollment_detail" table
}
