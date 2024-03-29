% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pull_tables_gether.R
\name{gether_facility_procs}
\alias{gether_facility_procs}
\title{Gather facility visits associated with a set of procedures}
\usage{
gether_facility_procs(
  collect_tab = collect_table(),
  proc_list,
  tbl_vars = NULL,
  db_con
)
}
\arguments{
\item{collect_tab}{A collection table}

\item{tbl_vars}{names of variables to pull from corresponding tables. If NULL date and enrolid will be collected}

\item{db_con}{A connection to the database}

\item{proc_codes}{A named list of icd9, icd10 and CPT procedure codes, with names of "icd9pcs_codes" and
"icd10pcs_codes", and "cpt_codes" respectively}
}
\description{
This function return a tibble of
}
