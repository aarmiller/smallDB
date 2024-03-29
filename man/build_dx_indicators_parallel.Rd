% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/add_time_map_objects_parallel.R
\name{build_dx_indicators_parallel}
\alias{build_dx_indicators_parallel}
\title{Gather all visit keys containing specific diagnosis codes over multiple combinations of setting, source, and year (in parallel)}
\usage{
build_dx_indicators_parallel(
  condition_dx_list,
  db_con,
  db_path,
  collect_tab = collect_table(),
  num_cores = NULL,
  return_keys_only = FALSE
)
}
\arguments{
\item{condition_dx_list}{A list of specific diagnosis codes that are of interest. The diagnosis codes need to be seperated into
diagnosis categories (e.g. cough, fever, ect.) and within the categories diagnosis codes should be seperated into ICD 9 and
ICD 10 specific codes, with list elements labled as icd9_codes and icd10_codes}

\item{db_con}{The database connection}

\item{db_path}{Path to the database}

\item{collect_tab}{A tibble with the specific setting (i.e. inpatient or outpatient), source (i.e. ccae or mdcr), and year to access.
Default is all possible combinations of setting, source, and year}

\item{num_cores}{The number of worker cores to use. If not specified will determined the number of cores based on the which ever
is the smallest value between number of rows in for collect_tab or detected number of cores - 1}

\item{return_keys_only}{Logical to return only the  visit keys containing specific diagnosis codes}
}
\value{
A tibble with visit keys and indicators for the diagnosis codes categories supplied to the condition_dx_list argument
}
\description{
Gather all visit keys containing specific diagnosis codes over multiple combinations of setting, source, and year (in parallel)
}
