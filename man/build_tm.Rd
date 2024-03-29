% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/new_patient_id_functions.R
\name{build_tm}
\alias{build_tm}
\title{Build longitudinal timemap from small db}
\usage{
build_tm(db_con)
}
\arguments{
\item{db_con}{connection to the small database}
}
\value{
a tibble containing the timemap
}
\description{
This function makes a time_map from the inpatient and outpatient visit keys,
contained in a small database. IF keys are not found in the database, optional
arguments allow keys to be generated temporarily (NOTE: this function cannot
be used to permanently add keys to the database, to permanently add keys use,
`add_time_map_keys()`).
}
