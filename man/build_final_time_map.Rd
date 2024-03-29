% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/build_final_time_map_function.R
\name{build_final_time_map}
\alias{build_final_time_map}
\title{Builds a time map containing visit specific information for a specific condition and cohort.}
\usage{
build_final_time_map(
  time_map = NULL,
  condition_short_name,
  duration_prior_to_index = 365L,
  collect_tab = smallDB::collect_table(),
  cohort_path,
  ssd_list
)
}
\arguments{
\item{time_map}{A time map that is already created. If NULL then it will create a time map. Default is NULL.}

\item{condition_short_name}{The condition of interest. Specifically the short_name version of the condition}

\item{duration_prior_to_index}{How far from the index should the time map extend back (e.g. 90, 180, 365)}

\item{collect_tab}{a collection table to build time_map from. By default all years will be used}

\item{cohort_path}{Path to the specific cohort for the condition of interest#'}

\item{ssd_list}{A list of diagnosis codes of interest. The diagnosis codes need to be separated into
diagnosis categories (e.g. cough, fever, etc.). Additionally, within the categories, diagnosis codes should be seperated into ICD 9 and
ICD 10 specific codes, with list elements labeled as icd9_codes and icd10_codes}
}
\value{
A tibble with all the specified diagnosis codes and the corresponding visit key where the diagnosis codes appeared
}
\description{
Builds a time map containing visit specific information for a specific condition and cohort.
}
\examples{
ssds <- read_rds("/Shared/Statepi_Diagnosis/params/ssd_codes/ssd_hsv_enceph.RDS")

final_time_map <- build_final_time_map(condition_short_name = "hsv_enceph",
                                       duration_prior_to_index = 365L,
                                       cohort_path = "/Shared/Statepi_Diagnosis/grant_projects/hsv_enceph/scripts/validation/enrolled_ge_365/updated_index_cohort.RDS",
                                       ssd_list = ssds)
}
