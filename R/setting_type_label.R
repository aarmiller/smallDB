#' Converts setting type integer values to their corresponding character forms.
#' @name setting_type_labels
#' @param setting_type_vector A vector of setting types in integer form
#' @return A vector of setting types converted to the respective character forms
#' @export

setting_type_labels <- function(setting_type_vector){
  out <- as.character(factor(setting_type_vector,
                             levels= 1:5,
                             labels = c("outpatient",
                                        "ed",
                                        "obs_stay",
                                        "rx",
                                        "inpatient")))
  return(out)
}
