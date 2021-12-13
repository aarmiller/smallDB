#' Builds a time map containing visit specific information for a specific condition and cohort.
#' @name build_final_time_map
#' @param time_map A time map that is already created. If NULL then it will create a time map. Default is NULL.
#' @param condition_short_name The condition of interest. Specifically the short_name version of the condition
#' @param duration_prior_to_index How far from the index should the time map extend back (e.g. 90, 180, 365)
#' @param cohort_path Path to the specific cohort for the condition of interest#' 
#' @param collect_tab a collection table to build time_map from. By default all years will be used
#' @param ssd_list A list of diagnosis codes of interest. The diagnosis codes need to be separated into
#' diagnosis categories (e.g. cough, fever, etc.). Additionally, within the categories, diagnosis codes should be seperated into ICD 9 and
#' ICD 10 specific codes, with list elements labeled as icd9_codes and icd10_codes
#' @return A tibble with all the specified diagnosis codes and the corresponding visit key where the diagnosis codes appeared
#' @examples
#' ssds <- read_rds("/Shared/Statepi_Diagnosis/params/ssd_codes/ssd_hsv_enceph.RDS")
#'
#' final_time_map <- build_final_time_map(condition_short_name = "hsv_enceph",
#'                                        duration_prior_to_index = 365L,
#'                                        cohort_path = "/Shared/Statepi_Diagnosis/grant_projects/hsv_enceph/scripts/validation/enrolled_ge_365/updated_index_cohort.RDS",
#'                                        ssd_list = ssds)
#' @export
#'

build_final_time_map <- function (time_map = NULL,
                                  condition_short_name, 
                                  duration_prior_to_index = 365L,
                                  collect_tab = smallDB::collect_table(),
                                  cohort_path, ssd_list){

    #update all_dx to include dx date instead of time to dx (this is necessary as index dates are shifted do time to dx needs to be recalculated)
    all_dx <- all_dx  %>% dplyr::inner_join(index_data %>% select(enrolid, index_date),
                                     by = "enrolid") %>%
      dplyr::mutate(visit_date = index_date + days_since_index) %>%
      dplyr::select(-days_since_index, - index_date)

    # Get subset of enrolids that will be used in the analysis
    final_cohort <- readr::read_rds(cohort_path)

    index_data <- final_cohort %>%
      dplyr::mutate(time_before_index = index_date - first_enroll) %>%
      dplyr::select(enrolid, index_date , enroll_first = first_enroll, time_before_index, first_dx_source = index_dx_source, stdplac, key)

    #update all_dx to include adjusted days_since_index using new index dates also select for enrolids in cohort
    all_dx <- all_dx  %>%
      dplyr::inner_join(index_data %>% select(enrolid, index_date),
                        by = "enrolid") %>%
      dplyr::mutate(days_since_index = visit_date - index_date) %>%
      dplyr::select(-visit_date, -index_date)

    # select for enroilid that were enrolled >=duration_prior_to_index days prior to index
    enrolled_ge_1year <- index_data %>% 
      dplyr::filter(time_before_index >= duration_prior_to_index) %>%
      dplyr::distinct(enrolid) %>% .$enrolid
    
    index_data <- index_data %>% 
      dplyr::filter(time_before_index >= duration_prior_to_index)
    
    dx_data <- all_dx %>% 
      dplyr::filter(enrolid %in% enrolled_ge_1year) %>%
      dplyr::rename(days_since_dx=days_since_index) %>%
      dplyr::filter(dplyr::between(days_since_dx,-duration_prior_to_index,0))
    
    rx_data <- all_rx %>% 
      dplyr::inner_join(index_data %>% select(enrolid, index_date), by = "enrolid") %>%
      dplyr::mutate(days_since_rx = rx_date - index_date) %>%
      dplyr::filter(dplyr::between(days_since_rx,-duration_prior_to_index,0)) %>% 
      dplyr::select(enrolid, rx, days_since_rx)

    if (is.null(time_map)){
      # build timemap
      time_map <- smallDB::build_time_map(db_con = db_con, 
                                 collect_tab = collect_tab)
      gc()
  
      # subset TB timemap to cases of interest
      time_map <- time_map %>% 
        dplyr::inner_join(index_data %>%
                            dplyr::distinct(enrolid, index_date),
                          by = "enrolid")
      
      # add first_dx visit
      time_map <-  time_map %>%
        dplyr::filter(!is.na(setting_type)) %>% 
        dplyr::mutate(setting_type = smallDB::setting_type_labels(.$setting_type)) %>%
        dplyr::left_join(index_data %>%
                           dplyr::select(enrolid, admdate = index_date,
                                         setting_type = first_dx_source, 
                                         stdplac, key) %>%
                           dplyr::mutate(first_dx = 1),
                  by = c("key", "enrolid", "admdate", "stdplac", "setting_type")) %>%
        dplyr::mutate(first_dx = ifelse(is.na(first_dx), 0, first_dx))
  
      # create inpatient, ED, and outpatient indicators
      time_map <- time_map %>% 
        dplyr::mutate(ind = 1L) %>% 
        dplyr::distinct() %>% 
        dplyr::filter(!is.na(setting_type)) %>%
        tidyr::pivot_wider(names_from = setting_type, values_from = ind) %>% 
        dplyr::mutate_at(vars(ed, inpatient, outpatient, rx,  obs_stay),~ifelse(is.na(.),0L,.))
  
      # compute time to first diagnosis
      time_map <- time_map %>%
        dplyr::group_by(enrolid) %>%
        dplyr::arrange(enrolid, admdate) %>%
        dplyr::mutate(days_since_dx= admdate - index_date) %>%
        dplyr::mutate(visit_no=row_number()) %>%
        dplyr::ungroup()
      warning("Time map was created as time map was not specified")
    } else {
      time_map <- time_map %>% 
        dplyr::inner_join(index_data %>%
                            dplyr::distinct(enrolid),
                          by = "enrolid")
    }
    
    # get ssd keys
    ssd_inds <- smallDB::build_dx_indicators(condition_dx_list = ssd_list,
                                             collect_tab = collect_tab,
                                             db_con = db_con) %>%
      mutate(any_ssd=1L)


    # Make final timemap with condition indicators
    # extract names of the condition indicators
    ind_names <- ssd_inds %>%
      dplyr::select(-key) %>%
      names()

    # add indicators to time_map
    final_time_map <- time_map %>%
      dplyr::left_join(ssd_inds, by="key") %>%
      dplyr::mutate_at(dplyr::vars(dplyr::all_of(ind_names)),.funs = list(~ifelse(is.na(.),0L,.)))
    
    # aggregate duplicate visits on same day and in same location
    vars_to_summarise <- c("inpatient", "ed", "first_dx","outpatient","rx","obs_stay",
                           ind_names)
    
    final_time_map <- final_time_map %>%
                      group_by(enrolid, days_since_dx, stdplac) %>%
                      summarise_at(vars(vars_to_summarise), .funs = list(~max(.))) %>% 
                      ungroup()

    # filter timemap to period of interest before diagnosis
    final_time_map <- final_time_map %>%
      dplyr::filter(dplyr::between(days_since_dx,-duration_prior_to_index,0)) %>% 
      dplyr::mutate(all_visits = 1)

  return(list(final_time_map = final_time_map,
              index_data = index_data,
              dx_data = dx_data,
              rx_data = rx_data))
}



