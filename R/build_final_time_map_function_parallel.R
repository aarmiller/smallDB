#' Builds a time map containing visit specific information for a specific condition and cohort (in parallel).
#' @name build_final_time_map_parallel
#' @param condition_short_name The condition of interest. Specifically the short_name version of the condition
#' @param duration_prior_to_index How far from the index should the time map extend back (e.g. 90, 180, 365)
#' @param cohort_path Path to the specific cohort for the condition of interest
#' @param collect_tab a collection table to build time_map from. By default all years will be used
#' @param ssd_list A list of diagnosis codes of interest. The diagnosis codes need to be seperated into
#' diagnosis categories (e.g. cough, fever, etc.). Additionally, within the categories, diagnosis codes should be seperated into ICD 9 and
#' ICD 10 specific codes, with list elements labled as icd9_codes and icd10_codes
#' @param num_cores The number of worker cores to use. If not specified will determined the number of cores based on the which ever
#' is the smallest value between number of rows in for collect_tab or detected number of cores - 1
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

build_final_time_map_parallel <- function (condition_short_name, duration_prior_to_index = 365L,
                                           num_cores = NULL, cohort_path, ssd_list, collect_tab = collect_table()){

    #update all_dx to include dx date instead of time to dx (this is necessary as index dates are shifted do time to dx needs to be recalculated)
    all_dx <- all_dx  %>%
    inner_join(index_data %>% select(enrolid, index_date)) %>%
    mutate(visit_date = index_date + days_since_index) %>%
    select(-days_since_index, - index_date)

    # Get subset of enrolids that will be used in the analysis
    final_cohort <- read_rds(cohort_path)

    index_data <- final_cohort %>%
      mutate(time_before_index = index_date - first_enroll) %>%
      select(enrolid, index_date , enroll_first = first_enroll, time_before_index, first_dx_source = index_dx_source, stdplac, key)

    #update all_dx to include adjusted days_since_index using new index dates also select for enrolids in cohort
    all_dx <- all_dx  %>%
      inner_join(index_data %>% select(enrolid, index_date)) %>%
      mutate(days_since_index = visit_date - index_date) %>%
      select(-visit_date, - index_date)

    # select for enroilid that were enrolled >=duration_prior_to_index days prior to index
    enrolled_ge_1year <- index_data %>% filter(time_before_index >= duration_prior_to_index) %>% distinct(enrolid) %>% .$enrolid
    index_data <- index_data %>% filter(time_before_index >= duration_prior_to_index)
    dx_data <- all_dx %>% filter(enrolid %in% enrolled_ge_1year) %>% rename(days_since_dx=days_since_index) %>%
      filter(between(days_since_dx,-duration_prior_to_index,0))
    rx_data <- all_rx %>% inner_join(index_data %>% select(enrolid, index_date)) %>%
      mutate(days_since_rx = rx_date - index_date) %>%
      filter(between(days_since_rx,-duration_prior_to_index,0)) %>% select(enrolid, rx, days_since_rx)

    # build timemap
    time_map <- build_time_map_parallel(db_con = db_con,
                                        collect_tab = collect_tab,
                                        db_path = db_path,
                                        num_cores = num_cores)
    gc()

    # subset TB timemap to cases of interest
    time_map <- time_map %>% inner_join(index_data %>%
                                          distinct(enrolid, index_date))
    # add first_dx visit
    time_map <-  time_map %>% left_join(index_data %>%
                                          select(enrolid, admdate = index_date, source = first_dx_source, stdplac, key) %>%
                                          mutate(first_dx = 1)) %>%
      mutate(first_dx = ifelse(is.na(first_dx), 0, first_dx))

    # create inpatient, ED, and outpatient indicators
    time_map <- time_map %>% mutate(ind = 1L) %>% distinct() %>% filter(!is.na(source)) %>%
      spread(key = source, value = ind) %>%
      mutate_at(vars(ed, inpatient, outpatient, rx),~ifelse(is.na(.),0L,.))

    # compute time to first diagnosis
    time_map <- time_map %>%
      group_by(enrolid) %>%
      arrange(enrolid, admdate) %>%
      mutate(days_since_dx= admdate - index_date) %>%
      mutate(visit_no=row_number()) %>%
      ungroup()

    ssd_inds <- build_dx_indicators_parallel(condition_dx_list = ssd_list,
                                             db_con = db_con,
                                             collect_tab = collect_tab,
                                             db_path = db_path,
                                             num_cores = num_cores) %>%
      mutate(any_ssd=1L)

    # Make final timemap with condition indicators
    # extract names of the condition indicators
    ind_names <- ssd_inds %>%
      select(-key) %>%
      names()

    # add indicators to time_map
    final_time_map <- time_map %>%
      left_join(ssd_inds, by="key") %>%
      mutate_at(vars(all_of(ind_names)),.funs = list(~ifelse(is.na(.),0L,.)))

    # filter timemap to period of interest before diagnosis
    final_time_map <- final_time_map %>%
      filter(between(days_since_dx,-duration_prior_to_index,0))

    # get distinct visit (distinct enrolid, days_since_dx, std_place). If on a given "distinct" visit, if it was labeled
    # as both and outpatient and ED, it should be labeled as an ED visit and not an outpatient visit.
      vars_to_summarise <- c("inpatient", "ed", "rx", "first_dx", ind_names)

      final_time_map <- final_time_map %>%
        group_by(enrolid, days_since_dx, stdplac) %>%
        summarise_at(vars(all_of(vars_to_summarise)),.funs = list(~max(.))) %>%
        ungroup() %>%
        mutate(outpatient = ifelse(inpatient == 0 & ed == 0  & rx == 0, 1, 0)) %>%
        mutate(all_visits = 1)

  return(list(final_time_map = final_time_map,
              index_data = index_data,
              dx_data = dx_data,
              rx_data = rx_data))
}



