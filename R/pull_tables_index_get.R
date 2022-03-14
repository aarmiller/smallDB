
#' Collect all diagnoses that occur in proximity to an index date
#'
#' Collect all inpatient visits along with the diagnosis order
#'
#' @param source ccae, mdcr or medicaid
#' @param year year (as integer value)
#' @param enrolid_table a table containing enrolid and index_date
#' @param days_before an integer for number of days before index date to look
#' @param days_after an integer for number of days after index date to look
#' @param db_con a connection to a database,
#' @param collect_n the number of rows to collect
#' @return A tibble of variables from the respective table
#'
#' @export
get_index_dx_visits <- function(source,year,enrolid_table,days_before,days_after,db_con,collect_n = Inf){
  
  tmp_core <-get_core_data(setting = "inpatient",
                           source = source,
                           year = year,
                           vars = c("caseid","enrolid","admdate"),
                           db_con = db_con,
                           collect_n = collect_n)
  
  tmp_core <- tmp_core %>% 
    dplyr::inner_join(enrolid_table, by = "enrolid") %>% 
    dplyr::filter(admdate>=(index_date-days_before), admdate<=(index_date+days_after))
  
  tmp_dx <- get_all_inpatient_dx_visits(source = source,
                                        year = year,
                                        dx_num = TRUE,
                                        db_con = db_con,
                                        collect_n = collect_n)
  
  out1 <- tmp_core %>% 
    dplyr::inner_join(tmp_dx, by = "caseid") %>% 
    dplyr::distinct(enrolid,dx,dx_ver)
  
  if (as.integer(year)<=14){
    
    tmp_out_dx <- get_table_data(table = "outpatient_dx",
                                 source = source,
                                 year = year,
                                 vars = c("enrolid","svcdate","dx"),
                                 db_con = db_con,
                                 collect_n = collect_n) %>% 
      dplyr::mutate(dx_ver = 9L)
  } else {
    
    tmp_out_dx1 <- get_table_data(table = "outpatient_dx9",
                                  source = source,
                                  year = year,
                                  vars = c("enrolid","svcdate","dx"),
                                  db_con = db_con,
                                  collect_n = collect_n) %>% 
      dplyr::mutate(dx_ver = 9L)
    
    tmp_out_dx2 <- get_table_data(table = "outpatient_dx10",
                                  source = source,
                                  year = year,
                                  vars = c("enrolid","svcdate","dx"),
                                  db_con = db_con,
                                  collect_n = collect_n) %>% 
      dplyr::mutate(dx_ver = 10L)
    
    tmp_out_dx <- dplyr::bind_rows(tmp_out_dx1,tmp_out_dx2)
  }
  
  out2 <- tmp_out_dx %>% 
    dplyr::inner_join(enrolid_table, by = "enrolid") %>% 
    dplyr::filter(svcdate>=(index_date-days_before), svcdate<=(index_date+days_after)) %>% 
    dplyr::distinct(enrolid,dx,dx_ver)
  
  out <- dplyr::bind_rows(out1,out2)
  
  return(out)
  
}
