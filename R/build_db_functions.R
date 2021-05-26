

#### Write path directory ####

#' Get Diagnosis Dates for List of ICD-9 diagnoses
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param cond name of the condition
#' @param group name of group, options include 'grant', 'collab' (default is
#' 'grant')
#' @param base_path base path for output, if not specified will use group defined
#' above
#' @param years years of ccae and mdcr data to collect
#' @param medicaid_years years of medicaid data to collect
#' @param sources options include ccae, mdcr, and medicaid, default is all three
#' @param directory_path path to where directory file to write to is located, if NULL
#' the defaul location will be used
#' @param overwrite logical indicator of whether to overwrite if 'cond' exists
#'
#' @export
#'
add_directory_path <- function(cond, group = "grant", base_path = NULL,
                               years = 1:20, medicaid_years = 16:18,
                               sources = c("ccae","mdcr","medicaid"),
                               directory_path = NULL, overwrite = FALSE){

  if (is.null(directory_path)) {
    directory_path <- "/Shared/Statepi_Diagnosis/params/"
  }

  # Check if output file exists

  if (file.exists(paste0(directory_path,"small_db_paths.RData"))) {
    load(paste0(directory_path,"small_db_paths.RData"))

    if (cond %in% names(small_db_paths) & overwrite == FALSE){
      stop("Condition already exists in path directory and overwrite set to
           `FALSE`")
    }

    } else {
    small_db_paths <- list()
    }

  if (is.null(base_path)){
    if (group=="grant"){
      base_path <- "/Shared/Statepi_Diagnosis/grant_projects/"
    } else {
      base_path <- "/Shared/Statepi_Diagnosis/collab_projects/"
    }
  }

  cond_out <- list(sources = sources,
                   years = years,
                   medicaid_years = medicaid_years,
                   base_path = base_path,
                   last_build = Sys.Date(),
                   build_status = "initiated")

  small_db_paths[[cond]] <- cond_out

  save(small_db_paths,file = paste0(directory_path,"small_db_paths.RData"))

}



#### Get DX Functions ####

#' Get Diagnosis Dates for List of ICD-9 diagnoses
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param dx_list vector of diagnoses to pull visits for
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
get_dx9_dates <- function(setting,source,year,dx_list,con,collect_n=10){
  if (setting=="inpatient") {
    if (as.integer(year)<15){
      if (as.integer(year)==1){
        dat <- dplyr::tbl(con,paste0(setting,"_dx_",source,"_",year)) %>%
          dplyr::filter(dx %in% dx_list) %>%
          dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                       dplyr::mutate(disdate=admdate+los) %>%
                       dplyr::select(caseid,enrolid,admdate,disdate),
                     by="caseid") %>%
          dplyr::collect(n=collect_n) %>%
          dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
      } else {
        dat <- dplyr::tbl(con,paste0(setting,"_dx_",source,"_",year)) %>%
          dplyr::filter(dx %in% dx_list) %>%
          dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                       dplyr::select(caseid,enrolid,admdate,disdate),
                     by="caseid") %>%
          dplyr::collect(n=collect_n) %>%
          dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
      }
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx9_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                            dplyr::select(caseid,enrolid,admdate,disdate),
                   by="caseid") %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }
  } else {
    if (year<15){
      dat <- dplyr::tbl(con,paste0(setting,"_dx_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx9_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }

  }
  return(dat)
}



#' Get Diagnosis Dates for List of ICD-10 diagnoses
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param dx_list vector of diagnoses to pull visits for
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
get_dx10_dates <- function(setting,source,year,dx_list,con,collect_n=10){
  if (setting=="inpatient") {
    if (year<15){
      dat <- tibble::tibble(caseid = integer(),
                            dx = character(),
                            dx_num = integer(),
                            enrolid = bit64::integer64(),
                            admdate = integer(),
                            disdate = integer())
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx10_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::inner_join(dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
                            dplyr::select(caseid,enrolid,admdate,disdate),
                   by="caseid") %>%
        dplyr::collect(n=collect_n) %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }
  } else {
    if (year<15){
      dat <- tibble::tibble(seqnum_o = integer(),
                            enrolid = bit64::integer64(),
                            svcdate = integer(),
                            dx = character(),
                            dx_num = integer())
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_dx10_",source,"_",year)) %>%
        dplyr::filter(dx %in% dx_list) %>%
        dplyr::collect(n=collect_n)  %>%
        dplyr::mutate(enrolid = bit64::as.integer64(enrolid))
    }
  }
  return(dat)
}

#' Get Diagnosis Dates for List of ICD-9 and ICD-10 diagnoses
#'
#' This function is a wrapper around `get_dx9_dates()` and `get_dx10_dates()`
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param dx9_list vector of ICD-9 diagnoses to pull visits for
#' @param dx10_list vector of ICD-10 diagnoses to pull visits for
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
get_dx_dates <- function(setting,source,year,dx9_list,dx10_list,con,
                         collect_n=10){

  dplyr::bind_rows(get_dx9_dates(setting = setting,
                                 source = source,
                                 year = year,
                                 dx_list = dx9_list,
                                 con = con,
                                 collect_n = collect_n),
                   get_dx10_dates(setting = setting,
                                  source = source,
                                  year = year,
                                  dx_list = dx10_list,
                                  con = con,
                                  collect_n = collect_n))
}

#### Pull DB functions ####

#' Get Linked Visits (All)
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param setting "inpatient", "outpatient", "rx"
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param enrolid_table a table containing enrolids to pull data for
#' @param con a database connection
#' @param collect_n number of rows to collect
#' @param HCUP a logical indicator of whether collecting data from HCUP (vs Truven)
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'
get_linked_visits_all <- function(setting,source,year,enrolid_table,con,
                                  collect_n=10,HCUP=FALSE){
  if (HCUP==FALSE){
    if (setting=="inpatient"){
      dat <- dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
        dplyr::inner_join(enrolid_table,by="enrolid",copy=TRUE) %>%
        dplyr::collect(n=collect_n) %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    } else {
      dat <- dplyr::tbl(con,paste0(setting,"_core_",source,"_",year)) %>%
        dplyr::inner_join(enrolid_table,by="enrolid",copy=TRUE) %>%
        dplyr::collect(n=collect_n) %>%
        dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
    }
  } else {
    # con <- con[[tolower(setting)]]
    # dat <- tbl(con,paste0(setting,"_",year,"_",db,"_dte")) %>%
    #   inner_join(tbl(con,enrolid_table),by="VisitLink") %>%
    #   left_join(tbl(con,paste0(setting,"_",year,"_",db,"_core")) %>%
    #               select_(.dots=c("KEY","LOS_X",get_vars)),by="KEY") %>%
    #   collect(n=collect_n)
  }
  return(dat)
}


#' Get Linked All
#'
#' This function goes to the main database and pulls...
#'
#' @importFrom rlang .data
#'
#' @param table_name name of table to pull
#' @param source "ccae" or "mdcr" or c("ccae","mdcr")
#' @param year vector of years to collect. Note: if no argument is provided
#' all years will be selected
#' @param join_table a table containing enrolids (or other) to join on
#' @param join_by a vector of variables to join by in the join table
#' @param con a database connection
#' @param collect_n number of rows to collect
#'
#' @return A list containing three tibbles (timemap, outpatient keys, and inpatient keys)
#'
#' @export
#'
get_linked_all_truven <- function(table_name,source,year,join_table,
                                  join_by="enrolid",con,collect_n=10){
  if (join_by=="enrolid"){
    dat <- dplyr::tbl(con,paste0(table_name,"_",source,"_",year)) %>%
      dplyr::inner_join(join_table,by=join_by,copy=TRUE) %>%
      dplyr::collect(n=collect_n) %>%
      dplyr::mutate(enrolid=bit64::as.integer64(enrolid))
  } else {
    dat <- dplyr::tbl(con,paste0(table_name,"_",source,"_",year)) %>%
      dplyr::inner_join(join_table,by=join_by,copy=TRUE) %>%
      dplyr::collect(n=collect_n)
  }
  return(dat)
}
