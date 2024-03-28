
rm(list = ls())
library(tidyverse)
library(smallDB)

# db_con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cftr/cftr_main_cohort/cftr_main_cohort.db")
db_con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cftr/cftr_sinusitis_chronic/cftr_sinusitis_chronic.db")


plan <- collect_plan(db_con)

# db_con %>% tbl("inpatient_core_ccae_12") %>% names()

###################
#### functions ####
###################
add_ed_obs_indicator <- function(data){
  out <- data %>% 
    # ED Stay
    dplyr::mutate(setting_type = ifelse((stdplac %in% c(23) |
                                           ((stdplac %in% c(19,21,22,28)) &
                                              (stdprov %in% c("220","428"))) |
                                           ((stdplac %in% c(19,21,22,28)) &
                                              svcscat %in% c("10120","10220","10320","10420","10520",
                                                             "12220","20120","20220","21120","21220",
                                                             "22120","22320","30120","30220","30320",
                                                             "30420","30520","30620","31120","31220",
                                                             "31320","31420","31520","31620")) |
                                           (procgrp %in% c("110","111","114")) |
                                           (revcode %in% c("450","451","452","453","454",
                                                           "455","456","457","458","459")) |
                                           (proc1 %in% c("99281","99282","99283","99284","99285"))), 2L, setting_type)) %>%
    # Observational Stay
    dplyr::mutate(setting_type = ifelse(proc1 %in% c("99218", "99219", "99220", "99224", "99225", 
                                                     "99226", "99234", "99235", "99236"), 3L, setting_type)) %>% 
    dplyr::select(any_of(c("year", "source_type", "enrolid","patient_id", "admdate", "disdate","svcdate", "setting_type", "stdplac","stdprov","svcscat")))
  
  return(out)
}

########################
#### Collect Visits ####
########################


### get inpatient visits -----------------------------------------------------
temp.in <- plan %>%
  dplyr::mutate(setting="inpatient") %>%
  gether_core_data(vars = c("patient_id","caseid","admdate","disdate","los"),db_con = db_con)

temp.in <- temp.in %>%
  dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                     ifelse(source=="mdcr",0L,2L))) %>%
  dplyr::select("year","source_type","core_data") %>%
  tidyr::unnest(cols = c(core_data)) %>%
  dplyr::mutate(disdate=ifelse(is.na(disdate),admdate+los,disdate),
                setting_type=5L) %>%
  dplyr::select(-los) %>% 
  distinct() %>% 
  mutate(year = as.integer(year))

### get outpatient visits ----------------------------------------------------
temp.out <- plan %>%
  dplyr::mutate(setting = "outpatient") %>% 
  gether_core_data(vars = c("patient_id", "stdplac", "svcdate", "stdprov",
                            "svcscat", "procgrp", "revcode", "proc1"),
                   db_con = db_con) %>%
  dplyr::mutate(core_data=purrr::map(core_data,~dplyr::distinct(.)))

# clean outpatient visits
temp.out <- temp.out %>%
  dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                     ifelse(source=="mdcr",0L,2L))) %>%
  dplyr::select("year","source_type","core_data") %>%
  mutate(core_data = map(core_data, ~mutate(., procgrp = as.character(procgrp),
                                            stdprov = as.character(stdprov)))) %>%
  tidyr::unnest(cols = c(core_data)) %>%
  dplyr::mutate(setting_type = 1L) %>%
  distinct()

# Identify ED visits
temp.out <-  temp.out %>% 
  add_ed_obs_indicator() %>% 
  distinct() %>% 
  mutate(year = as.integer(year))


### get facility header visits -----------------------------------------------
temp.fac <- plan %>%
  filter(year != "01") %>% 
  dplyr::mutate(setting = "facility") %>% 
  gether_core_data(vars = c("patient_id", "stdplac", "svcdate", "stdprov","caseid",
                            "svcscat", "procgrp", "revcode","fachdid"),
                   db_con = db_con)

temp.fac <- temp.fac %>% 
  dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                     ifelse(source=="mdcr",0L,2L))) %>%
  dplyr::select("year","source_type","core_data") %>%
  mutate(core_data = map(core_data, ~mutate(.,stdprov = as.character(stdprov),
                                            caseid = as.integer(caseid)))) %>%
  tidyr::unnest(cols = c(core_data)) 

# filter to outpatient 
temp.fac.out <- temp.fac %>% 
  filter(caseid==0 | is.na(caseid)) %>%
  mutate(setting_type = 1L) %>% 
  mutate(svcscat=NA,procgrp=NA,proc1=NA,revcode=NA) %>% 
  add_ed_obs_indicator() %>% 
  distinct()

# filter to inpatient
temp.fac.in <- temp.fac %>% 
  filter(!(caseid==0 | is.na(caseid))) %>% 
  mutate(setting_type = 5L) %>% 
  distinct(year,source_type,patient_id,svcdate,setting_type,stdplac,stdprov,caseid)


### get inpatient services -----------------------------------------------------

temp.in_serv <- db_con %>% 
  tbl("inpatient_services_core") %>% 
  collect()

temp.in_serv <- temp.in_serv %>% 
  distinct(patient_id,year,source_type=source,caseid,admdate,disdate,svcdate,stdplac,stdprov,svcscat)


### get rx visits --------------------------------------------------------------
temp.rx <- plan %>%
  dplyr::mutate(setting="rx") %>%
  gether_rx_dates(db_con = db_con)

# clean rx visits
temp.rx <- temp.rx %>%
  dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                     ifelse(source=="mdcr",0L,2L))) %>%
  dplyr::select("year","source_type","rx_data") %>%
  tidyr::unnest(cols = c(rx_data)) %>%
  dplyr::mutate(setting_type = 4L) %>%
  dplyr::distinct()



#######################
#### Build Timemap ####
#######################


# primary inpatient
tm.in <- temp.in %>% 
  distinct(patient_id,caseid,admdate,disdate) %>% 
  mutate(date_span = map2(admdate,disdate,~.x:.y)) %>% 
  unnest(date_span) %>% 
  distinct(patient_id,svcdate=date_span,caseid,admdate,disdate) %>% 
  mutate(in_db=1L)

# Inpatient services missing disdate
tm.in_serv1 <- temp.in_serv %>% 
  distinct(patient_id,caseid,admdate,disdate,svcdate) %>% 
  filter(is.na(disdate)) %>% 
  select(-disdate) %>% 
  left_join(distinct(temp.in,patient_id,caseid,admdate,disdate),by = join_by(patient_id, caseid, admdate))

# Inpatient services non-missing disdate
tm.in_serv2 <- temp.in_serv %>% 
  distinct(patient_id,caseid,admdate,disdate,svcdate) %>% 
  filter(!is.na(disdate)) 

tm.in_serv <- bind_rows(tm.in_serv1,tm.in_serv2) %>% mutate(in_serv_db = 1L)
rm(tm.in_serv1,tm.in_serv2)

# Inpatient facility headers
tm.fac_in <- temp.fac.in %>% 
  distinct(patient_id,svcdate,caseid) %>% 
  mutate(fac_in_db=1L)

## Add Inpatient to timemap ----------------------------------------------------  
# Create two timemaps: tm - with separate rows for each caseid

tm_full <- tm.in %>% 
  full_join(tm.in_serv,
            by = join_by(patient_id, svcdate, caseid, admdate, disdate)) %>% 
  full_join(tm.fac_in,by = join_by(patient_id, svcdate, caseid)) %>% 
  mutate(inpatient=1L)

tm_collapsed <- distinct(tm.in,patient_id,svcdate,in_db) %>% 
  full_join(distinct(tm.in_serv,patient_id,svcdate,in_serv_db),by = join_by(patient_id, svcdate)) %>% 
  full_join(distinct(tm.fac_in,patient_id,svcdate,fac_in_db),by = join_by(patient_id, svcdate)) %>% 
  mutate(inpatient=1L)

rm(tm.in,tm.in_serv,tm.fac_in)


## Assemble outpatient visits --------------------------------------------------

# Outpatient Services
tmp_out_serv <- temp.out %>% 
  distinct(patient_id,svcdate,setting_type)

# Outpatient Facility Headers
tmp_fac_out <- temp.fac.out %>% 
  distinct(patient_id,svcdate,setting_type)

# Outpatient Events
tm.out <- bind_rows(tmp_out_serv %>%
                    filter(setting_type==1),
                  tmp_fac_out %>%
                    filter(setting_type==1)) %>% 
  distinct(patient_id,svcdate) %>% 
  mutate(outpatient = 1L)

# ED Events
tm.ed <- bind_rows(tmp_out_serv %>%
                     filter(setting_type==2),
                   tmp_fac_out %>%
                     filter(setting_type==2)) %>% 
  distinct(patient_id,svcdate) %>% 
  mutate(ed = 1L)

# Obs Stays
tm.obs <- bind_rows(tmp_out_serv %>%
                      filter(setting_type==3),
                    tmp_fac_out %>%
                      filter(setting_type==3)) %>% 
  distinct(patient_id,svcdate) %>% 
  mutate(obs_stay = 1L)

tm.out <- tm.out %>% 
  full_join(tm.ed,by = join_by(patient_id, svcdate)) %>% 
  full_join(tm.obs,by = join_by(patient_id, svcdate)) 

# create db indicators

tmp1 <- tmp_out_serv %>% 
  distinct(patient_id,svcdate) %>% 
  mutate(out_serv_db =1L)

tmp2 <- tmp_fac_out %>% 
  distinct(patient_id,svcdate) %>% 
  mutate(fac_out_db =1L)

out_db_inds <- tmp1 %>% 
  full_join(tmp2,by = join_by(patient_id, svcdate))


## Add outpatient to tm --------------------------------------------------------
tm_full <- tm_full %>% 
  full_join(tm.out,join_by(patient_id, svcdate)) %>% 
  full_join(out_db_inds,join_by(patient_id, svcdate))

tm_collapsed <- tm_collapsed %>% 
  full_join(tm.out,join_by(patient_id, svcdate)) %>% 
  full_join(out_db_inds,join_by(patient_id, svcdate))

## Add rx to timemap -----------------------------------------------------------

tm.rx <- temp.rx %>% 
  distinct(patient_id,svcdate) %>% 
  mutate(rx = 1L)


tm_full <- tm_full %>% 
  full_join(tm.rx,by = join_by(patient_id, svcdate))

tm_collapsed <- tm_collapsed %>% 
  full_join(tm.rx,by = join_by(patient_id, svcdate))


## Build setting source indicators ---------------------------------------------

tmp1 <- temp.in %>% 
  distinct(patient_id,source_type,admdate,disdate) %>% 
  mutate(svcdate = map2(admdate,disdate,~.x:.y)) %>% 
  unnest(svcdate) %>% 
  distinct(patient_id,svcdate,source_type)

tmp2 <- distinct(temp.out,patient_id,svcdate,source_type)

tmp3 <- distinct(temp.in_serv,patient_id,svcdate,source_type)

tmp4 <- distinct(temp.fac,patient_id,svcdate,source_type)

tmp5 <- distinct(temp.rx,patient_id,svcdate,source_type)

source_inds <- bind_rows(tmp1,tmp2,tmp3,tmp4,tmp5) %>% distinct()

rm(tmp1,tmp2,tmp3,tmp4,tmp5)

tmp1 <- source_inds %>% 
  filter(source_type==0) %>% 
  mutate(mdcr=1L) %>% 
  select(patient_id,svcdate,mdcr)

tmp2 <- source_inds %>% 
  filter(source_type==1) %>% 
  mutate(ccae=1L) %>% 
  select(patient_id,svcdate,ccae)

tmp3 <- source_inds %>% 
  filter(source_type==2) %>% 
  mutate(medicaid=1L) %>% 
  select(patient_id,svcdate,medicaid)

source_inds <- full_join(tmp1,tmp2,by = join_by(patient_id, svcdate)) %>% 
  full_join(tmp3,by = join_by(patient_id, svcdate)) %>% 
  mutate_at(vars(mdcr:medicaid),~replace_na(.,0L))


## Finalize timemap ------------------------------------------------------------

tm_full <- mutate_at(tm_full,vars(in_db:rx),~replace_na(.,0L)) %>% 
  select(patient_id,svcdate,caseid,admdate,disdate,outpatient,ed,obs_stay,inpatient,rx,in_db,in_serv_db,
         fac_in_db,out_serv_db,fac_out_db) %>% 
  arrange(patient_id,svcdate)


tm_collapsed <- mutate_at(tm_collapsed,vars(in_db:rx),~replace_na(.,0L)) %>% 
  select(patient_id,svcdate,outpatient,ed,obs_stay,inpatient,rx,in_db,in_serv_db,
         fac_in_db,out_serv_db,fac_out_db) %>% 
  arrange(patient_id,svcdate)

tm_full <- tm_full %>% 
  inner_join(source_inds,by = join_by(patient_id, svcdate)) %>% 
  select(patient_id,svcdate,mdcr,ccae,medicaid,everything())

tm_collapsed <- tm_collapsed %>% 
  inner_join(source_inds,by = join_by(patient_id, svcdate)) %>% 
  select(patient_id,svcdate,mdcr,ccae,medicaid,everything())

## write tm to db --------------------------------------------------------------

copy_to(dest = db_con,
        df = tm_full,
        name = "tm_full",
        temporary = FALSE,
        indexes = list("patient_id"),
        analyze = TRUE,
        overwrite = TRUE)

copy_to(dest = db_con,
        df = tm_collapsed,
        name = "tm",
        temporary = FALSE,
        indexes = list("patient_id"),
        analyze = TRUE,
        overwrite = TRUE)

# db_con %>% tbl("tm_full")
# db_con %>% tbl("tm")

  
##################################
#### Build STDPLAC indicators ####
##################################

tm_stdplac <- bind_rows(distinct(temp.out,patient_id,svcdate,stdplac),
                        distinct(temp.fac.out,patient_id,svcdate,stdplac),
                        distinct(temp.fac.in,patient_id,svcdate,stdplac),
                        distinct(temp.in_serv,patient_id,svcdate,stdplac) %>% 
                          mutate(stdplac=as.integer(stdplac))) %>% 
  distinct(patient_id,svcdate,stdplac) %>% 
  filter(!is.na(stdplac))

copy_to(dest = db_con,
        df = tm_stdplac,
        name = "stdplac_visits",
        temporary = FALSE,
        indexes = list("patient_id"),
        analyze = TRUE,
        overwrite = TRUE)

# db_con %>% tbl("stdplac_visits")


##################################
#### Build STDPROV indicators ####
##################################

tm_stdprov <- bind_rows(distinct(temp.out,patient_id,svcdate,stdprov) %>% 
                          mutate(stdprov=as.integer(stdprov)),
                        distinct(temp.fac.out,patient_id,svcdate,stdprov) %>% 
                          mutate(stdprov=as.integer(stdprov)),
                        distinct(temp.fac.in,patient_id,svcdate,stdprov) %>% 
                          mutate(stdprov=as.integer(stdprov)),
                        distinct(temp.in_serv,patient_id,svcdate,stdprov) %>% 
                          mutate(stdprov=as.integer(stdprov))) %>% 
  distinct(patient_id,svcdate,stdprov) %>% 
  filter(!is.na(stdprov))

copy_to(dest = db_con,
        df = tm_stdprov,
        name = "stdprov_visits",
        temporary = FALSE,
        indexes = list("patient_id"),
        analyze = TRUE,
        overwrite = TRUE)

# db_con %>% tbl("stdprov_visits")

##################################
#### Build SVCSCAT indicators ####
##################################

tm_svcscat <- bind_rows(temp.out %>% 
                          filter(!is.na(svcscat)) %>% 
                          distinct(patient_id,svcdate,svcscat),
                        temp.in_serv %>% 
                          filter(!is.na(svcscat)) %>% 
                          distinct(patient_id,svcdate,svcscat)) %>% 
  distinct(patient_id,svcdate,svcscat)

copy_to(dest = db_con,
        df = tm_svcscat,
        name = "svcscat_visits",
        temporary = FALSE,
        indexes = list("patient_id"),
        analyze = TRUE,
        overwrite = TRUE)


#################################
#### Build Inpatient Overlay ####
#################################

# Come back to this...

# We need to figure out how to work with this when we want to differentiate different
# hospital stays, including overlapping and nested stays

DBI::dbDisconnect(db_con)
rm(list = ls())
