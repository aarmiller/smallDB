
library(tidyverse)
library(smallDB)

# db_con <- DBI::dbConnect(RSQLite::SQLite(), "/Shared/AML/truven_extracts/small_dbs/cftr_main_cohort/cftr_main_cohort.db")

db_con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cftr/cftr_sinusitis_chronic/cftr_sinusitis_chronic.db")

######################################
#### Load Conditions for Analysis ####
######################################

dx_list <- map(codeBuildr::load_disease_codes(c("asthma","bronchitis_acute","bronchitis_chronic","bronchiectasis",
                                                "unspec_upper_resp","pneumonia_gn","pneumonia_mrsa",
                                                "pneumonia_recurrent","pseudomonas_pneumonia","resp_failure",
                                                "nasal_polyps","rsv","mrsa",
                                                "gerd")),~.[c("icd9_codes","icd10_codes")])


###################
#### Functions ####
###################


gether_dx_visits <- function(dx_list,db_con){
  ## Setup collection steps ------------------------------------------------------
  
  dx_list <- list(icd9_codes=purrr::map(dx_list,~.$icd9_codes) %>% unlist(use.names = F),
                  icd10_codes=purrr::map(dx_list,~.$icd10_codes) %>% unlist(use.names = F))
  
  icd_9_codes <- dx_list$icd9_codes
  icd_10_codes <- dx_list$icd10_codes
  
  plan <- collect_plan(db_con)
  
  ## Get Inpatient Diagnoses -----------------------------------------------------
  
  
  
  temp.in1 <- plan %>%
    dplyr::filter(as.integer(year)<15) %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_9_codes) %>%
                                     dplyr::distinct(caseid,dx,dx_num) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(any_of(c("caseid","patient_id","admdate","disdate","los"))),
                                                       by="caseid") %>%
                                     dplyr::distinct() %>%
                                     dplyr::collect(n=Inf))) 
  
  temp.in1 <- temp.in1 %>% 
    unnest(data) %>% 
    mutate(disdate = ifelse(is.na(disdate),admdate+los,disdate)) %>% 
    select(-los) %>% 
    mutate(dx_ver = 9L)
  
  
  temp.in2 <- plan %>%
    dplyr::filter(as.integer(year)>14) %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_9_codes) %>%
                                     dplyr::distinct(caseid,dx,dx_num) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(any_of(c("caseid","patient_id","admdate","disdate"))),
                                                       by="caseid") %>%
                                     dplyr::distinct() %>%
                                     dplyr::collect(n=Inf)))
  
  temp.in2 <- temp.in2 %>% 
    unnest(data) %>% 
    mutate(dx_ver = 9L)
  
  
  temp.in3 <- plan %>%
    dplyr::filter(as.integer(year)>14) %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_10_codes) %>%
                                     dplyr::distinct(caseid,dx,dx_num) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(any_of(c("caseid","patient_id","admdate","disdate"))),
                                                       by="caseid") %>%
                                     dplyr::distinct() %>%
                                     dplyr::collect(n=Inf)))
  
  temp.in3 <- temp.in3 %>% 
    unnest(data) %>% 
    mutate(dx_ver = 10L)
  
  temp.in <- dplyr::bind_rows(temp.in1,temp.in2,temp.in3) %>% 
    distinct(patient_id,dx,dx_ver,dx_num,caseid,admdate,disdate)
  
  rm(temp.in1,temp.in2,temp.in3)
  
  ## Get Inpatient Services Diagnoses --------------------------------------------
  
  temp.in_serv <- db_con %>% 
    tbl("inpatient_services_dx") %>% 
    dplyr::filter((dx %in% icd_9_codes & dx_ver==9) | (dx %in% icd_10_codes & dx_ver==10)) %>% 
    distinct(patient_id,caseid,svcdate,admdate,disdate,dx,dx_ver) %>% 
    collect()
  
  
  ## Get Outpatient Services Diagnoses -------------------------------------------
  temp.out1 <- plan %>%
    dplyr::filter(as.integer(year)<15) %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_9_codes) %>%
                                     dplyr::distinct(seqnum_o,patient_id,svcdate,dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(patient_id,svcdate,stdplac,dx) %>%
                                     dplyr::collect(n=Inf)))
  
  temp.out1 <- temp.out1 %>% 
    unnest(data) %>% 
    mutate(dx_ver = 9L)
  
  temp.out2 <- plan %>%
    dplyr::filter(as.integer(year)>=15) %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_9_codes) %>%
                                     dplyr::distinct(seqnum_o,patient_id,svcdate,dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(patient_id,svcdate,stdplac,dx) %>%
                                     dplyr::collect(n=Inf)))
  
  temp.out2 <- temp.out2 %>% 
    unnest(data) %>% 
    mutate(dx_ver = 9L)
  
  temp.out3 <- plan %>%
    dplyr::filter(as.integer(year)>=15) %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_10_codes) %>%
                                     dplyr::distinct(seqnum_o,patient_id,svcdate,dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(patient_id,svcdate,stdplac,dx) %>%
                                     dplyr::collect(n=Inf)))
  
  temp.out3 <- temp.out3 %>% 
    unnest(data) %>% 
    mutate(dx_ver = 10L)
  
  temp.out <- dplyr::bind_rows(temp.out1,temp.out2,temp.out3) %>%
    distinct(patient_id,dx,dx_ver,svcdate,stdplac)
  
  rm(temp.out1,temp.out2,temp.out3)
  
  ## Get Facility Headers --------------------------------------------------------
  
  db_con %>% tbl("facility_dx_ccae_12")
  
  temp.fac <- plan %>%
    filter(as.integer(year)>1) %>% 
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("facility_dx_",.x,"_",.y)) %>%
                                     dplyr::filter((dx %in% icd_9_codes & dx_ver==9) | (dx %in% icd_10_codes & dx_ver==10)) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("facility_core_",.x,"_",.y)) %>%
                                                         dplyr::select(any_of(c("fachdid","caseid"))),
                                                       by="fachdid") %>%
                                     dplyr::distinct(patient_id,svcdate,caseid,dx,dx_ver,dx_num) %>%
                                     dplyr::mutate(caseid = as.integer(caseid)) %>% 
                                     dplyr::collect(n=Inf))) 
  
  temp.fac <- temp.fac %>% 
    unnest(data) %>% 
    mutate(caseid = ifelse(caseid==0,NA,caseid)) %>% 
    distinct(patient_id,svcdate,caseid,dx,dx_ver,dx_num)
  
  temp.fac_in = temp.fac %>% filter(!is.na(caseid))
  temp.fac_out = temp.fac %>% filter(is.na(caseid)) %>% select(-caseid)
  
  out <- list(inpatient = temp.in,
              outpatient = temp.out,
              in_serv = temp.in_serv,
              facility_in = temp.fac_in,
              facility_out = temp.fac_out)
  
  return(out)
}

collapse_dx_visits <- function(visit_list, expand_inpat_dates=TRUE, preserve_dx_order = FALSE){
  
  # expand inpatient dates
  tmp.in <- visit_list$inpatient %>% 
    mutate(svcdate = map2(admdate,disdate,~.x:.y)) %>% 
    unnest(svcdate) %>%
    distinct(patient_id,caseid,admdate,disdate,svcdate)
  
  # isolate inpatient svc dates in the services and facility table
  tmp.in_fac_serv <- bind_rows(distinct(visit_list$in_serv,patient_id,svcdate),
                               distinct(visit_list$facility_in,patient_id,svcdate)) %>% 
    distinct()
  
  # if expanding to all inpatient visit date
  if (expand_inpat_dates) {
    
    tmp.in <- tmp.in %>% 
      distinct(patient_id,svcdate)
    
  } else {
    
    # exclude if svcdate is provided in facility or inpatient services
    in_exclude <- tmp.in %>% 
      inner_join(tmp.in_fac_serv,by = join_by(patient_id, svcdate)) %>% 
      distinct(patient_id,caseid,admdate,disdate)
    
    # remove the cases
    tmp.in <- tmp.in %>% 
      anti_join(in_exclude,
                by = join_by(patient_id, caseid, admdate, disdate))
    
  }
  
  # combine distinct svcdate
  
  out <- bind_rows(tmp.in,  #remaining inpatient
                   tmp.in_fac_serv,   #inpatient service and facility dates
                   distinct(visit_list$facility_out,patient_id,svcdate),  # facility outpatient
                   distinct(visit_list$outpatient,patient_id,svcdate)) %>%   # general outpatient
    distinct(patient_id,svcdate)
  
  if (preserve_dx_order) {
    
    tmp.in.prime <- visit_list$inpatient %>% 
      filter(dx_num == 1) %>% 
      mutate(svcdate = map2(admdate,disdate,~.x:.y)) %>% 
      unnest(svcdate) %>%
      distinct(patient_id,svcdate)
    
    tmp.in_fac.prime <- distinct(filter(visit_list$facility_in,dx_num==1),patient_id,svcdate)
    
    prime_vis <- bind_rows(tmp.in.prime,
                           tmp.in_fac.prime) %>% 
      distinct(patient_id,svcdate) %>% 
      mutate(prime = 1L)
    
    out <- left_join(out,prime_vis, by = join_by(patient_id, svcdate))
    
  }
 
  return(out)
   
}

separate_dx_visit_list <- function(dx_list,visit_list){
  
  # create list of nested code tibbles
  tmp <- dx_list %>% 
    code_tibble() %>% 
    distinct() %>% 
    group_by(name) %>% 
    nest()
  
  # output list
  out_list <- list()
  
  # loop over codes in code tibble
  for (i in 1:nrow(tmp)){
    
    cond_name <- tmp$name[[i]]
    
    out_list[[cond_name]] <- map(visit_list,~inner_join(.,tmp$data[[i]],by = join_by(dx, dx_ver)))
    
  }
  
  return(out_list)
  
}

code_tibble <- function(code_set){
  tmp1 <- map(code_set,~.$icd9_codes) %>% 
    enframe() %>% 
    unnest(value) %>% 
    select(name,dx=value) %>% 
    mutate(dx_ver=9L)
  
  tmp2 <- map(code_set,~.$icd10_codes) %>% 
    enframe() %>% 
    unnest(value) %>% 
    select(name,dx=value) %>% 
    mutate(dx_ver=10L)
  
  bind_rows(tmp1,tmp2) %>% 
    arrange(name,dx_ver)
}

find_dx_visits <- function(dx_list,db_con){
  
  # collect visits
  tmp_dx_visits <- gether_dx_visits(dx_list,db_con)
  
  # separate visits
  tmp_dx_visits <- separate_dx_visit_list(dx_list = dx_list,
                                          visit_list = tmp_dx_visits)
  
  
  out_list <- map(tmp_dx_visits,collapse_dx_visits)
  
  return(out_list)
  
}


###################
#### Analysis #####
###################

dx_visits <- find_dx_visits(dx_list, db_con)



map2(tmp,names(tmp),~mutate(.x,!!.y:= 1L)) %>% 
  reduce(full_join, by = c("patient_id","svcdate"))

load("~/Data/cftr/cftr_sinusitis_chronic/study_cohort_all.RData")

tmp2 <- study_cohort %>% 
  select(patient_id,case,strata) %>% 
  left_join(tmp$asthma %>% 
              distinct(patient_id) %>% 
              mutate(outcome = 1L)) %>% 
  mutate(outcome = replace_na(outcome,0L))

tmp <- glm(case~outcome,data=tmp2,family = binomial)

exp(tmp$coefficients["outcome"])
exp(confint.default(tmp)["outcome",])


library(survival)
tmp3 <- clogit(case~outcome + strata(strata), data = tmp2)
tmp4 <- exp(confint.default(tmp3))

tibble(OR = exp(tmp3$coefficients["outcome"]),
       low = tmp4[1],
       high = tmp4[2])

exp(confint.default(tmp3))

run_risk_models <- function(dx_visit_data,study_data){
  
  tmp <- map(dx_visit_data,~run_single_risk_model(.x,study_data)) 
  
  map2(tmp,names(tmp),~mutate(.x, condition = .y)) %>% 
    bind_rows() %>% 
    select(condition,everything())
}


run_single_risk_model(visit_data = dx_visits$unspec_upper_resp,
                      study_data = study_cohort)


run_risk_models(dx_visit_data = dx_visits,
                study_data = study_cohort)

visit_data <- dx_visits$bronchiectasis
study_data <-  study_cohort

tmp <- map(dx_visits,~run_single_risk_model(.x,study_cohort)) 

map2(tmp,names(tmp),~mutate(.x, condition = .y)) %>% 
  bind_rows() %>% 
  select(condition,everything())

run_single_risk_model <- function(visit_data,study_data){
  
  tmp_out_inds <- visit_data %>% 
    distinct(patient_id) %>% 
    mutate(outcome = 1L)
  
  reg_data <- study_data %>% 
    select(patient_id,case,strata) %>% 
    left_join(tmp_out_inds, by = join_by(patient_id)) %>% 
    mutate(outcome = replace_na(outcome,0L))
  
  fit_glm <- glm(case~outcome, data = reg_data,family = "binomial")
  ci_glm <- exp(confint.default(fit_glm)["outcome",])
  
  tmp1 <- tibble(model = "base",
                 OR = exp(fit_glm$coefficients["outcome"]),
                 low = ci_glm[1],
                 high = ci_glm[2])
  
 fit_cl <- clogit(case~outcome + strata(strata), data = reg_data)
 ci_cl <- exp(confint.default(fit_cl))
 
 tmp2 <- tibble(model = "clogit",
                OR = exp(fit_cl$coefficients["outcome"]),
                low = ci_cl[1],
                high = ci_cl[2])
 
 bind_rows(tmp1,tmp2)
  
}

exp(0.34158)

tmp2 %>% count(case,outcome)

(801/1999)/(3103/10897)

tmp$asthma %>% 
  distinct(patient_id) %>% 
  mutate(outcome = 1L) %>% 
  left_join(study_cohort %>% 
              select(patient_id,case)) %>% 
  count(case,outcome)

study_cohort %>% 
  select(patient_id,case) 


tmp[[1]]["test"]=1L


tmp_visit_list <- gether_dx_visits(dx_list,db_con)

tmp_cond_visit_list <- separate_visit_list(dx_list = dx_list,
                                           visit_list = tmp_visit_list)

map(out_list,collapse_dx_visits)

tmp <- dx_list %>% 
  code_table() %>% 
  distinct() %>% 
  group_by(name) %>% 
  nest()


out_list <- list()

for (i in 1:nrow(tmp)){
  # print(tmp$name[[i]])
  
  cond_name <- tmp$name[[i]]
  
  out_list[[cond_name]] <- map(tmp_visit_list,~inner_join(.,tmp$data[[i]],by = join_by(dx, dx_ver)))
  
}

map(out_list,collapse_dx_visits)

# map(tmp_visit_list,~inner_join(.,tmp$data[[13]],by = join_by(dx, dx_ver)))



tmp_visit_list %>% collapse_dx_visits()


tmp.in.prime <- tmp_visit_list$inpatient %>% 
  filter(dx_num == 1) %>% 
  mutate(svcdate = map2(admdate,disdate,~.x:.y)) %>% 
  unnest(svcdate) %>%
  distinct(patient_id,svcdate)

tmp.in_fac.prime <- distinct(filter(tmp_visit_list$facility_in,dx_num==1),patient_id,svcdate)

prime_vis <- bind_rows(tmp.in.prime,
                       tmp.in_fac.prime)

tmp.in_fac_serv.prime <- bind_rows(distinct(visit_list$in_serv,patient_id,svcdate),
                                   distinct(visit_list$facility_in,patient_id,svcdate)) %>% 
  distinct()


collapse_dx_visits(tmp_visit_list,
                   expand_inpat_dates = TRUE)

collapse_dx_visits(tmp_visit_list,
                   expand_inpat_dates = FALSE)

collapse_dx_visits(tmp_visit_list,
                   expand_inpat_dates = TRUE,
                   preserve_dx_order = TRUE) 

collapse_dx_visits(tmp_visit_list,
                   expand_inpat_dates = FALSE,
                   preserve_dx_order = TRUE) 

# bind_rows(distinct(tmp_visit_list$outpatient,patient_id,svcdate),
#           distinct(tmp_visit_list$facility_out,patient_id,svcdate),
#           distinct(tmp_visit_list$facility_in,patient_id,svcdate),
#           distinct(tmp_visit_list$in_serv,patient_id,svcdate)) %>% 
#   distinct(patient_id,svcdate)

# need to create a function to partition a set of dx visit


## Setup collection steps ------------------------------------------------------

dx_list <- list(icd9_codes=purrr::map(dx_list,~.$icd9_codes) %>% unlist(use.names = F),
                icd10_codes=purrr::map(dx_list,~.$icd10_codes) %>% unlist(use.names = F))

icd_9_codes <- dx_list$icd9_codes
icd_10_codes <- dx_list$icd10_codes

plan <- collect_plan(db_con)

## Get Inpatient Diagnoses -----------------------------------------------------



temp.in1 <- plan %>%
  dplyr::filter(as.integer(year)<15) %>%
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("inpatient_dx_",.x,"_",.y)) %>%
                                   dplyr::filter(dx %in% icd_9_codes) %>%
                                   dplyr::distinct(caseid,dx,dx_num) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                       dplyr::select(any_of(c("caseid","patient_id","admdate","disdate","los"))),
                                                     by="caseid") %>%
                                   dplyr::distinct() %>%
                                   dplyr::collect(n=Inf))) 

temp.in1 <- temp.in1 %>% 
  unnest(data) %>% 
  mutate(disdate = ifelse(is.na(disdate),admdate+los,disdate)) %>% 
  select(-los) %>% 
  mutate(dx_ver = 9L)


temp.in2 <- plan %>%
  dplyr::filter(as.integer(year)>14) %>%
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("inpatient_dx9_",.x,"_",.y)) %>%
                                   dplyr::filter(dx %in% icd_9_codes) %>%
                                   dplyr::distinct(caseid,dx,dx_num) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                       dplyr::select(any_of(c("caseid","patient_id","admdate","disdate"))),
                                                     by="caseid") %>%
                                   dplyr::distinct() %>%
                                   dplyr::collect(n=Inf)))

temp.in2 <- temp.in2 %>% 
  unnest(data) %>% 
  mutate(dx_ver = 9L)


temp.in3 <- plan %>%
  dplyr::filter(as.integer(year)>14) %>%
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("inpatient_dx10_",.x,"_",.y)) %>%
                                   dplyr::filter(dx %in% icd_10_codes) %>%
                                   dplyr::distinct(caseid,dx,dx_num) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                       dplyr::select(any_of(c("caseid","patient_id","admdate","disdate"))),
                                                     by="caseid") %>%
                                   dplyr::distinct() %>%
                                   dplyr::collect(n=Inf)))

temp.in3 <- temp.in3 %>% 
  unnest(data) %>% 
  mutate(dx_ver = 10L)

temp.in <- dplyr::bind_rows(temp.in1,temp.in2,temp.in3) %>% 
  distinct(patient_id,dx,dx_ver,dx_num,caseid,admdate,disdate)

rm(temp.in1,temp.in2,temp.in3)

## Get Inpatient Services Diagnoses --------------------------------------------

temp.in_serv <- db_con %>% 
  tbl("inpatient_services_dx") %>% 
  dplyr::filter((dx %in% icd_9_codes & dx_ver==9) | (dx %in% icd_10_codes & dx_ver==10)) %>% 
  distinct(patient_id,caseid,svcdate,admdate,disdate,dx,dx_ver) %>% 
  collect()
  

## Get Outpatient Services Diagnoses -------------------------------------------
temp.out1 <- plan %>%
  dplyr::filter(as.integer(year)<15) %>%
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("outpatient_dx_",.x,"_",.y)) %>%
                                   dplyr::filter(dx %in% icd_9_codes) %>%
                                   dplyr::distinct(seqnum_o,patient_id,svcdate,dx) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                       dplyr::select(seqnum_o,stdplac),
                                                     by="seqnum_o") %>%
                                   dplyr::distinct(patient_id,svcdate,stdplac,dx) %>%
                                   dplyr::collect(n=Inf)))

temp.out1 <- temp.out1 %>% 
  unnest(data) %>% 
  mutate(dx_ver = 9L)

temp.out2 <- plan %>%
  dplyr::filter(as.integer(year)>=15) %>%
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("outpatient_dx9_",.x,"_",.y)) %>%
                                   dplyr::filter(dx %in% icd_9_codes) %>%
                                   dplyr::distinct(seqnum_o,patient_id,svcdate,dx) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                       dplyr::select(seqnum_o,stdplac),
                                                     by="seqnum_o") %>%
                                   dplyr::distinct(patient_id,svcdate,stdplac,dx) %>%
                                   dplyr::collect(n=Inf)))

temp.out2 <- temp.out2 %>% 
  unnest(data) %>% 
  mutate(dx_ver = 9L)

temp.out3 <- plan %>%
  dplyr::filter(as.integer(year)>=15) %>%
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("outpatient_dx10_",.x,"_",.y)) %>%
                                   dplyr::filter(dx %in% icd_10_codes) %>%
                                   dplyr::distinct(seqnum_o,patient_id,svcdate,dx) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                       dplyr::select(seqnum_o,stdplac),
                                                     by="seqnum_o") %>%
                                   dplyr::distinct(patient_id,svcdate,stdplac,dx) %>%
                                   dplyr::collect(n=Inf)))

temp.out3 <- temp.out3 %>% 
  unnest(data) %>% 
  mutate(dx_ver = 10L)

temp.out <- dplyr::bind_rows(temp.out1,temp.out2,temp.out3) %>%
  distinct(patient_id,dx,dx_ver,svcdate,stdplac)

rm(temp.out1,temp.out2,temp.out3)

## Get Facility Headers --------------------------------------------------------

db_con %>% tbl("facility_dx_ccae_12")

temp.fac <- plan %>%
  filter(as.integer(year)>1) %>% 
  dplyr::mutate(data=purrr::map2(source,year,
                                 ~dplyr::tbl(db_con,paste0("facility_dx_",.x,"_",.y)) %>%
                                   dplyr::filter((dx %in% icd_9_codes & dx_ver==9) | (dx %in% icd_10_codes & dx_ver==10)) %>%
                                   dplyr::inner_join(dplyr::tbl(db_con,paste0("facility_core_",.x,"_",.y)) %>%
                                                       dplyr::select(any_of(c("fachdid","caseid"))),
                                                     by="fachdid") %>%
                                   dplyr::distinct(patient_id,svcdate,caseid,dx,dx_ver,dx_num) %>%
                                   dplyr::mutate(caseid = as.integer(caseid)) %>% 
                                   dplyr::collect(n=Inf))) 

temp.fac <- temp.fac %>% 
  unnest(data) %>% 
  mutate(caseid = ifelse(caseid==0,NA,caseid)) %>% 
  distinct(patient_id,svcdate,caseid,dx,dx_ver,dx_num)

list(inpatient = temp.in,
     outpatient = temp.out,
     in_serv = temp.in_serv,
     facility = temp.fac)






gether_dx_keys <- function(collect_tab=collect_table(),dx_list,db_con){
  
  
  if (!("icd9_codes" %in% names(dx_list))) {
    dx_list <- list(icd9_codes=purrr::map(dx_list,~.$icd9_codes) %>% unlist(use.names = F),
                    icd10_codes=purrr::map(dx_list,~.$icd10_codes) %>% unlist(use.names = F))
  }
  
  icd_9_codes <- dx_list$icd9_codes
  icd_10_codes <- dx_list$icd10_codes
  
  ##### get inpatient diagnoses ####
  in_temp1 <- collect_tab %>%
    dplyr::filter(as.integer(year)<15,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_9_codes) %>%
                                     dplyr::distinct(caseid,dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(caseid,enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(enrolid,caseid,dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(enrolid))))
  
  in_temp2 <- collect_tab %>%
    dplyr::filter(as.integer(year)>14,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_9_codes) %>%
                                     dplyr::distinct(caseid,dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(caseid,enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(enrolid,caseid,dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(enrolid))))
  
  in_temp3 <- collect_tab %>%
    dplyr::filter(as.integer(year)>14,
                  setting == "inpatient") %>%
    dplyr::mutate(data=purrr::map2(source,year,
                                   ~dplyr::tbl(db_con,paste0("inpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::filter(dx %in% icd_10_codes) %>%
                                     dplyr::distinct(caseid,dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("inpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(caseid,enrolid),
                                                       by="caseid") %>%
                                     dplyr::distinct(.data$enrolid,.data$caseid,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  in_temp <- dplyr::bind_rows(in_temp1,in_temp2,in_temp3) %>%
    dplyr::select(-.data$setting) %>%
    tidyr::unnest(cols = c(data)) %>%
    dplyr::group_by(source,.data$year) %>%
    tidyr::nest()
  
  rm(in_temp1,in_temp2,in_temp3)
  
  #### get outpatient diagnoses ####
  out_temp1 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)<15,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  out_temp2 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx9_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_9_codes) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  out_temp3 <- collect_tab %>%
    dplyr::filter(as.integer(.data$year)>14,
                  setting == "outpatient") %>%
    dplyr::mutate(data=purrr::map2(source,.data$year,
                                   ~dplyr::tbl(db_con,paste0("outpatient_dx10_",.x,"_",.y)) %>%
                                     dplyr::filter(.data$dx %in% icd_10_codes) %>%
                                     dplyr::distinct(seqnum_o,.data$enrolid,.data$svcdate,.data$dx) %>%
                                     dplyr::inner_join(dplyr::tbl(db_con,paste0("outpatient_core_",.x,"_",.y)) %>%
                                                         dplyr::select(seqnum_o,.data$stdplac),
                                                       by="seqnum_o") %>%
                                     dplyr::distinct(.data$enrolid,.data$svcdate,.data$stdplac,.data$dx) %>%
                                     dplyr::collect(n=Inf) %>%
                                     dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid))))
  
  
  out_temp <- dplyr::bind_rows(out_temp1,out_temp2,out_temp3) %>%
    dplyr::select(-.data$setting) %>%
    tidyr::unnest(cols = c(data)) %>%
    dplyr::group_by(source,.data$year) %>%
    tidyr::nest()
  rm(out_temp1,out_temp2,out_temp3)
  ### Merge in the keys ####
  # Merge inpatient keys
  in_dx_keys <- db_con %>%
    dplyr::tbl("inpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$source_type,.data$year,.data$caseid,.data$key) %>%
    dplyr::inner_join(in_temp %>%
                        dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                                           ifelse(source=="mdcr",2L,3L))) %>% 
                        tidyr::unnest(cols = c(data)),
                      by = c("source_type", "year", "caseid")) %>%
    dplyr::select(.data$dx,.data$key)
  
  ## Merge outpatient keys
  out_dx_keys <- db_con %>%
    dplyr::tbl("outpatient_keys") %>%
    dplyr::collect(n=Inf) %>%
    dplyr::mutate(enrolid=bit64::as.integer64(.data$enrolid)) %>%
    dplyr::select(.data$enrolid,.data$stdplac,.data$svcdate,.data$key) %>%
    dplyr::inner_join(out_temp %>%
                        ungroup() %>%
                        dplyr::select(.data$data) %>%
                        tidyr::unnest(cols = c(data)),
                      by = c("enrolid", "stdplac", "svcdate")) %>%
    dplyr::select(.data$key,.data$dx)
  
  dx_keys <- dplyr::bind_rows(in_dx_keys,out_dx_keys) %>%
    dplyr::distinct()
  
  #### Return ####
  return(dx_keys)
}