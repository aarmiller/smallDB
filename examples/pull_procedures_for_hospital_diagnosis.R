
# DESCRIPTION: This file provides an example for pulling in all the procedure codes
#              that correspond to hospital visits for a particular diagnosis. Specifically,
#              in this example we pull in all of the procedures that occured during
#              inpatient visits for CDI


library(tidyverse)
library(smallDB)
library(codeBuildr)

# connect to database
con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cf/cf.db")

# add visit keys to database, if not already done (uncomment this if there are if no 
# visit keys already in the database) NOTE: you need this step for the following
# operations to work properly

# add_time_map_keys(collect_tab = collect_table(medicaid_years = 14:18),
#                   db_con = con,
#                   temporary = TRUE)


# pull visit keys for the hospitalizations where CDI occured
inpatient_cdi_visit_keys <- gether_inpatient_dx_keys(collect_tab = collect_table(medicaid_years = 14:18),
                                                     dx_list = load_disease_codes(c("cdi")),
                                                     db_con = con) 


# get the inpatient procedures
inpatient_procs <- gether_table_data(collect_tab = collect_table(medicaid_years = 14:18),
                                     table = "inpatient_proc",
                                     vars = c("caseid","proc"),
                                     db_con = con)

# get the inpatient visit keys
inpatient_keys <- con %>% tbl("inpatient_keys") %>% collect()

# add caseids into cdi visits
inpatient_cdi_visit_keys <- inpatient_cdi_visit_keys %>% 
  inner_join(select(inpatient_keys,year,source_type,caseid,key),by = "key")

# join the inpatient procs with their corresponding CDI visit keys
inpatient_procs <- inpatient_procs %>% 
  dplyr::mutate(source_type = ifelse(source=="ccae",1L,
                                     ifelse(source=="mdcr",2L,3L))) %>% 
  select(year,source_type,table_data) %>% 
  unnest(table_data) %>% 
  inner_join(inpatient_cdi_visit_keys, by = c("year","source_type","caseid"))


# count procedure codes associated with the CDI visits
inpatient_procs %>% 
  count(proc) %>% 
  arrange(desc(n))
