
# DESCRIPTION: This example demonstrates how to add an indicator to a timemap 
#              for a particular diagnosis code based on order of diagnosis. 
#              Specifically, this code adds an indicator for primary versus 
#              a secondary diagnosis of CDI.


library(tidyverse)
library(smallDB)
library(codeBuildr)

# connect to database
con <- DBI::dbConnect(RSQLite::SQLite(), "/Shared/AML/small_dbs/blasto/truven/blasto.db")

# add keys to database, if not already done (uncomment if no keys)
# add_time_map_keys(collect_tab = collect_table(medicaid_years = 14:18),
#                   db_con = con,
#                   temporary = FALSE)

# build a time map
tm <- build_time_map(db_con = con)

tm %>% count(setting_type)


# collect keys for inpatient CDI
inpatient_cdi_keys <- gether_inpatient_dx_keys(collect_tab = collect_table(medicaid_years = 14:18),
                                               dx_list = load_disease_codes(c("cdi")),
                                               db_con = con) 


# add primary and secondary diagnosis indicators. Here inpatient_cdi will take on a 
# value of 1 if it is a primary diagnosis and a 2 otherwise. Note: we default to a
# primary diagnosis if the code also occurs in a secondary placeholder (in some 
# cases a code can occur in a second and primary diagnosis)
inpatient_cdi_keys <- inpatient_cdi_keys %>% 
  group_by(key) %>%                    # this and the next 2 lines are to prioritize primary diagnosis
  summarize(dx_num = min(dx_num)) %>% 
  ungroup() %>% 
  mutate(inpatient_cdi=ifelse(dx_num==1,1L,2L))


# add indicators to database
tm <- tm %>% 
  left_join(select(inpatient_cdi_keys,key,inpatient_cdi), by = "key") %>% 
  mutate(inpatient_cdi = replace_na(inpatient_cdi,0L))  # fixing missing indicators


