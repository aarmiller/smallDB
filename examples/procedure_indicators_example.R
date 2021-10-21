
# DESCRIPTION: This example demonstrates how to add indicators to a timemap for
#              a given procedure code.


library(tidyverse)
library(smallDB)
library(codeBuildr)

# connect to database
con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cf/cf.db")

# build a time map
tm <- build_time_map(db_con = con)

# load the mechancical ventilation codes from the codeBuildr package
mech_vent_codes <- load_procedure_codes("mechanical_ventilation")

# gather keys for mechanical ventilation
mech_vent_keys <- gether_proc_keys(proc_list = mech_vent_codes,
                                   db_con = con)

# add mechanical ventilation indicators to the timemap
tm %>% 
  left_join(distinct(mech_vent_keys,key) %>% 
              mutate(mech_vent = 1L),
            by = "key") %>% 
  mutate(mech_vent = replace_na(mech_vent,0L))


