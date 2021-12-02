# DESCRIPTION: This example demonstrates how to pull a set of procedure codes
#              from the facilities headers tables


library(tidyverse)
library(smallDB)

# connect to database
con <- DBI::dbConnect(RSQLite::SQLite(), "/Shared/AML/small_dbs/fourniers/truven/fourniers_old.db")

# put together a list of procedure codes to pass to the gether function
proc_list <- list(icd9pcs_codes = c("3893","9999","8622"),
                  icd10pcs_codes = c("02HV33Z","30233N1","5A1D60Z"),
                  cpt_codes = c())

# gather the facility visits using `gether_facility_procs()`
tmp <- gether_facility_procs(collect_tab = collect_table(years = 1:17),
                             proc_list = proc_list,
                             db_con = con)

# view output
tmp

