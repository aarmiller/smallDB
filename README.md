smallDB - A package for working with small truven databases
================

## Overview

This package contains functions for working with the small Truven
database extracts.

## Examples

Load the required packages:

``` r
library(tidyverse)
library(smallDB)
```

Connect to an example database extract:

``` r
# connect to database
con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cftr/pancreatitis_chronic.db")
```

### Create timemap

Create a longitudinal timemap. In this example visit keys have not been
added to the database, so these will be added temporarily to create the
timemap.

``` r
tm <- build_time_map(db_con = con)
```

    ## Warning in build_time_map(db_con = con): Database contains no visit keys.
    ## Temporary visit keys were generated using the collection table specified.

``` r
# view timemap object
tm
```

    ## # A tibble: 1,388,049 x 9
    ##      key year   ccae enrolid admdate disdate inpatient    rx stdplac
    ##    <int> <chr> <int>   <int>   <int>   <int>     <int> <int>   <int>
    ##  1     1 01        1       1   11333   11333         0     0      11
    ##  2     2 01        1       1   11443   11443         0     0      11
    ##  3     3 01        1       1   11571   11571         0     0      11
    ##  4     4 02        1       1   11694   11694         0     0      22
    ##  5     5 02        1       1   11695   11695         0     0      22
    ##  6     6 02        0       1   11802   11802         0     1      -2
    ##  7     7 02        0       1   11841   11841         0     0      22
    ##  8     8 02        0       1   11841   11841         0     0      41
    ##  9     9 02        0       1   11905   11905         0     0      11
    ## 10    10 02        0       1   11918   11918         0     1      -2
    ## # … with 1,388,039 more rows

### Add diagnosis indicators

The `build_dx_indicators()` function can be used to create a list of
diagnosis indicators. In this example we create a single indicator for a
single diagnosis - CDI.

``` r
# create the diagnosis list
dx_list <- list(cdi = list(icd9_codes = "00845",
                           icd10_codes = c("A047","A0471","A0472")))

# build the indicators
build_dx_indicators(db_con = con,condition_dx_list = dx_list)
```

    ## # A tibble: 847 x 2
    ##      key   cdi
    ##    <int> <int>
    ##  1  1529     1
    ##  2  6320     1
    ##  3  6326     1
    ##  4  6334     1
    ##  5  6342     1
    ##  6  6351     1
    ##  7  6698     1
    ##  8  6732     1
    ##  9  6741     1
    ## 10 16336     1
    ## # … with 837 more rows

We could also generate a list of multiple diagnoses to create multiple
indicators for. Here I will use the `codeBuildr` package to load the
diagnosis codes for AMI and CDI.

``` r
library(codeBuildr)

# load the diagnosis codes for AMI and CDI
dx_list <- load_disease_codes(c("ami","cdi"))

# view the code list
dx_list
```

    ## $ami
    ## $ami$desc
    ## [1] "Acute Myocardial Infarction"
    ## 
    ## $ami$icd9_codes
    ##  [1] "410"   "4100"  "41000" "41001" "41002" "4101"  "41010" "41011" "41012"
    ## [10] "4102"  "41020" "41021" "41022" "4103"  "41030" "41031" "41032" "4104" 
    ## [19] "41040" "41041" "41042" "4105"  "41050" "41051" "41052" "4106"  "41060"
    ## [28] "41061" "41062" "4107"  "41070" "41071" "41072" "4108"  "41080" "41081"
    ## [37] "41082" "4109"  "41090" "41091" "41092"
    ## 
    ## $ami$icd10_codes
    ##  [1] "I21"   "I210"  "I2101" "I2102" "I2109" "I211"  "I2111" "I2119" "I212" 
    ## [10] "I2121" "I2129" "I213"  "I214"  "I219"  "I21A"  "I21A1" "I21A9" "I22"  
    ## [19] "I220"  "I221"  "I222"  "I228"  "I229" 
    ## 
    ## 
    ## $cdi
    ## $cdi$desc
    ## [1] "Clostridioides difficile infection"
    ## 
    ## $cdi$icd9_codes
    ## [1] "00845"
    ## 
    ## $cdi$icd10_codes
    ## [1] "A047"  "A0471" "A0472"

``` r
# get the diagnosis indicators
build_dx_indicators(db_con = con,condition_dx_list = dx_list)
```

    ## # A tibble: 1,352 x 3
    ##      key   ami   cdi
    ##    <int> <int> <int>
    ##  1  1529     0     1
    ##  2  3276     1     0
    ##  3  3277     1     0
    ##  4  4093     1     0
    ##  5  6320     0     1
    ##  6  6326     0     1
    ##  7  6334     0     1
    ##  8  6342     0     1
    ##  9  6351     0     1
    ## 10  6698     0     1
    ## # … with 1,342 more rows

### Add medication indicators

The function `codeBuildr::load_rx_codes("all_abx")` can be used to load
the ndc codes for the group of all antibiotics.

``` r
abx_codes <- load_rx_codes("all_abx")
```

The function `build_rx_indicators()` can then be used to collect
indicators for each of the antibiotics in the `all_abx` list.

``` r
build_rx_indicators(rx_list = abx_codes,db_con = con)
```

    ## Joining, by = "ndcnum"

    ## # A tibble: 64,907 x 13
    ##        key clindamycin fluoroquinolones cephalosporins monobactams carbapenems
    ##      <int>       <int>            <int>          <int>       <int>       <int>
    ##  1   92723           1                0              0           0           0
    ##  2  127251           1                0              0           0           0
    ##  3  236970           1                0              0           0           0
    ##  4  981649           1                0              0           0           0
    ##  5 1122691           1                0              1           0           0
    ##  6 1228787           1                0              0           0           0
    ##  7 1228793           1                0              0           0           0
    ##  8   92583           1                0              0           0           0
    ##  9  760325           1                0              0           0           0
    ## 10   72744           1                0              0           0           0
    ## # … with 64,897 more rows, and 7 more variables: penicillins <int>,
    ## #   macrolides <int>, sulfonamides <int>, other <int>, tetracyclines <int>,
    ## #   glycopeptides <int>, nitroimidazoles <int>

### Close database connection

``` r
DBI::dbDisconnect(con)
```
