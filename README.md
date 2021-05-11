smallDB - A package for working with small truven databases
================

## Overview

## Examples

``` r
library(tidyverse)
```

    ## ── Attaching packages ─────────────────────────────────────── tidyverse 1.3.1 ──

    ## ✓ ggplot2 3.3.3     ✓ purrr   0.3.4
    ## ✓ tibble  3.1.1     ✓ dplyr   1.0.5
    ## ✓ tidyr   1.1.3     ✓ stringr 1.4.0
    ## ✓ readr   1.4.0     ✓ forcats 0.5.1

    ## ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
    ## x dplyr::filter() masks stats::filter()
    ## x dplyr::lag()    masks stats::lag()

``` r
library(smallDB)
```

``` r
# connect to database
con <- DBI::dbConnect(RSQLite::SQLite(), "~/Data/cftr/pancreatitis_chronic.db")
```

### Create timemap

Create timemap, in this example visit keys have not been added to the
database, so these will be added temporarily to create the timemap.

``` r
tm <- build_time_map(db_con = con)
```

    ## Warning in build_time_map(db_con = con): Database contains no visit keys.
    ## Temporary visit keys were generated using the collection table specified.

``` r
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

### Add indicators

Create a list of diagnosis indicators. In this example we add indicators
for CDI.

``` r
dx_list <- list(cdi = list(icd9_codes = "00845",
                           icd10_codes = c("A047","A0471","A0472")))

dx_list
```

    ## $cdi
    ## $cdi$icd9_codes
    ## [1] "00845"
    ## 
    ## $cdi$icd10_codes
    ## [1] "A047"  "A0471" "A0472"

``` r
build_dx_indicators(db_con = con,condition_dx_list = dx_list)
```

    ## Adding missing grouping variables: `source`, `year`

    ## # A tibble: 847 x 3
    ##      key   cdi any_ind
    ##    <int> <int>   <int>
    ##  1  1529     1       1
    ##  2  6320     1       1
    ##  3  6326     1       1
    ##  4  6334     1       1
    ##  5  6342     1       1
    ##  6  6351     1       1
    ##  7  6698     1       1
    ##  8  6732     1       1
    ##  9  6741     1       1
    ## 10 16336     1       1
    ## # … with 837 more rows

### Close database connection

``` r
DBI::dbDisconnect(con)
```
