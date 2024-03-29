% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pull_tables_gether.R
\name{gether_table_data}
\alias{gether_table_data}
\title{Gather (gether) data from all tables defined in a collection tab}
\usage{
gether_table_data(
  collect_tab = collect_table(),
  table,
  vars = c(),
  db_con,
  collect_n = Inf
)
}
\arguments{
\item{collect_tab}{a collection table}

\item{table}{name of table to collect data from}

\item{vars}{variables to collect}

\item{db_con}{a connection to a database,}

\item{collect_n}{the number of rows to collect}
}
\description{
Gather (gether) data from all tables defined in a collection tab
}
