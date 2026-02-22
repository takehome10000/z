# payments engine

## summary

a lock free, no share work engine where we distribute workloads across avaliable cores on the machine

worker threads are pinned at a 1:1 ratio to each core

accounts remain on the cores they are initaited on throughout the processing lifecycle

a work stealing algorithm is not implemented so all cores may not be fully saturated depending
on the distribution of our input transactions' clients

apache arrow is used due its wide spread use in the data engineering / ml world due to nice mmap / parquet, zero copy,
and other features.  i thought it would be cool to work on this skill.

## use 

`cargo run -- $CSV_INPUT > $CSV_OUTPUT`

## future work

* allow white spaces in the csv - i ran out of time before i could the wrangle apache arrow format
* zero copy serialization when creating transactions from apache arrow csv records
* use a scheduler with an effective work stealing algorithm i.e tokio scheduler or rayon for
  distributing workloads 
* much more ...

## assumptions

* clients are not skewed and are evenly distributed across transaction inputs
* transactions per client are ordered 'chronologically' the transactions can't come in out of order

## artifical intellligence use

claude chat website ui was used to generate `is_csv`, `resolve_path`, and `apache arrow type casting` in `io's consume` when `casting` arrow types to be used in `tx`
