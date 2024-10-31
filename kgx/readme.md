# Info

Create KGX dump from COHD database.

## Overview
1. Dump data from COHD database
2. Scripts reproduce COHD TRAPI behavior to creates nodes and edges jsonl files

## Notes
1. MySQL server runs out of memory if I try to pull all data at once, so I split the query up
2. Reproduce COHD TRAPI default behavior of using dataset 3 (5-year hierarchical) whenver drugs involved in the 
response, otherwise dataset 1 (5-year non-hierarchical)

# Instructions

## Configuration
1. Install mysql-client  
   `sudo apt install mysql-client`
1. Install python requirements  
   `conda install numpy scipy`

## Run 

1. Run queries to dump from MySQL database (takes ~45 min)  
`./dump_cohd_mysql.sh`
1. Run python script to generate KGX files (takes ~3 hours on laptop) 
`python kgx_cohd.py`
