#!/usr/bin/env bash

# Warning: this script uses command line for password, which is not safe in a shared environment
# Prompt for MySQL password
read -sp "Enter MySQL password: " password
echo  # Move to a new line after input

# Create output directory from today's date
dir_name=$(date +%Y%m%d)
mkdir $dir_name
cd $dir_name

date

# Dump out mapping and concept data
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT * FROM biolink.mappings;" > mappings.tsv
date
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT concept_id, domain_id, concept_name FROM cohd.concept; " > concepts.tsv
date

# Dump dataset 1 (takes about 2 minutes)
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT cp.concept_id_1, cp.concept_id_2, cc1.concept_count AS concept_1_count, cc2.concept_count AS concept_2_count, cp.concept_count AS concept_pair_count, cp.dataset_id FROM cohd.concept_pair_counts cp JOIN biolink.mappings bm1 ON cp.concept_id_1 = bm1.omop_id JOIN biolink.mappings bm2 ON cp.concept_id_2 = bm2.omop_id JOIN cohd.concept_counts cc1 ON cp.concept_id_1 = cc1.concept_id AND cp.dataset_id = cc1.dataset_id JOIN cohd.concept_counts cc2 ON cp.concept_id_2 = cc2.concept_id AND cp.dataset_id = cc2.dataset_id JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id JOIN cohd.concept c1 ON cp.concept_id_1 = c1.concept_id JOIN cohd.concept c2 ON cp.concept_id_2 = c2.concept_id WHERE ((c1.domain_id != 'Drug' AND c2.domain_id != 'Drug') AND cp.dataset_id = 1);" > counts_ds1.tsv
date

# Split up dataset 3 by domain pairs. The following all work, 5-20 min each 
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT cp.concept_id_1, cp.concept_id_2, cc1.concept_count AS concept_1_count, cc2.concept_count AS concept_2_count, cp.concept_count AS concept_pair_count, cp.dataset_id FROM cohd.concept_pair_counts cp JOIN biolink.mappings bm1 ON cp.concept_id_1 = bm1.omop_id JOIN biolink.mappings bm2 ON cp.concept_id_2 = bm2.omop_id JOIN cohd.concept_counts cc1 ON cp.concept_id_1 = cc1.concept_id AND cp.dataset_id = cc1.dataset_id JOIN cohd.concept_counts cc2 ON cp.concept_id_2 = cc2.concept_id AND cp.dataset_id = cc2.dataset_id JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id JOIN cohd.concept c1 ON cp.concept_id_1 = c1.concept_id JOIN cohd.concept c2 ON cp.concept_id_2 = c2.concept_id WHERE c1.domain_id = 'Condition' AND c2.domain_id = 'Drug' AND cp.dataset_id = 3;" > counts_cd.tsv
date
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT cp.concept_id_1, cp.concept_id_2, cc1.concept_count AS concept_1_count, cc2.concept_count AS concept_2_count, cp.concept_count AS concept_pair_count, cp.dataset_id FROM cohd.concept_pair_counts cp JOIN biolink.mappings bm1 ON cp.concept_id_1 = bm1.omop_id JOIN biolink.mappings bm2 ON cp.concept_id_2 = bm2.omop_id JOIN cohd.concept_counts cc1 ON cp.concept_id_1 = cc1.concept_id AND cp.dataset_id = cc1.dataset_id JOIN cohd.concept_counts cc2 ON cp.concept_id_2 = cc2.concept_id AND cp.dataset_id = cc2.dataset_id JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id JOIN cohd.concept c1 ON cp.concept_id_1 = c1.concept_id JOIN cohd.concept c2 ON cp.concept_id_2 = c2.concept_id WHERE c1.domain_id = 'Drug' AND c2.domain_id = 'Condition' AND cp.dataset_id = 3;" > counts_dc.tsv
date
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT cp.concept_id_1, cp.concept_id_2, cc1.concept_count AS concept_1_count, cc2.concept_count AS concept_2_count, cp.concept_count AS concept_pair_count, cp.dataset_id FROM cohd.concept_pair_counts cp JOIN biolink.mappings bm1 ON cp.concept_id_1 = bm1.omop_id JOIN biolink.mappings bm2 ON cp.concept_id_2 = bm2.omop_id JOIN cohd.concept_counts cc1 ON cp.concept_id_1 = cc1.concept_id AND cp.dataset_id = cc1.dataset_id JOIN cohd.concept_counts cc2 ON cp.concept_id_2 = cc2.concept_id AND cp.dataset_id = cc2.dataset_id JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id JOIN cohd.concept c1 ON cp.concept_id_1 = c1.concept_id JOIN cohd.concept c2 ON cp.concept_id_2 = c2.concept_id WHERE c1.domain_id = 'Drug' AND c2.domain_id = 'Drug' AND cp.dataset_id = 3;" > counts_dd.tsv
date
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT cp.concept_id_1, cp.concept_id_2, cc1.concept_count AS concept_1_count, cc2.concept_count AS concept_2_count, cp.concept_count AS concept_pair_count, cp.dataset_id FROM cohd.concept_pair_counts cp JOIN biolink.mappings bm1 ON cp.concept_id_1 = bm1.omop_id JOIN biolink.mappings bm2 ON cp.concept_id_2 = bm2.omop_id JOIN cohd.concept_counts cc1 ON cp.concept_id_1 = cc1.concept_id AND cp.dataset_id = cc1.dataset_id JOIN cohd.concept_counts cc2 ON cp.concept_id_2 = cc2.concept_id AND cp.dataset_id = cc2.dataset_id JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id JOIN cohd.concept c1 ON cp.concept_id_1 = c1.concept_id JOIN cohd.concept c2 ON cp.concept_id_2 = c2.concept_id WHERE c1.domain_id = 'Drug' AND c2.domain_id = 'Procedure' AND cp.dataset_id = 3;" > counts_dp.tsv
date
mysql -h tr-kp-clinical-db.ncats.io -u admin --password=$password --connect-timeout=3600 -e "SELECT cp.concept_id_1, cp.concept_id_2, cc1.concept_count AS concept_1_count, cc2.concept_count AS concept_2_count, cp.concept_count AS concept_pair_count, cp.dataset_id FROM cohd.concept_pair_counts cp JOIN biolink.mappings bm1 ON cp.concept_id_1 = bm1.omop_id JOIN biolink.mappings bm2 ON cp.concept_id_2 = bm2.omop_id JOIN cohd.concept_counts cc1 ON cp.concept_id_1 = cc1.concept_id AND cp.dataset_id = cc1.dataset_id JOIN cohd.concept_counts cc2 ON cp.concept_id_2 = cc2.concept_id AND cp.dataset_id = cc2.dataset_id JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id JOIN cohd.concept c1 ON cp.concept_id_1 = c1.concept_id JOIN cohd.concept c2 ON cp.concept_id_2 = c2.concept_id WHERE c1.domain_id = 'Procedure' AND c2.domain_id = 'Drug' AND cp.dataset_id = 3;" > counts_pd.tsv
date
