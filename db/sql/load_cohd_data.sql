CREATE SCHEMA cohd;
USE cohd;

-- Clear out old tables
DROP TABLE IF EXISTS cohd.dataset;
DROP TABLE IF EXISTS cohd.concept;
DROP TABLE IF EXISTS cohd.concept_relationship;
DROP TABLE IF EXISTS cohd.concept_counts;
DROP TABLE IF EXISTS cohd.concept_pair_counts;

-- Create the tables
CALL create_tables();

-- --------------------------------------------------------
-- Load the required OMOP tables
-- --------------------------------------------------------

-- How NULL is represented in the data files
SET @NULL_SERIALIZATION = 'NULL';

LOAD DATA LOCAL INFILE 'D:/cohd/ohdsi_west_cumc_20180326/concepts_all_except_licensed.txt' 
INTO TABLE concept
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\r\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, @vstandard_concept, concept_code, valid_start_date, valid_end_date, @vinvalid_reason)
SET
	standard_concept = nullif(@vstandard_concept, @NULL_SERIALIZATION),
	invalid_reason = nullif(@vinvalid_reason, @NULL_SERIALIZATION)
;

LOAD DATA LOCAL INFILE 'D:/cohd/ohdsi_west_cumc_20180326/concepts_relationships_all_except_licensed.txt' 
INTO TABLE concept_relationship
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\r\n' STARTING BY ''
IGNORE 1 LINES
(concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, @vinvalid_reason)
SET
	invalid_reason = nullif(@vinvalid_reason, @NULL_SERIALIZATION)
;

-- --------------------------------------------------------
-- 5-year dataset: 2013-2017
-- --------------------------------------------------------

-- Data set info
CALL add_dataset_info('5 year', 'Clinical data from 2013-2017');
SET @dataset_id_5year = (SELECT LAST_INSERT_ID());
CALL add_patient_count(@dataset_id_5year, 1790431);

-- Load concept_counts data 
-- Can't use prepared statements for LOAD
LOAD DATA LOCAL INFILE 'D:/cohd/ohdsi_west_cumc_20180326/concept_counts_2013-2017_randomized_mincount=11.txt'
INTO TABLE concept_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\r\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, concept_count)
SET
	dataset_id = @dataset_id_5year
;

-- Load concept_pair_counts data
-- Can't use prepared statements for LOAD
LOAD DATA LOCAL INFILE 'D:/cohd/ohdsi_west_cumc_20180326/concept_pair_counts_2013-2017_randomized_mincount=11.txt'
INTO TABLE concept_pair_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\r\n' STARTING BY ''
IGNORE 1 LINES
(concept_id_1, concept_id_2, concept_count)
SET
	dataset_id = @dataset_id_5year
;

-- --------------------------------------------------------
-- lifetime dataset
-- --------------------------------------------------------

-- Data set info
CALL add_dataset_info('Lifetime', 'Clinical data from all years in the database');
SET @dataset_id_lifetime = (SELECT LAST_INSERT_ID());
CALL add_patient_count(@dataset_id_lifetime, 5364781);

-- Load concept_counts data
-- Can't use prepared statements for LOAD
LOAD DATA LOCAL INFILE 'D:/cohd/ohdsi_west_cumc_20180326/concept_counts_0-9999_randomized_mincount=11.txt'
INTO TABLE concept_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\r\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, concept_count)
SET
	dataset_id = @dataset_id_lifetime
;

-- Load concept_pair_counts data
-- Can't use prepared statements for LOAD
LOAD DATA LOCAL INFILE 'D:/cohd/ohdsi_west_cumc_20180326/concept_pair_counts_0-9999_randomized_mincount=11.txt'
INTO TABLE concept_pair_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\r\n' STARTING BY ''
IGNORE 1 LINES
(concept_id_1, concept_id_2, concept_count)
SET
	dataset_id = @dataset_id_lifetime
;

-- --------------------------------------------------------
-- Add primary keys and indices after finished loading data
-- --------------------------------------------------------
CALL alter_tables();

-- --------------------------------------------------------
-- Delete iatrogenic codes
-- --------------------------------------------------------
CALL delete_iatrogenic();

-- Double check if there are any iatrogenic codes remaining
SELECT * FROM concept_counts cc
JOIN cohd_temp.iatrogenic i on cc.concept_id = i.concept_id
LIMIT 1;


-- --------------------------------------------------------
-- Create additional tables
-- --------------------------------------------------------
-- Create the metadata tables
CALL create_metadata_tables();

























