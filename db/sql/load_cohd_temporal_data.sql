-- ####################################################################
-- TIPS: Use the following settings for more efficient load
-- Pre-sort the data files in the index order of the destination table
-- ####################################################################
SET autocommit = 0;
SET unique_checks = 0;
SET foreign_key_checks = 0;
-- SET sql_log_bin = 0;  -- Don't have permission for this
-- scripts to load data
COMMIT;


USE cohd;

-- How NULL is represented in the data files
SET @NULL_SERIALIZATION = 'NULL';


-- Add column to dataset table to indicate whether the dataset supports temporal analysis
-- new values default to false, as desired
ALTER TABLE dataset
	ADD temporal BOOLEAN NOT NULL;




-- Update the concept table with new definitions from latest OMOP database
LOAD DATA LOCAL INFILE '/path/to/files/concepts2.csv'
REPLACE
INTO TABLE concept
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, @vstandard_concept, concept_code, valid_start_date, valid_end_date, @vinvalid_reason)
SET
	standard_concept = nullif(@vstandard_concept, @NULL_SERIALIZATION),
	invalid_reason = nullif(@vinvalid_reason, @NULL_SERIALIZATION)
;


-- Add the new dataset description    
INSERT INTO dataset 
	(dataset_name, dataset_description, temporal)
    VALUES('Temporal beta', 'Beta data set capturing temporal relationships between pairs of concepts', true);

SET @dataset_id = (SELECT LAST_INSERT_ID());


-- Add patient count
INSERT INTO patient_count (dataset_id, count) VALUES(@dataset_id, 4536187);
    
    
-- Load the single concept counts
LOAD DATA LOCAL INFILE '/path/to/files/concept_counts.csv'
INTO TABLE concept_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, concept_count)
SET
	dataset_id = @dataset_id;


-- Load concept_pair_counts data
LOAD DATA LOCAL INFILE '/path/to/files/concept_pair_counts.csv'
INTO TABLE concept_pair_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id_1, concept_id_2, concept_count)
SET
	dataset_id = @dataset_id;
    

-- Update the domain_concept_counts table
INSERT INTO domain_concept_counts
SELECT dataset_id, domain_id, COUNT(domain_id) as count
FROM concept_counts cc
JOIN concept ON cc.concept_id = concept.concept_id
WHERE dataset_id = @dataset_id
GROUP BY dataset_id, domain_id;


-- Update the domain_pair_concept_counts table
INSERT INTO domain_pair_concept_counts
SELECT dataset_id, IF(c1.domain_id < c2.domain_id, c1.domain_id, c2.domain_id) AS domain_id_1, 
	IF(c1.domain_id < c2.domain_id, c2.domain_id, c1.domain_id)  AS domain_id_2, COUNT(*) as count
FROM concept_pair_counts cpc
JOIN concept c1 ON cpc.concept_id_1 = c1.concept_id
JOIN concept c2 ON cpc.concept_id_2 = c2.concept_id
WHERE dataset_id = @dataset_id
GROUP BY dataset_id, domain_id_1, domain_id_2;


-- ####################################################################
-- Creating new tables
-- ####################################################################
    
CREATE TABLE IF NOT EXISTS concept_age_counts (
  dataset_id INT NOT NULL,
  concept_id INT(11) NOT NULL,
  bin TINYINT UNSIGNED NOT NULL,
  count INT UNSIGNED NOT NULL);


CREATE TABLE IF NOT EXISTS delta_counts (
  dataset_id INT NOT NULL,
  concept_id_1 INT(11) NOT NULL,
  concept_id_2 INT(11) NOT NULL,
  bin TINYINT NOT NULL,
  count INT UNSIGNED NOT NULL);
  
  
CREATE TABLE IF NOT EXISTS concept_age_schemes (
  dataset_id INT NOT NULL,
  concept_id INT(11) NOT NULL,
  bin_width TINYINT UNSIGNED NOT NULL,
  bins TINYINT UNSIGNED NOT NULL);


CREATE TABLE IF NOT EXISTS delta_schemes (
  dataset_id INT NOT NULL,
  concept_id_1 INT(11) NOT NULL,
  concept_id_2 INT(11) NOT NULL,
  bin_width TINYINT UNSIGNED NOT NULL,
  n TINYINT UNSIGNED NOT NULL);


-- ####################################################################
-- Load new tables
-- ####################################################################

-- Load the concept age counts
LOAD DATA LOCAL INFILE '/path/to/files/age_distributions.csv'
INTO TABLE concept_age_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, bin, count)
SET
	dataset_id = @dataset_id;

-- Add primary key to concept_age_counts
ALTER TABLE concept_age_counts
  ADD PRIMARY KEY (dataset_id, concept_id, bin);


-- Load the concept age schemes
LOAD DATA LOCAL INFILE '/path/to/files/age_schemes.csv'
INTO TABLE concept_age_schemes
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id, bin_width, bins)
SET
	dataset_id = @dataset_id;
    
-- Add primary key to concept_age_schemes
ALTER TABLE concept_age_schemes
  ADD PRIMARY KEY (dataset_id, concept_id);



-- Load the delta counts
LOAD DATA LOCAL INFILE '/path/to/files/deltas.csv'
INTO TABLE delta_counts
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id_1, concept_id_2, bin, count)
SET
	dataset_id = @dataset_id;

-- Add primary key to delta_counts
ALTER TABLE delta_counts
  ADD PRIMARY KEY (dataset_id, concept_id_1, concept_id_2, bin);


-- Load the delta schemes
LOAD DATA LOCAL INFILE '/path/to/files/delta_schemes.csv'
INTO TABLE delta_schemes
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY "\\"
LINES TERMINATED BY '\n' STARTING BY ''
IGNORE 1 LINES
(concept_id_1, concept_id_2, bin_width, n)
SET
	dataset_id = @dataset_id;
COMMIT;

-- Add primary key to delta_schemes
ALTER TABLE delta_schemes
  ADD PRIMARY KEY (dataset_id, concept_id_1, concept_id_2);



