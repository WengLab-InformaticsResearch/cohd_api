USE cohd;

-- Delete old stored procedures
DELETE FROM mysql.proc WHERE db LIKE 'cohd';

DELIMITER //


-- Create the dataset table
CREATE PROCEDURE create_dataset_table()
BEGIN
	CREATE TABLE dataset (
	  dataset_id INT NOT NULL AUTO_INCREMENT,
	  dataset_name VARCHAR(45) NULL,
	  dataset_description VARCHAR(255) NULL,
	  PRIMARY KEY (dataset_id));
END//


-- Create the concept table
CREATE PROCEDURE create_concepts_table()
BEGIN
    -- Create the concept table
	CREATE TABLE IF NOT EXISTS concept (
	  concept_id INT(11) NOT NULL,
	  concept_name VARCHAR(255) NOT NULL,
	  domain_id VARCHAR(20) NOT NULL,
      vocabulary_id VARCHAR(20) NOT NULL,
	  concept_class_id VARCHAR(20) NOT NULL,
      standard_concept CHAR(1) NULL,
      concept_code VARCHAR(50) NOT NULL,
      valid_start_date DATE NOT NULL,
	  valid_end_date DATE NOT NULL,
	  invalid_reason CHAR(1) NULL,
      PRIMARY KEY (concept_id),
	  INDEX vocabulary_id (vocabulary_id ASC, concept_code ASC),
	  INDEX concept_class (concept_class_id ASC),
	  INDEX domain (domain_id ASC),
      INDEX concept_code (concept_code ASC));
END//


-- Create the concept table
CREATE PROCEDURE create_concept_relationship_table()
BEGIN
    -- Create the concept table
	CREATE TABLE IF NOT EXISTS concept_relationship (
	  concept_id_1 INTEGER NOT NULL,
	  concept_id_2 INTEGER NOT NULL,
	  relationship_id VARCHAR(20) NOT NULL,
	  valid_start_date DATE NOT NULL,
	  valid_end_date DATE NOT NULL,
	  invalid_reason CHAR(1) NULL,
      PRIMARY KEY (concept_id_1, concept_id_2, relationship_id),
      INDEX concept_id_1 (concept_id_1 ASC),
      INDEX concept_id_2 (concept_id_2 ASC));
END//


-- Create the concept_counts table
CREATE PROCEDURE create_concept_counts_table()
BEGIN   
	CREATE TABLE IF NOT EXISTS concept_counts (
	  dataset_id INT NOT NULL,
	  concept_id INT(11) NOT NULL,
	  concept_count INT UNSIGNED NOT NULL,
      PRIMARY KEY (dataset_id, concept_id),
      INDEX concept_count (dataset_id ASC, concept_count ASC));
END//


-- Create the concept_pair_counts table
CREATE PROCEDURE create_concept_pair_counts_table()
BEGIN
	CREATE TABLE IF NOT EXISTS concept_pair_counts (
      dataset_id INT NOT NULL,
	  concept_id_1 INT(11) NOT NULL,
	  concept_id_2 INT(11) NOT NULL,
	  concept_count INT UNSIGNED NOT NULL,
      PRIMARY KEY (dataset_id, concept_id_1, concept_id_2),
      INDEX concept_id_1_idx (dataset_id ASC, concept_id_1 ASC, concept_id_2 ASC, concept_count ASC),
	  INDEX concept_id_2_idx (dataset_id ASC, concept_id_2 ASC, concept_id_1 ASC, concept_count ASC));
END//


CREATE PROCEDURE create_tables()
BEGIN
	CALL create_dataset_table();
	CALL create_concepts_table();
	CALL create_concept_relationship_table();
	CALL create_concept_counts_table();
	CALL create_concept_pair_counts_table();
END//


-- Add a dataset
CREATE PROCEDURE add_dataset_info(IN dataset_name VARCHAR(45), IN dataset_description VARCHAR(255))
BEGIN   
    -- Insert the new dataset info
	SET @dataset_name = dataset_name;
    SET @dataset_description = dataset_description;
	PREPARE prep_stmnt FROM 'INSERT INTO dataset (dataset_name, dataset_description) VALUES(?,?);';
    EXECUTE prep_stmnt USING @dataset_name, @dataset_description;
    DEALLOCATE PREPARE prep_stmnt;
END//


-- Delete iatrogenic codes from concept_counts and concept_pair_counts
CREATE PROCEDURE delete_iatrogenic()
BEGIN       
	-- Allow delete
	SET @orig_sql_safe_updates = (SELECT @@SQL_SAFE_UPDATES);
	SET SQL_SAFE_UPDATES = 0;

	-- Delete iatrogenic data from concept_counts
	DELETE c
	FROM concept_counts c
	LEFT JOIN cohd_temp.iatrogenic 
	ON c.concept_id = iatrogenic.concept_id
	WHERE iatrogenic.concept_id IS NOT NULL;

	-- Delete iatrogenic data from concept_pair_counts
	DELETE c
	FROM concept_pair_counts c
	LEFT JOIN cohd_temp.iatrogenic i1 ON c.concept_id_1 = i1.concept_id 
	LEFT JOIN cohd_temp.iatrogenic i2 ON c.concept_id_2 = i2.concept_id
	WHERE i1.concept_id IS NOT NULL OR i2.concept_id IS NOT NULL;

	SET SQL_SAFE_UPDATES = @orig_sql_safe_updates;
END//


-- Create the metadata tables containing counts of concepts per domain and pair of domains
CREATE PROCEDURE create_metadata_tables()
BEGIN
	-- Drop existing table
    DROP TABLE IF EXISTS domain_concept_counts;
    DROP TABLE IF EXISTS domain_pair_concept_counts;
    DROP TABLE IF EXISTS patient_count;
    
    -- Create the domain_concept_counts table
    CREATE TABLE domain_concept_counts
    SELECT dataset_id, domain_id, COUNT(domain_id) as count
	FROM concept_counts cc
	JOIN concept ON cc.concept_id = concept.concept_id
	GROUP BY dataset_id, domain_id;
    
    -- Create the domain_pair_concept_counts table
	CREATE TABLE domain_pair_concept_counts
	SELECT dataset_id, IF(c1.domain_id < c2.domain_id, c1.domain_id, c2.domain_id) AS domain_id_1, 
		IF(c1.domain_id < c2.domain_id, c2.domain_id, c1.domain_id)  AS domain_id_2, COUNT(*) as count
	FROM concept_pair_counts cpc
	JOIN concept c1
	ON cpc.concept_id_1 = c1.concept_id
	JOIN concept c2
	ON cpc.concept_id_2 = c2.concept_id
	GROUP BY dataset_id, domain_id_1, domain_id_2;
    
    -- Create the patient_count table
    CREATE TABLE patient_count
	SELECT dataset_id, ROUND(AVG(concept_count / concept_frequency)) AS count
	FROM concept_counts cc
	GROUP BY dataset_id;
    
    -- Add keys and indices
	ALTER TABLE domain_concept_counts
		ADD PRIMARY KEY (dataset_id, domain_id);
	ALTER TABLE domain_pair_concept_counts
		ADD PRIMARY KEY (dataset_id, domain_id_1, domain_id_2);
	ALTER TABLE patient_count
		ADD PRIMARY KEY (dataset_id);
END//


DELIMITER ;