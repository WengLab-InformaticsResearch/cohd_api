-- Export concept, concept_relationship, and concept_ancestor data from SQL Server OMOP DB for COHD API.
--
-- The COHD API requires definitions for concepts and concept relationships. 
-- This provides two options for extracting definitions for concepts and concept relationships from
-- an OMOP database, but only one of each is needed for the COHD API. Using all concepts requires
-- more resources, but potentially provides more options for mapping concepts to source vocabularies
-- and external vocabularies. Using only the observed concepts will provide a more compact definition
-- focused on the observed data.
-- 
-- Instructions for setting output format:
-- Set file output format to tab delimited:
--     In SQL Server Management Studio: Tools > Options > Query Results > SQL Server > Results to Text >
-- 	       Output format: tab delimited
--         Include column headers in the result set: enabled
--     Restart SSMS for new settings to take effect
--
-- Turn on SQL CMD mode in SSMS: Query > SQL CMD Mode

-- Prevent the count from showing up in the text file results
SET NOCOUNT ON;


-- Export definitions for all concepts that don't come from a vocabulary that requires a license
:OUT C:\Users\username\Documents\data\concepts_all_except_licensed.txt
SELECT *
FROM concept
WHERE vocabulary_id NOT IN 
	('GPI', 'Indication', 'ETC', 'Multilex', 'CCS', 'Gemscript', 'DA_France', 'AMIS', 'EU Product', 'LPD_Australia', 'GRR');


-- Export all 'Maps to' concept relationships from vocabularies that don't require a license
:OUT C:\Users\username\Documents\data\concepts_relationships_all_except_licensed.txt
SELECT cr.*
FROM concept_relationship cr 
JOIN concept c1 ON cr.concept_id_1 = c1.concept_id
JOIN concept c2 ON cr.concept_id_2 = c2.concept_id
WHERE cr.relationship_id = 'Maps to'
	AND c1.vocabulary_id NOT IN 
	('GPI', 'Indication', 'ETC', 'Multilex', 'CCS', 'Gemscript', 'DA_France', 'AMIS', 'EU Product', 'LPD_Australia', 'GRR')
	AND c2.vocabulary_id NOT IN 
	('GPI', 'Indication', 'ETC', 'Multilex', 'CCS', 'Gemscript', 'DA_France', 'AMIS', 'EU Product', 'LPD_Australia', 'GRR');


-- Get all of the concept relationships that map to concepts that show up in the observational tables
-- Note: use union instead of union all because 0 is in each domain
IF OBJECT_ID('tempdb.dbo.#cohd_cr', 'U') IS NOT NULL
  DROP TABLE #cohd_cr; 
SELECT cr.*
INTO #cohd_cr
FROM
	(SELECT DISTINCT condition_concept_id AS concept_id FROM dbo.condition_occurrence
	UNION
	SELECT DISTINCT drug_concept_id AS concept_id FROM dbo.drug_exposure
	UNION
	SELECT DISTINCT procedure_concept_id AS concept_id FROM dbo.procedure_occurrence
	UNION
	SELECT DISTINCT gender_concept_id AS concept_id FROM dbo.person
	UNION
	SELECT DISTINCT race_concept_id AS concept_id FROM dbo.person
	UNION
	SELECT DISTINCT ethnicity_concept_id AS concept_id FROM dbo.person) concept_ids
JOIN concept_relationship cr ON concept_ids.concept_id = concept_id_2
JOIN concept ON concept.concept_id = concept_id_1
WHERE cr.relationship_id = 'Maps to'
	AND concept.vocabulary_id NOT IN 
	('GPI', 'Indication', 'ETC', 'Multilex', 'CCS', 'Gemscript', 'DA_France', 'AMIS', 'EU Product', 'LPD_Australia', 'GRR');


-- -----------------------------------------------------------------------------------------------------
-- Export the concept ancestor table where the ancestor_concept_id is an ancestor of an observed concept
-- and the descendant_concept_id is an observed concept or its ancestor
-- -----------------------------------------------------------------------------------------------------

-- Load the iatrogenic codes into temporary tables: #iatrogenic_codes and #iatrogenic_codes_with_desc
-- Iatrogenic codes and their descendants will be excluded from the analysis
:r C:\Path\To\Repo\db\sql\load_iatrogenic_codes.sql

-- Get observed concepts from conditions, drugs, and procedures
SELECT DISTINCT condition_concept_id AS concept_id
INTO #observed_condition_concepts
FROM condition_occurrence o
JOIN concept c ON o.condition_concept_id = c.concept_id
LEFT JOIN #iatrogenic_codes_with_desc i ON c.concept_id = i.concept_id
WHERE c.domain_id = 'Condition' AND i.concept_id IS NULL;

SELECT DISTINCT drug_concept_id AS concept_id
INTO #observed_drug_concepts
FROM drug_exposure o
JOIN concept c ON o.drug_concept_id = c.concept_id
LEFT JOIN #iatrogenic_codes_with_desc i ON c.concept_id = i.concept_id
WHERE c.domain_id = 'Drug' AND i.concept_id IS NULL;

SELECT DISTINCT procedure_concept_id AS concept_id
INTO #observed_procedure_concepts
FROM procedure_occurrence o
JOIN concept c ON o.procedure_concept_id = c.concept_id
LEFT JOIN #iatrogenic_codes_with_desc i ON c.concept_id = i.concept_id
WHERE c.domain_id = 'Procedure' AND i.concept_id IS NULL;

-- Get condition concepts and their ancestors
SELECT * 
INTO #hierarchical_condition_concepts
FROM
	((SELECT *
	FROM #observed_condition_concepts)
	UNION
	(SELECT DISTINCT ca.ancestor_concept_id
	FROM #observed_condition_concepts x
	JOIN concept_ancestor ca ON ca.descendant_concept_id = x.concept_id
	JOIN concept c ON ca.ancestor_concept_id = c.concept_id
	WHERE c.domain_id = 'Condition' AND c.vocabulary_id = 'SNOMED')
	UNION
	(SELECT DISTINCT ca.ancestor_concept_id
	FROM #observed_condition_concepts x
	JOIN concept_ancestor ca ON ca.descendant_concept_id = x.concept_id
	JOIN concept c ON ca.ancestor_concept_id = c.concept_id
	LEFT JOIN concept_relationship cr ON (cr.concept_id_1 = c.concept_id AND cr.relationship_id = 'MedDRA - SNOMED eq')
	WHERE c.domain_id = 'Condition' AND c.vocabulary_id = 'MedDRA' AND cr.relationship_id IS NULL)) y
;


-- Get drug concepts and their ancestors
SELECT * 
INTO #hierarchical_drug_concepts
FROM
	((SELECT *
	FROM #observed_drug_concepts)
	UNION
	(SELECT DISTINCT ca.ancestor_concept_id
	FROM #observed_drug_concepts x
	JOIN concept_ancestor ca ON ca.descendant_concept_id = x.concept_id
	JOIN concept c ON ca.ancestor_concept_id = c.concept_id
	WHERE c.domain_id = 'Drug' AND c.vocabulary_id = 'RxNorm' AND c.concept_class_id IN ('Ingredient', 'Clinical Drug Form', 'Clinical Drug Comp', 'Clinical Drug'))
	UNION
	(SELECT DISTINCT ca.ancestor_concept_id
	FROM #observed_drug_concepts x
	JOIN concept_ancestor ca ON ca.descendant_concept_id = x.concept_id
	JOIN concept c ON ca.ancestor_concept_id = c.concept_id
	LEFT JOIN concept_relationship cr ON (cr.concept_id_1 = c.concept_id AND cr.relationship_id IN ('ATC - RxNorm', 'ATC - RxNorm name'))
	WHERE c.domain_id = 'Drug' AND c.vocabulary_id = 'ATC' AND cr.relationship_id IS NULL)) y
;

-- Get procedure concepts and their ancestors
SELECT * 
INTO #hierarchical_procedure_concepts
FROM
	((SELECT *
	FROM #observed_procedure_concepts)
	UNION
	(SELECT DISTINCT ca.ancestor_concept_id
	FROM #observed_procedure_concepts x
	JOIN concept_ancestor ca ON ca.descendant_concept_id = x.concept_id
	JOIN concept c ON ca.ancestor_concept_id = c.concept_id
	WHERE c.domain_id = 'Procedure' AND c.vocabulary_id IN ('SNOMED', 'ICD10PCS'))
	UNION
	(SELECT DISTINCT ca.ancestor_concept_id
	FROM #observed_procedure_concepts x
	JOIN concept_ancestor ca ON ca.descendant_concept_id = x.concept_id
	JOIN concept c ON ca.ancestor_concept_id = c.concept_id
	LEFT JOIN concept_relationship cr ON (cr.concept_id_1 = c.concept_id AND cr.relationship_id = 'MedDRA - SNOMED eq')
	WHERE c.domain_id = 'Procedure' AND c.vocabulary_id = 'MedDRA' AND cr.relationship_id IS NULL)) y
;

-- Export the concept ancestor table where the ancestor_concept_id is an ancestor of an observed concept
-- and the descendant_concept_id is an observed concept or its ancestor
:OUT C:\Users\username\Documents\data\concept_ancestors_observed_20180914.txt
SELECT *
FROM
	((SELECT ca.*
	FROM concept_ancestor ca 
	-- Only get rows where ancestor_concept_id is an ancestor (or self) of observed concepts
	JOIN #hierarchical_condition_concepts hca ON ca.ancestor_concept_id = hca.concept_id
	-- Only get rows where descendant_concept_id is an observed concept or its ancestor
	JOIN #hierarchical_condition_concepts hcd ON ca.descendant_concept_id = hcd.concept_id)
	UNION
	(SELECT ca.*
	FROM concept_ancestor ca 
	-- Only get rows where ancestor_concept_id is an ancestor (or self) of observed concepts
	JOIN #hierarchical_drug_concepts hca ON ca.ancestor_concept_id = hca.concept_id
	-- Only get rows where descendant_concept_id is an observed concept or its ancestor
	JOIN #hierarchical_drug_concepts hcd ON ca.descendant_concept_id = hcd.concept_id)
	UNION
	(SELECT ca.*
	FROM concept_ancestor ca 
	-- Only get rows where ancestor_concept_id is an ancestor (or self) of observed concepts
	JOIN #hierarchical_procedure_concepts hca ON ca.ancestor_concept_id = hca.concept_id
	-- Only get rows where descendant_concept_id is an observed concept or its ancestor
	JOIN #hierarchical_procedure_concepts hcd ON ca.descendant_concept_id = hcd.concept_id)) tmp
ORDER BY ancestor_concept_id, descendant_concept_id
;


-- Return to normal settings
SET NOCOUNT OFF;