CREATE SCHEMA IF NOT EXISTS biolink;

USE biolink;

CREATE TABLE IF NOT EXISTS mappings (
    omop_id INT NOT NULL,
    biolink_id VARCHAR(255) NOT NULL,
    biolink_label VARCHAR(511) NULL,
    categories JSON NOT NULL,
    provenance VARCHAR(255) NOT NULL,
    string_search BOOL NOT NULL,
    distance INT NOT NULL,    
    string_similarity FLOAT NOT NULL,
    preferred BOOL NOT NULL,
    PRIMARY KEY (omop_id),
    INDEX idx_biolink_id (biolink_id ASC));