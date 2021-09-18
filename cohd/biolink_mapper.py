import logging
from typing import Iterable, Optional, Dict, List, Tuple
import json
import difflib
from collections import defaultdict

from .cohd_utilities import DomainClass
from .app import cache
from .query_cohd_mysql import sql_connection
from .translator.sri_node_normalizer import SriNodeNormalizer, NormalizedNode
from .translator import bm_toolkit


def map_blm_class_to_omop_domain(node_type: str) -> Optional[List[DomainClass]]:
    """ Maps the Biolink Model class to OMOP domain_id, e.g., 'biolink:Disease' to 'Condition'

    Note, some classes may map to multiple domains, e.g., 'biolink:PopulationOfIndividualOrganisms' maps to
    ['Ethnicity', 'Gender', 'Race']

    Parameters
    ----------
    concept_type - Biolink Model class, e.g., 'biolink:Disease'

    Returns
    -------
    If normalized successfully: List of OMOP domains, e.g., ['Drug']; Otherwise: None
    """

    mappings = {
        'biolink:ChemicalEntity': [DomainClass('Drug', 'Ingredient')],
        'biolink:Device': [DomainClass('Device', None)],
        'biolink:DiseaseOrPhenotypicFeature': [DomainClass('Condition', None)],
        'biolink:Disease': [DomainClass('Condition', None)],
        'biolink:PhenotypicFeature': [DomainClass('Condition', None)],
        'biolink:Drug': [DomainClass('Drug', None)],
        'biolink:MolecularEntity': [DomainClass('Drug', 'Ingredient')],
        'biolink:Phenomenon': [DomainClass('Measurement', None),
                               DomainClass('Observation', None)],
        'biolink:PopulationOfIndividualOrganisms': [DomainClass('Ethnicity', None),
                                                    DomainClass('Gender', None),
                                                    DomainClass('Race', None)],
        'biolink:Procedure': [DomainClass('Procedure', None)],
        'biolink:SmallMolecule': [DomainClass('Drug', 'Ingredient')],
    }
    return mappings.get(node_type)


def map_omop_domain_to_blm_class(domain: str,
                                 concept_class: Optional[str] = None,
                                 desired_blm_categories: Iterable[str] = None
                                 ) -> str:
    """ Maps the OMOP domain_id to Biolink Model class, e.g., 'Condition' to 'biolink:Disease'

    Parameters
    ----------
    domain - OMOP domain, e.g., 'Condition'
    concept_class - [optional] OMOP concept class ID, e.g., 'Ingredient'
    desired_blm_categories - [optional] desired Biolink categories to choose from (e.g., if a query node specifies a
                     list of acceptable categories). This list does not have an ordered preference. Use the ordered
                     preference defined within this function's mappings_domain

    Returns
    -------
    If normalized successfully: Biolink Model semantic type. If no mapping found, use NamedThing
    """
    default_type = 'biolink:NamedThing'

    if DomainClass(domain, concept_class) in map_omop_domain_to_blm_class.mappings_domain_class:
        biolink_cat = map_omop_domain_to_blm_class.mappings_domain_class[DomainClass(domain, concept_class)]
    elif DomainClass(domain, None) in map_omop_domain_to_blm_class.mappings_domain_class:
        biolink_cat = map_omop_domain_to_blm_class.mappings_domain_class[DomainClass(domain, None)]
    else:
        biolink_cat = default_type

    if desired_blm_categories is None:
        # No preferred list of biolink categories provided. Return first item in preferred order
        biolink_cat = biolink_cat[0]
    else:
        # Find the first blm category that's in blm_categories. If none of the mapped biolink categories are in the
        # desired list, then use the first mapped biolink category
        biolink_cat = next((cat for cat in biolink_cat if cat in desired_blm_categories), biolink_cat[0])

    return biolink_cat


# Preferred mappings from OMOP (domain_id, concept_class_id) to biolink categories
# List items are in preferred order
map_omop_domain_to_blm_class.mappings_domain_class = {
    DomainClass('Condition', None): ['biolink:DiseaseOrPhenotypicFeature',
                                     'biolink:Disease',
                                     'biolink:PhenotypicFeature'],
    DomainClass('Device', None): ['biolink:Device'],
    DomainClass('Drug', None): ['biolink:Drug',
                                'biolink:MolecularEntity',
                                'biolink:ChemicalEntity',
                                'biolink:SmallMolecule',],
    DomainClass('Drug', 'Ingredient'): ['biolink:MolecularEntity',
                                        'biolink:ChemicalEntity',
                                        'biolink:SmallMolecule'],
    DomainClass('Ethnicity', None): ['biolink:PopulationOfIndividualOrganisms'],
    DomainClass('Gender', None): ['biolink:PopulationOfIndividualOrganisms'],
    DomainClass('Measurement', None): ['biolink:Phenomenon'],
    DomainClass('Observation', None): ['biolink:Phenomenon'],
    DomainClass('Procedure', None): ['biolink:Procedure'],
    DomainClass('Race', None): ['biolink:PopulationOfIndividualOrganisms']
}


class OmopBiolinkMapping:
    """
    Class for storing information about mappings between OMOP and Biolink, including provenance
    """
    def __init__(self, omop_id: Optional[str] = None, biolink_id: Optional[str] = None,
                 omop_label: Optional[str] = None, biolink_label: Optional[str] = None,
                 provenance: Optional[str] = None, distance: Optional[int] = None):
        self.omop_id = omop_id
        self.biolink_id = biolink_id
        self.omop_label = omop_label
        self.biolink_label = biolink_label
        self.provenance = provenance
        self.distance = distance


class BiolinkConceptMapper:
    """ Maps between OMOP concepts and Biolink Model

    When mapping from OMOP conditions to Biolink Model diseases, since SNOMED-CT, ICD10CM, ICD9CM, and MedDRA are now
    included in Biolink Model, map to these source vocabularies using the OMOP concept definitions. 

    When mapping from OMOP drugs (non-ingredients), use RXCUI for RXNORM identifiers. When mapping from OMOP drug
    ingredients, map through MESH. 
    """

    # Store mappings keyed by omop/biolink IDs in memory
    _map_omop = dict()
    _map_biolink = dict()

    # Pre-fetch mappings from SQL database
    @staticmethod
    def prefetch_mappings():
        sql = """SELECT m.*, c.concept_name
        FROM biolink.mappings m
        JOIN concept c ON m.omop_id = c.concept_id
        WHERE preferred = true;"""
        conn = sql_connection()
        cur = conn.cursor()
        cur.execute(sql)
        mapping_rows = cur.fetchall()
        BiolinkConceptMapper._map_omop = {r['omop_id']:r for r in mapping_rows}
        BiolinkConceptMapper._map_biolink = {r['biolink_id']:r for r in mapping_rows}
    
    @staticmethod
    def map_to_omop(curies: List[str]) -> Optional[Tuple[Dict[str, Optional[OmopBiolinkMapping]], Dict[str, Optional[NormalizedNode]]]]:
        """ Map to OMOP concept from Biolink

        Parameters
        ----------
        curies: list of CURIEs

        Returns
        -------
        Tuple (Dict of Mapping objects, normalized curies from SRI)
        """        
        # Default response
        omop_mappings = {curie:None for curie in curies}
        
        # Get equivalent identifiers from SRI Node Normalizer
        normalized_nodes = SriNodeNormalizer.get_normalized_nodes(curies)
        if normalized_nodes is None:
            logging.error('Failure with querying SRI Node Normalizer')
            return omop_mappings, normalized_nodes
                
        # Query internal mappings
        canonical_ids = [nn.normalized_identifier.id for nn in normalized_nodes.values() if nn is not None]
        canonical_str = ','.join([f"'{id}'" for id in canonical_ids])
        if not canonical_ids:
            # No normalized nodes found
            return omop_mappings, normalized_nodes
        
        for curie in curies:            
            normalized_node = normalized_nodes.get(curie)
            if normalized_node is None:
                omop_mappings[curie] = None
                continue
            normalized_id = normalized_node.normalized_identifier.id

            # Get label from SRI Node Normalizer
            biolink_label = ''            
            for equiv_id in normalized_node.equivalent_identifiers:
                if equiv_id.id == curie:
                    biolink_label = equiv_id.label
                    break
            
            # Create mapping object
            r = BiolinkConceptMapper._map_biolink.get(normalized_id)
            if r is None:
                omop_mappings[curie] = None
                continue
            omop_curie = f"OMOP:{r['omop_id']}"
            distance = r['distance'] + (curie != normalized_id)
            mapping = OmopBiolinkMapping(omop_curie, curie, r['concept_name'], biolink_label, r['provenance'], distance)            
            omop_mappings[curie] = mapping

        return omop_mappings, normalized_nodes

    @staticmethod
    def map_from_omop(concept_id: int) -> Tuple[Optional[OmopBiolinkMapping], Optional[List]]:
        """ Map from OMOP concept to Biolink

        Parameters
        ----------
        concept_id: OMOP concept ID

        Returns
        -------
        tuple: (Mapping object or None, list of categories or None)
        """
        r = BiolinkConceptMapper._map_omop.get(concept_id)
        if r:
            omop_curie = f'OMOP:{concept_id}'            
            mapping = OmopBiolinkMapping(omop_curie, r['biolink_id'], r['concept_name'], r['biolink_label'], r['provenance'], r['distance'])
            categories = json.loads(r['categories'])
            return mapping, categories

        return None, None

    @staticmethod
    def build_mappings() -> Tuple[str, int]:
        """ Rebuilds the mappings between OMOP and Biolink

        Returns
        -------
        Number of concepts
        """
        logging.info('Starting a new build of OMOP-Biolink mappings')

        # Map OMOP vocabulary_id to Biolink prefixes
        prefix_map = {
            'ICD10CM': 'ICD10',
            'ICD9CM': 'ICD9',
            'MedDRA': 'MEDDRA',
            'SNOMED': 'SNOMEDCT',
            'HCPCS': 'HCPCS',
            'CPT4': 'CPT'
        }

        # COHD MySQL database
        conn = sql_connection()
        cur = conn.cursor()

        # Get current number of mappings
        sql = 'SELECT COUNT(*) AS count FROM biolink.mappings;'
        cur.execute(sql)
        current_count = cur.fetchone()['count']

        # Build the SQL insert
        mapping_count = 0
        params = list()

        ########################## Conditions ##########################
        logging.info('Mapping condition concepts')

        # Get all active condition concepts
        sql = """
        SELECT c.concept_id, c.concept_name, c.vocabulary_id, c.concept_code,
            c.concept_class_id, standard_concept
        FROM
            (SELECT DISTINCT concept_id FROM concept_counts) x
        JOIN concept c ON x.concept_id = c.concept_id
        WHERE c.domain_id = 'condition'
        ORDER BY c.concept_id;
        """
        cur.execute(sql)
        condition_concepts = cur.fetchall()
        omop_concepts = {c['concept_id']:c for c in condition_concepts}

        # Normalize with SRI Node Norm via ICD, MedDRA, and SNOMED codes
        omop_biolink = {c['concept_id']:prefix_map[c['vocabulary_id']] + ':' + c['concept_code'] for c in condition_concepts if c['vocabulary_id'] in prefix_map}
        biolink_ids = list(omop_biolink.values())
        normalized_ids = SriNodeNormalizer.get_normalized_nodes(biolink_ids)

        # Create mappings
        for omop_id in omop_concepts:
            biolink_id = omop_biolink[omop_id]
            if normalized_ids[biolink_id] is None:
                continue
            
            biolink_norm_node = normalized_ids[biolink_id]
            biolink_norm_id = biolink_norm_node.normalized_identifier.id
            biolink_label = biolink_norm_node.normalized_identifier.label
            omop_label = omop_concepts[omop_id]['concept_name']
            categories = json.dumps(biolink_norm_node.categories)
            provenance = f'OMOP:{omop_id}-{biolink_id}'
            distance = 0
            string_similarity = difflib.SequenceMatcher(None, omop_label, biolink_label).ratio()
            if biolink_id != biolink_norm_id:
                provenance += f'-{biolink_norm_id}'
                distance += 1
            params.extend([omop_id, biolink_norm_id, biolink_label, categories, provenance, distance, string_similarity])
            mapping_count += 1

        ########################## Drugs - non-ingredients ##########################
        logging.info('Mapping drug concepts')
        
        # Get all active RXNORM drug (non-ingredient) concepts
        sql = """
        SELECT c.concept_id, c.concept_name, c.vocabulary_id, c.concept_code, c.concept_class_id, standard_concept
        FROM
        (SELECT DISTINCT concept_id FROM concept_counts) x
        JOIN concept c ON x.concept_id = c.concept_id
        WHERE c.domain_id = 'drug' AND c.concept_class_id != 'ingredient' AND c.vocabulary_id = 'RxNorm'
        ORDER BY c.concept_id;
        """
        cur.execute(sql)
        drug_concepts = cur.fetchall()

        # Use RXNORM (RXCUI) for Biolink ID
        omop_concepts = {c['concept_id']:c for c in drug_concepts}
        omop_biolink = {c['concept_id']:'RXCUI:' + c['concept_code'] for c in drug_concepts}
        biolink_ids = list(omop_biolink.values())

        # Create mappings
        for omop_id in omop_concepts:
            biolink_id = omop_biolink[omop_id]
            biolink_label = omop_concepts[omop_id]['concept_name']
            categories = json.dumps(['biolink:Drug'])
            provenance = f'OMOP:{omop_id}-{biolink_id}'
            distance = 0
            string_similarity = 1 
            params.extend([omop_id, biolink_id, biolink_label, categories, provenance, distance, string_similarity])
            mapping_count += 1

        ########################## Drug ingredients ##########################
        logging.info('Mapping drug ingredient concepts')
        
        # Get all active drug ingredients which have OMOP mappings to MESH
        sql = """
        SELECT c.concept_id, c.concept_name, '|||', c_mesh.concept_code AS mesh_code, c_mesh.concept_name AS mesh_name
        FROM
        (SELECT DISTINCT concept_id FROM concept_counts) x
        JOIN concept c ON x.concept_id = c.concept_id
        JOIN concept_relationship cr ON c.concept_id = cr.concept_id_2
        JOIN concept c_mesh ON cr.concept_id_1 = c_mesh.concept_id
        --    AND cr.relationship_id = 'Maps to' -- only Maps to relationships in the COHD database
        WHERE c.domain_id = 'drug' AND c.concept_class_id = 'ingredient'
            AND c_mesh.vocabulary_id = 'MeSH'
        ORDER BY c.concept_id;
        """
        cur.execute(sql)
        ingredient_concepts = cur.fetchall()

        # Normalize with SRI Node Norm via MESH
        # Note: multiple MESH concepts may map to the same standard OMOP concept
        omop_concepts = defaultdict(dict)
        omop_biolink = defaultdict(list)
        biolink_ids = list()
        for c in ingredient_concepts:
            biolink_id = 'MESH:' + c['mesh_code']
            omop_id = c['concept_id']
            omop_concepts[omop_id][biolink_id] = c
            omop_biolink[omop_id].append(biolink_id)
            biolink_ids.append(biolink_id)
        normalized_ids = SriNodeNormalizer.get_normalized_nodes(biolink_ids)
        
        for omop_id in omop_concepts:
            biolink_ids = omop_biolink[omop_id]
            for biolink_id in biolink_ids:
                if normalized_ids[biolink_id] is None:
                    continue

                biolink_norm_node = normalized_ids[biolink_id]
                biolink_norm_id = biolink_norm_node.normalized_identifier.id
                biolink_label = biolink_norm_node.normalized_identifier.label
                omop_label = omop_concepts[omop_id][biolink_id]['concept_name']
                categories = json.dumps(biolink_norm_node.categories)
                provenance = f'OMOP:{omop_id}-{biolink_id}'
                distance = 0
                string_similarity = difflib.SequenceMatcher(None, omop_label, biolink_label).ratio()
                if biolink_id != biolink_norm_id:
                    provenance += f'-{biolink_norm_id}'
                    distance += 1
                params.extend([omop_id, biolink_norm_id, biolink_label, categories, provenance, distance, string_similarity])
                mapping_count += 1

                # Naively use the first MESH ID that can be normalized
                break

        ########################## Procedures ##########################
        logging.info('Mapping procedure concepts')
        
        # Note: Biolink doesn't list any prefixes in biolink:Procedure. Use vocabularies that are supported
        # by Biolink in general (SNOMED, CPT4, MedDRA, HCPCS, and ICD9CM). Currently unsupported vocabularies 
        # include ICD10PCS and ICD9Proc
        sql = """
        SELECT c.concept_id, c.concept_name, c.vocabulary_id, c.concept_code, c.concept_class_id, standard_concept
        FROM
        (SELECT DISTINCT concept_id FROM concept_counts) x
        JOIN concept c ON x.concept_id = c.concept_id
        WHERE c.domain_id = 'procedure' AND c.vocabulary_id IN ('CPT4', 'HCPCS', 'ICD9CM', 'MedDRA', 'SNOMED')
        ORDER BY c.concept_id;
        """
        cur.execute(sql)
        procedure_concepts = cur.fetchall()
        
        # Use the vocabulary concept codes as the Biolink IDs
        omop_concepts = {c['concept_id']:c for c in procedure_concepts}
        omop_biolink = {c['concept_id']:prefix_map[c['vocabulary_id']] + ':' + c['concept_code'] for c in procedure_concepts if c['vocabulary_id'] in prefix_map}
        biolink_ids = list(omop_biolink.values())

        # Create the mappings
        for omop_id in omop_concepts:
            biolink_id = omop_biolink[omop_id]
            biolink_label = omop_concepts[omop_id]['concept_name']
            categories = json.dumps(['biolink:Procedure'])
            provenance = f'OMOP:{omop_id}-{biolink_id}'
            distance = 0
            string_similarity = 1
            params.extend([omop_id, biolink_id, biolink_label, categories, provenance, distance, string_similarity])
            mapping_count += 1

        ########################## Finalize ##########################
        # Make sure that the new mappings have at least 95% as many mappings as the existing mappings
        status_message = f"""Current number of mappings: {current_count}
            New mappings: {mapping_count}
            """
        if mapping_count < (0.95 * current_count):
            status_message += 'Retained old mappings'
            logging.info(status_message)
            return status_message, 200
        else:
            logging.info('Updating biolink.mappings database')

            # Clear out old mappings
            sql = 'TRUNCATE TABLE biolink.mappings;'
            cur.execute(sql)

            # Insert new mappings
            sql = """
            INSERT INTO biolink.mappings (omop_id, biolink_id, biolink_label, categories, provenance, distance, string_similarity) VALUES
            """
            placeholders = ['(%s, %s, %s, %s, %s, %s, %s)'] * mapping_count
            sql += ','.join(placeholders) + ';'
            cur.execute(sql, params)
            conn.commit()

            # Multiple OMOP IDs may map to the same Biolink IDs. Try to find preferred mappings based on:
            # 1) mapping distance, 2) string similarity, 3) COHD counts
            sql = """
            WITH
            -- First choose based off of mapping distance
            dist AS (SELECT biolink_id, MIN(distance) AS distance
                FROM biolink.mappings
                GROUP BY biolink_id),
            
            -- Next, use string similarity
            string_sim AS (SELECT m.biolink_id, m.distance, MAX(string_similarity) AS string_similarity
                FROM biolink.mappings m
                JOIN dist ON m.biolink_id = dist.biolink_id AND m.distance = dist.distance
                GROUP BY biolink_id, distance),
            
            -- Next, use max concept count from COHD data
            max_count AS (SELECT m.biolink_id, m.distance, m.string_similarity, MAX(cc.concept_count) AS concept_count
                FROM biolink.mappings m
                JOIN string_sim s ON m.biolink_id = s.biolink_id
                    AND m.distance = s.distance
                    AND m.string_similarity = s.string_similarity
                JOIN cohd.concept_counts cc ON m.omop_id = cc.concept_id
                GROUP BY m.biolink_id, m.distance, m.string_similarity)

            UPDATE biolink.mappings m
            JOIN cohd.concept_counts cc ON m.omop_id = cc.concept_id
            JOIN max_count x ON m.biolink_id = x.biolink_id
                AND m.distance = x.distance
                AND m.string_similarity = x.string_similarity
                AND cc.concept_count = x.concept_count
            -- JOIN cohd.concept c ON m.omop_id = c.concept_id
            --    AND x.standard_concept = c.standard_concept
            SET preferred = true;
            """
            cur.execute(sql)
            conn.commit()

            status_message += 'Updated to new mappings'
            logging.info(status_message)
            return status_message, 200

BiolinkConceptMapper.prefetch_mappings()
