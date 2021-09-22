import logging
from typing import Iterable, Optional, Dict, List, Tuple
import json
import difflib
from collections import defaultdict
import urllib3

from .cohd_utilities import DomainClass
from .app import cache
from .query_cohd_mysql import sql_connection
from .translator.sri_node_normalizer import SriNodeNormalizer, NormalizedNode
from .translator.sri_name_resolution import SriNameResolution
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
        JOIN concept c ON m.omop_id = c.concept_id"""
        conn = sql_connection()
        cur = conn.cursor()
        cur.execute(sql)
        mapping_rows = cur.fetchall()
        BiolinkConceptMapper._map_omop = {r['omop_id']:r for r in mapping_rows}
        BiolinkConceptMapper._map_biolink = {r['biolink_id']:r for r in mapping_rows if r['preferred']}

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
    def map_from_omop(concept_id: int, preferred: bool = False) -> Tuple[Optional[OmopBiolinkMapping], Optional[List]]:
        """ Map from OMOP concept to Biolink

        Parameters
        ----------
        concept_id: OMOP concept ID
        preferred: True - return only preferred mappings; False - return mapping regardless if preferred or not

        Returns
        -------
        tuple: (Mapping object or None, list of categories or None)
        """
        r = BiolinkConceptMapper._map_omop.get(concept_id)
        if r:
            if not preferred or r['preferred']:
                omop_curie = f'OMOP:{concept_id}'
                mapping = OmopBiolinkMapping(omop_curie, r['biolink_id'], r['concept_name'], r['biolink_label'],
                                            r['provenance'], r['distance'])
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

        # Get current number of mappings that weren't from string searches
        sql = 'SELECT COUNT(*) AS count FROM biolink.mappings WHERE string_search = 0;'
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
        mapped_ids = list(omop_biolink.values())
        normalized_ids = SriNodeNormalizer.get_normalized_nodes(mapped_ids)

        # Create mappings
        for omop_id in omop_concepts:
            mapped_id = omop_biolink[omop_id]
            omop_label = omop_concepts[omop_id]['concept_name']
            if normalized_ids[mapped_id] is None:
                # Use the OMOP vocabulary mapping only
                categories = json.dumps(['biolink:DiseaseOrPhenotypicFeature'])
                provenance = f'(OMOP:{omop_id})-[OMOP Map]-({mapped_id})'
                distance = 1
                string_similarity = 1
                params.extend([omop_id, mapped_id, omop_label, categories, provenance, False, distance, string_similarity])
            else:
                # Use the normalized node
                biolink_norm_node = normalized_ids[mapped_id]
                biolink_norm_id = biolink_norm_node.normalized_identifier.id
                biolink_label = biolink_norm_node.normalized_identifier.label
                categories = json.dumps(biolink_norm_node.categories)
                provenance = f'(OMOP:{omop_id})-[OMOP Map]-({mapped_id})'
                distance = 1
                string_similarity = difflib.SequenceMatcher(None, omop_label.lower(), biolink_label.lower()).ratio()
                if mapped_id != biolink_norm_id:
                    provenance += f'-[SRI Node Norm]-({biolink_norm_id})'
                    distance += 1
                params.extend([omop_id, biolink_norm_id, biolink_label, categories, provenance, False, distance, string_similarity])
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

        # Create mappings
        for omop_id in omop_concepts:
            mapped_id = omop_biolink[omop_id]
            biolink_label = omop_concepts[omop_id]['concept_name']
            categories = json.dumps(['biolink:Drug'])
            provenance = f'(OMOP:{omop_id})-[OMOP Map]-({mapped_id})'
            distance = 1
            string_similarity = 1
            params.extend([omop_id, mapped_id, biolink_label, categories, provenance, False, distance, string_similarity])
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
        mapped_ids = list()
        for c in ingredient_concepts:
            mapped_id = 'MESH:' + c['mesh_code']
            omop_id = c['concept_id']
            omop_concepts[omop_id][mapped_id] = c
            omop_biolink[omop_id].append(mapped_id)
            mapped_ids.append(mapped_id)
        normalized_ids = SriNodeNormalizer.get_normalized_nodes(mapped_ids)

        for omop_id in omop_concepts:
            mapped_ids = omop_biolink[omop_id]
            for mapped_id in mapped_ids:
                if normalized_ids[mapped_id] is None:
                    continue

                biolink_norm_node = normalized_ids[mapped_id]
                biolink_norm_id = biolink_norm_node.normalized_identifier.id
                biolink_label = biolink_norm_node.normalized_identifier.label
                omop_label = omop_concepts[omop_id][mapped_id]['concept_name']
                categories = json.dumps(biolink_norm_node.categories)
                provenance = f'(OMOP:{omop_id})-[OMOP Map]-({mapped_id})'
                distance = 1
                string_similarity = difflib.SequenceMatcher(None, omop_label.lower(), biolink_label.lower()).ratio()
                if mapped_id != biolink_norm_id:
                    provenance += f'-[SRI Node Norm]-({biolink_norm_id})'
                    distance += 1
                params.extend([omop_id, biolink_norm_id, biolink_label, categories, provenance, False, distance, string_similarity])
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

        # Create the mappings
        for omop_id in omop_concepts:
            mapped_id = omop_biolink[omop_id]
            biolink_label = omop_concepts[omop_id]['concept_name']
            categories = json.dumps(['biolink:Procedure'])
            provenance = f'(OMOP:{omop_id})-[OMOP Map]-({mapped_id})'
            distance = 1
            string_similarity = 1
            params.extend([omop_id, mapped_id, biolink_label, categories, provenance, False, distance, string_similarity])
            mapping_count += 1

        ########################## Update mappings ##########################
        # Make sure that the new mappings have at least 95% as many mappings as the existing mappings
        if mapping_count < (0.95 * current_count):
            status_message = f"""Current number of mappings: {current_count}
            New mappings: {mapping_count}
            Retained old mappings"""
            logging.info(status_message)
            return status_message, 200
        else:
            logging.info('Updating biolink.mappings database')

            # Clear out old mappings
            sql = 'TRUNCATE TABLE biolink.mappings;'
            cur.execute(sql)

            # Insert new mappings
            sql = """
            INSERT INTO biolink.mappings (omop_id, biolink_id, biolink_label, categories, provenance, string_search, distance, string_similarity) VALUES
            """
            placeholders = ['(%s, %s, %s, %s, %s, %s, %s, %s)'] * mapping_count
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

        ########################## Search drug ingredients by name ##########################
        logging.info('Mapping drug ingredients by name')
        # Get all active drug ingredients which aren't mapped yet
        sql = """
        SELECT c.concept_id, c.concept_name
        FROM
        (SELECT DISTINCT concept_id FROM concept_counts) x
        JOIN concept c ON x.concept_id = c.concept_id
        LEFT JOIN biolink.mappings m ON x.concept_id = m.omop_id
        WHERE c.domain_id = 'drug' AND c.concept_class_id = 'ingredient'
            AND m.omop_id IS NULL
        ORDER BY c.concept_id;
        """
        cur.execute(sql)
        missing_ingredient_concepts = cur.fetchall()

        # First, collect responses from SRI Lookup service
        total_errors = 0
        max_total_errors = 10
        max_tries = 2
        omop_labels = dict()
        lookup_responses = dict()
        potential_curies = list()
        for r in missing_ingredient_concepts:
            if total_errors >= max_total_errors:
                logging.error(f'Biolink Mapper Max Total Errors')
                break

            tries = 1
            # SRI Lookup service can be a little flakey, retry a couple times
            while tries <= max_tries:
                try:
                    omop_id = r['concept_id']
                    concept_name = r['concept_name']
                    omop_labels[omop_id] = concept_name

                    # Lookup
                    j = SriNameResolution.name_lookup(concept_name)
                    if j is None:
                        logging.error(f'Biolink Mapper SRI Lookup Error: {omop_id} - {concept_name}')
                        total_errors += 1
                    else:
                        if len(j) > 0:
                            # Collect the responses
                            lookup_responses[omop_id] = j
                            potential_curies.extend(j.keys())
                            break
                        else:
                            logging.info(f'Biolink Mapper - No Match: {omop_id} - {concept_name}')
                            break
                except urllib3.exception.ConnectionError as e:
                    total_errors += 1

                tries += 1

        # Call SRI Node Normalizer to get categories for all potential CURIEs
        potential_curies = list(set(potential_curies))
        normalized_nodes = SriNodeNormalizer.get_normalized_nodes(potential_curies)

        # For each search result, find the first result that is a biolink:ChemicalEntity and high string similarity
        string_sim_criteria = 0.9
        string_match_count = 0
        params = list()
        chemical_descendants = bm_toolkit.get_descendants('biolink:ChemicalEntity', reflexive=True, formatted=True)
        for omop_id, lookup_response in lookup_responses.items():
            omop_label = omop_labels[omop_id].lower()
            # CURIEs are in order of best match, according to SRI, so use this order to find the first match
            for curie, labels in lookup_response.items():
                # Check if the categories of the CURIE include biolink:ChemicalEntity
                normalized_node = normalized_nodes.get(curie)
                if normalized_node is None:
                    continue
                is_chemical_descendant = False
                categories = normalized_node.categories
                for category in categories:
                    if category in chemical_descendants:
                        is_chemical_descendant = True
                        break
                if not is_chemical_descendant:
                    continue

                # Check if any of the labels match well enough
                found_match = False
                for label in labels:
                    string_similarity = difflib.SequenceMatcher(None, omop_label, label.lower()).ratio()
                    if string_similarity > string_sim_criteria:
                        found_match = True
                        categories = json.dumps(categories)
                        provenance = f'(OMOP:{omop_id})-[SRI Name Resolution]-({curie})'
                        params.extend([omop_id, curie, label, categories, provenance, True, 99, string_similarity])
                        string_match_count += 1
                        break

                if found_match:
                    break

        # Insert new string-based mappings
        sql = """
        INSERT INTO biolink.mappings (omop_id, biolink_id, biolink_label, categories, provenance, string_search, distance, string_similarity) VALUES
        """
        placeholders = ['(%s, %s, %s, %s, %s, %s, %s, %s)'] * string_match_count
        sql += ','.join(placeholders) + ';'
        cur.execute(sql, params)

        # Choose the preferred mappings among the string-search results only based on string similarity
        sql = """
        WITH
        -- First, get the biolink CURIEs that don't yet have a preferred mapping
        preferred AS (SELECT DISTINCT biolink_id
            FROM biolink.mappings
            WHERE preferred = 1),

        not_preferred AS (SELECT DISTINCT m.biolink_id AS biolink_id
            FROM biolink.mappings m
            LEFT JOIN preferred p ON m.biolink_id = p.biolink_id
            WHERE p.biolink_id IS NULL),

        -- Next, use string similarity
        string_sim AS (SELECT m.biolink_id, MAX(string_similarity) AS string_similarity
            FROM biolink.mappings m
            JOIN not_preferred np ON m.biolink_id = np.biolink_id
            GROUP BY biolink_id),

        -- Next, use max concept count from COHD data
        max_count AS (SELECT m.biolink_id, m.string_similarity, MAX(cc.concept_count) AS concept_count
            FROM biolink.mappings m
            JOIN string_sim s ON m.biolink_id = s.biolink_id
                AND m.string_similarity = s.string_similarity
            JOIN cohd.concept_counts cc ON m.omop_id = cc.concept_id
            GROUP BY m.biolink_id, m.string_similarity)

        UPDATE biolink.mappings m
        JOIN cohd.concept_counts cc ON m.omop_id = cc.concept_id
        JOIN max_count x ON m.biolink_id = x.biolink_id
            AND m.string_similarity = x.string_similarity
            AND cc.concept_count = x.concept_count
        SET preferred = true;
        """
        cur.execute(sql)
        conn.commit()

        status_message = f"""Current number of mappings: {current_count}
                New mappings: {mapping_count}
                String search mappings: {string_match_count}
                Updated to new mappings.
                """
        return status_message, 200


BiolinkConceptMapper.prefetch_mappings()
