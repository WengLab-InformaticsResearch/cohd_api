import threading
import logging
from typing import Iterable, Optional, Dict, List, Tuple
import json

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

        sql = f"""SELECT m.*, c.concept_name
        FROM biolink.mappings m
        JOIN concept c ON m.omop_id = c.concept_id
        WHERE biolink_id IN ({canonical_str}) AND preferred = true;"""
        conn = sql_connection()
        cur = conn.cursor()
        cur.execute(sql)
        mapping_rows = cur.fetchall()

        if not mapping_rows:
            # No mappings found 
            return omop_mappings, normalized_nodes

        # Create dictionary keyed on biolink_id
        mappings = {r['biolink_id']:r for r in mapping_rows}
        
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
            r = mappings.get(normalized_id)                  
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
        conn = sql_connection()
        cur = conn.cursor()
        sql = f"""SELECT m.*, c.concept_name
        FROM biolink.mappings m
        JOIN concept c ON m.omop_id = c.concept_id
        WHERE omop_id = {concept_id}"""
        cur.execute(sql)
        mapping_rows = cur.fetchall()

        if len(mapping_rows) == 1:
            r = mapping_rows[0]
            omop_curie = f'OMOP:{concept_id}'            
            mapping = OmopBiolinkMapping(omop_curie, r['biolink_id'], r['concept_name'], r['biolink_label'], r['provenance'], r['distance'])
            categories = json.loads(r['categories'])
            return mapping, categories

        return None, None

    @staticmethod
    def build_mapping() -> Tuple[str, int]:
        """ Calls the BiolinkConceptMapper's map_from_omop on all concepts with data in COHD to build the cache

        This function starts another thread to run the build.

        Returns
        -------
        Number of concepts
        """
        thread = threading.Thread(target=BiolinkConceptMapper._build_mapping, daemon=True)
        thread.start()
        return 'Build started.', 200

    # Flag to indicate that COHD is currently in the process of rebuilding the cache
    rebuilding_cache = False

    @staticmethod
    def _build_mapping() -> int:
        """ Calls the BiolinkConceptMapper's map_from_omop on all concepts with data in COHD to build the cache

        Returns
        -------
        Number of concepts
        """
        # Temporary shell
        return 0
