from abc import ABC, abstractmethod
import threading
import logging
import requests
from requests.compat import urljoin
from typing import Union, Any, Iterable, Optional, Dict, List, Tuple
from collections import defaultdict
from datetime import datetime
from enum import Enum
import pymysql
from bmt import Toolkit
from numpy import argsort

from .cohd_utilities import ln_ratio_ci, ci_significance, DomainClass
from .omop_xref import ConceptMapper, Mapping
from .app import cache
from .query_cohd_mysql import query_active_concepts


# Static instance of the Biolink Model Toolkit
bm_toolkit = Toolkit('https://raw.githubusercontent.com/biolink/biolink-model/1.8.2/biolink-model.yaml')


class TrapiStatusCode(Enum):
    """
    Enumerated TRAPI status codes.

    Note: There is currently no standardized list of allowed status codes. Below are a few examples
    from the TRAPI spec and from this doc:
    https://docs.google.com/document/d/12GRjcAqXQfp557kAcVEm7V0mE3hKGxAY5B6ZPavp6tQ/edit#
    plus a few defined for COHD
    """
    SUCCESS = 'Success'
    NO_RESULTS = 'NoResults'
    QUERY_NOT_TRAVERSABLE = 'QueryNotTraversable'
    KP_NOT_AVAILABLE = 'KPNotAvailable'
    UNRESOLVABLE_CURIE = 'UnresolvableCurie'
    COULD_NOT_MAP_CURIE_TO_LOCAL_KG = 'CouldNotMapCurieToLocalKG'
    UNSUPPORTED_QNODE_CATEGORY = 'UnsupportedQNodeCategory'


class CohdTrapi(ABC):
    """
    Abstract class for TRAPI endpoint implementation conforming to NCATS Biodmedical Data Translator Reasoner API Spec.
    This abstract class will be implemented for different versions of TRAPI
    """

    @abstractmethod
    def __init__(self, request):
        """ Constructor should take a flask request object """
        assert request is not None, 'cohd_trapi.py::CohdTrapi::__init__() - Bad request'

        self._method = None

    @abstractmethod
    def operate(self):
        """ Performs the operation requested by the TRAPI request.

        Returns
        -------
        Response message with JSON data in Translator Reasoner API Standard
        """
        pass

    # Default options
    default_method = 'obsExpRatio'
    default_min_cooccurrence = 50
    default_confidence_interval = 0.99
    default_dataset_id = 3
    default_local_oxo = False
    default_mapping_distance = 3
    default_biolink_only = True
    default_max_results = 500
    default_log_level = logging.WARNING
    limit_max_results = 500
    supported_query_methods = ['relativeFrequency', 'obsExpRatio', 'chiSquare']
    # Set of edge types that are supported by the COHD Reasoner. This list is in preferred order, most preferred first
    supported_edge_types = [
        'biolink:correlated_with',  # Currently, COHD models all relations using biolink:correlated_with
    ]

    # Mapping for which predicate should be used for each COHD analysis method. For now, it's all correlated_with
    default_predicate = 'biolink:correlated_with'
    method_predicates = {
        'obsExpRatio': default_predicate,
        'relativeFrequency': default_predicate,
        'chiSquare': default_predicate
    }

    def _get_kg_predicate(self) -> str:
        """ Determines which predicate should be used to represent the COHD analysis

        Returns
        -------
        Biolink predicate
        """
        return CohdTrapi.method_predicates.get(self._method, CohdTrapi.default_predicate)


class ResultCriteria:
    """
    Stores a defined criterion to be applied to a COHD result
    """

    def __init__(self, function, kargs):
        """ Constructor

        Parameters
        ----------
        function: a function
        kargs: keyword arguments
        """
        self.function = function
        self.kargs = kargs

    def check(self, cohd_result):
        """ Checks if the cohd_result passes the defined criterion

        Parameters
        ----------
        cohd_result: COHD result

        Returns
        -------
        True if passes
        """
        return self.function(cohd_result, **self.kargs)


def criteria_min_cooccurrence(cohd_result, cooccurrence):
    """ Checks that the raw co-occurrence count is >= the specified cooccurrence

    Parameters
    ----------
    cohd_result: COHD result
    cooccurrence: mininum co-occurrence

    Returns
    -------
    True if passes
    """
    if 'n_c1_c2' in cohd_result:
        # chi-square
        return cohd_result['n_c1_c2'] >= cooccurrence
    elif 'observed_count' in cohd_result:
        # obsExpRatio
        return cohd_result['observed_count'] >= cooccurrence
    elif 'concept_pair_count' in cohd_result:
        # relative frequency
        return cohd_result['concept_pair_count'] >= cooccurrence
    else:
        return False


def criteria_threshold(cohd_result, threshold):
    """ Checks that the metric passes the threshold.
    chi-square: p-value < threshold
    observed-expected frequency ratio: ln_ratio >= threshold
    relative frequency: relative_frequency >= threshold
    False for any other types of result

    Parameters
    ----------
    cohd_result
    threshold

    Returns
    -------
    True if passes
    """
    if 'p-value' in cohd_result:
        # chi-square
        return cohd_result['p-value'] < threshold
    elif 'ln_ratio' in cohd_result:
        # obsExpRatio
        if threshold >= 0:
            return cohd_result['ln_ratio'] >= threshold
        else:
            return cohd_result['ln_ratio'] <= threshold
    elif 'relative_frequency' in cohd_result:
        # relative frequency
        return cohd_result['relative_frequency'] >= threshold
    else:
        return False


def criteria_confidence(cohd_result, confidence):
    """ Checks the confidence interval of the result for significance using alpha. Only applies to observed-expected
    frequency ratio. Returns True for all other types of results.

    Parameters
    ----------
    cohd_result
    confidence

    Returns
    -------
    True if significant
    """
    if 'ln_ratio' in cohd_result:
        # obsExpFreq
        ci = ln_ratio_ci(cohd_result['observed_count'], cohd_result['ln_ratio'], confidence)
        return ci_significance(ci)
    else:
        # relativeFrequency doesn't have a good cutoff for confidence interval, and chiSquare uses
        # p-value for significance, so allow methods other than obsExpRatio to pass
        return True


mappings_domain_ontology = {
    '_DEFAULT': ['ICD9CM', 'RxNorm', 'UMLS', 'DOID', 'MONDO']
}


def fix_blm_category(blm_category):
    """ Checks and fixes blm_type.

    Translator Reasoner API changed conventions for blm node categories from snake case without 'biolink' prefix (e.g.,
    biolink:population_of_individual_organisms) to camel case requiring prefix (e.g.,
    biolink:PopulationOfIndividualOrganisms). This method attempts to correct the input if it matches the old spec.

    Parameters
    ----------
    blm_category - (String)

    Returns
    -------
    corrected blm_category
    """
    # Don't process None or empty string
    if blm_category is None or not blm_category:
        return blm_category

    # Remove any existing prefix and add biolink prefix
    suffix = blm_category.split(':')[-1]
    blm_category = 'biolink:' + suffix

    # Convert snake case to camel case. Keep the original input if not in this dictionary.
    supported_type_conversions = {
        'biolink:chemical_substance': 'biolink:ChemicalSubstance',
        'biolink:device': 'biolink:Device',
        'biolink:disease': 'biolink:Disease',
        'biolink:disease_or_phenotypic_feature': 'biolink:DiseaseOrPhenotypicFeature',
        'biolink:drug': 'biolink:Drug',
        'biolink:phenomenon': 'biolink:Phenomenon',
        'biolink:phenotypic_feature': 'biolink:PhenotypicFeature',
        'biolink:population_of_individual_organisms': 'biolink:PopulationOfIndividualOrganisms',
        'biolink:procedure': 'biolink:Procedure'
    }
    blm_category = supported_type_conversions.get(blm_category, blm_category)

    return blm_category


def suggest_blm_category(blm_category: str) -> Optional[str]:
    """ COHD prefers certain Biolink categories over others. This returns a preferred Biolink category if one exists.

    Parameters
    ----------
    blm_category

    Returns
    -------
    The preferred Biolink category, or None
    """
    suggestions = {
        # OMOP conditions are better represented as DiseaseOrPhenotypicFeature than as Disease. We used to replace
        # biolink:Disease with biolink:DiseaseOrPhenotypicFeature. However, now we won't make the replacement anymore
        # and will rely on the category suggested by SRI to distinguish between Disease and PhenotypicFeature.
        # 'biolink:Disease': 'biolink:DiseaseOrPhenotypicFeature'
    }
    return suggestions.get(blm_category)


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
        'biolink:ChemicalSubstance': [DomainClass('Drug', 'Ingredient')],
        'biolink:Device': [DomainClass('Device', None)],
        'biolink:DiseaseOrPhenotypicFeature': [DomainClass('Condition', None)],
        'biolink:Disease': [DomainClass('Condition', None)],
        'biolink:PhenotypicFeature': [DomainClass('Condition', None)],
        'biolink:Drug': [DomainClass('Drug', None)],
        'biolink:Phenomenon': [DomainClass('Measurement', None),
                               DomainClass('Observation', None)],
        'biolink:PopulationOfIndividualOrganisms': [DomainClass('Ethnicity', None),
                                                    DomainClass('Gender', None),
                                                    DomainClass('Race', None)],
        'biolink:Procedure': [DomainClass('Procedure', None)]
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
    DomainClass('Condition', None): ['biolink:DiseaseOrPhenotypicFeature'],
    DomainClass('Device', None): ['biolink:Device'],
    DomainClass('Drug', None): ['biolink:Drug',
                                'biolink:ChemicalSubstance'],
    DomainClass('Drug', 'Ingredient'): ['biolink:ChemicalSubstance',
                                        'biolink:Drug'],
    DomainClass('Ethnicity', None): ['biolink:PopulationOfIndividualOrganisms'],
    DomainClass('Gender', None): ['biolink:PopulationOfIndividualOrganisms'],
    DomainClass('Measurement', None): ['biolink:Phenomenon'],
    DomainClass('Observation', None): ['biolink:Phenomenon'],
    DomainClass('Procedure', None): ['biolink:Procedure'],
    DomainClass('Race', None): ['biolink:PopulationOfIndividualOrganisms']
}


class BiolinkConceptMapper:
    """ Maps between OMOP concepts and Biolink Model

    When mapping from OMOP conditions to Biolink Model diseases, since SNOMED-CT, ICD10CM, ICD9CM, and MedDRA are now
    included in Biolink Model, map to these source vocabularies using the OMOP concept definitions. When mapping from
    other ontologies to OMOP, leverage the OxO as well.
    """

    _mappings_prefixes_blm_to_oxo = {
        # DiseaseOrPhenotypicFeature prefixes
        'MONDO': 'MONDO',
        'DOID': 'DOID',
        'OMIM': 'OMIM',
        'ORPHANET': 'Orphanet',
        'EFO': 'EFO',
        'UMLS': 'UMLS',
        'MESH': 'MeSH',
        'MEDDRA': 'MedDRA',
        'NCIT': 'NCIT',
        'SNOMEDCT': 'SNOMEDCT',
        'medgen': None,
        'ICD10': 'ICD10CM',
        'ICD9': 'ICD9CM',
        'ICD0': None,
        'KEGG.DISEASE': None,
        'HP': 'HP',
        'MP': 'MP',
        'ZP': None,
        'UPHENO': None,
        'APO': 'APO',
        'FBcv': 'FBcv',
        'WBPhenotype': 'WBPhenotype',
        # Drug prefixes
        'RXCUI': 'RxNorm',
        'NDC': None,
        'PHARMGKB.DRUG': None,
        # Chemical Substances prefixes
        'PUBCHEM.COMPOUND': 'PubChem_Compound',
        'CHEMBL.COMPOUND': None,
        'UNII': None,
        'CHEBI': 'CHEBI',
        'DRUGBANK': 'DrugBank',
        'CAS': 'CAS',
        'DrugCentral': None,
        'GTOPDB': None,
        'HMDB': 'HMDB',
        'KEGG.COMPOUND': 'KEGG',
        'ChemBank': None,
        'Aeolus': None,
        'PUBCHEM.SUBSTANCE': None,
        'SIDER.DRUG': None,
        'INCHI': None,
        'INCHIKEY': 'InChIKey',
        'KEGG.GLYCAN': 'KEGG',
        'KEGG.DRUG': 'KEGG',
        'KEGG.DGROUP': 'KEGG',
        'KEGG.ENVIRON': 'KEGG',
        # Procedure prefixes
        'ICD10PCS': None
    }

    _mappings_prefixes_oxo_to_blm = {
        # Disease prefixes
        'MONDO': 'MONDO',
        'DOID': 'DOID',
        'OMIM': 'OMIM',
        'Orphanet': 'ORPHANET',
        'EFO': 'EFO',
        'UMLS': 'UMLS',
        'MeSH': 'MESH',
        'MedDRA': 'MEDDRA',
        'NCIT': 'NCIT',
        'SNOMEDCT': 'SNOMEDCT',
        'ICD10CM': 'ICD10',
        'ICD9CM': 'ICD9',
        'HP': 'HP',
        'MP': 'MP',
        'APO': 'APO',
        'FBcv': 'FBcv',
        'WBPhenotype': 'WBPhenotype',
        # Drug prefixes
        'RxNorm': 'RXCUI',
        # Chemical Substance prefixes
        'PubChem_Compound': 'PUBCHEM.COMPOUND',
        'CHEBI': 'CHEBI',
        'DrugBank': 'DRUGBANK',
        'CAS': 'CAS',
        'HMDB': 'HMDB',
        'INCHIKEY': 'InChIKey',
        # Procedure prefixes
    }

    _default_ontology_map = {
        'biolink:DiseaseOrPhenotypicFeature': ['MONDO', 'DOID', 'OMIM', 'ORPHANET', 'EFO', 'UMLS', 'MESH', 'MEDDRA',
                                               'NCIT', 'SNOMEDCT', 'medgen', 'ICD10', 'ICD9', 'ICD0', 'KEGG.DISEASE',
                                               'HP', 'MP', 'ZP', 'UPHENO', 'APO', 'FBcv', 'WBPhenotype'],
        'biolink:ChemicalSubstance': ['PUBCHEM.COMPOUND', 'CHEMBL.COMPOUND', 'UNII', 'CHEBI', 'DRUGBANK', 'MESH', 'CAS',
                                      'DrugCentral', 'GTOPDB', 'HMDB', 'KEGG.COMPOUND', 'ChemBank', 'Aeolus',
                                      'PUBCHEM.SUBSTANCE', 'SIDER.DRUG', 'INCHI', 'INCHIKEY', 'KEGG.GLYCAN',
                                      'KEGG.DRUG', 'KEGG.DGROUP', 'KEGG.ENVIRON'],
        'biolink:Drug': ['RXCUI', 'NDC', 'PHARMGKB.DRUG'],
        # Note: There are currently no prefixes allowed for Procedure in Biolink, so use some standard OMOP mappings
        'biolink:Procedure': ['ICD10PCS', 'SNOMEDCT']
    }

    # Update Flask cache. When True, flask cache will be updated for the given (parameter-sensitive) call
    force_update_cache = False

    @staticmethod
    def map_blm_prefixes_to_oxo_prefixes(s):
        """ Attempts to map the Biolink Model prefix to OxO prefix, e.g., 'ICD10' to 'ICD10CM'. If the mapping is not
        available, the function returns the original prefix.

        Parameters
        ----------
        s - Biolink Model prefix (e.g., 'ICD10') or CURIE (e.g., 'ICD10:45576876')

        Returns
        -------
        The OxO prefix/CURIE if it exists, else the original input is returned
        """
        split = s.split(':')
        if len(split) == 2:
            # Assume s is a curie. Replace the prefix
            prefix, suffix = split
            prefix = BiolinkConceptMapper._mappings_prefixes_blm_to_oxo.get(prefix, prefix)
            curie = '{prefix}:{suffix}'.format(prefix=prefix, suffix=suffix)
            return curie
        else:
            # Assume s is a prefix
            return BiolinkConceptMapper._mappings_prefixes_blm_to_oxo.get(s, s)

    @staticmethod
    def map_oxo_prefixes_to_blm_prefixes(s):
        """ Attempts to map the OxO prefix to Biolink Model prefix, e.g., 'ICD10CM' to 'ICD10'. If the mapping is not
        available, the function returns the original prefix.

        Parameters
        ----------
        s - OxO prefix (e.g., 'ICD10CM') or curie with OxO prefix (e.g., 'ICD10CM:45576876')

        Returns
        -------
        The prefix or CURIE with the prefix converted to the Biolink Model convention if the mapping exists, otherwise
        the original prefix or CURIE is returned.
        """
        split = s.split(':')
        if len(split) == 2:
            # Assume s is a curie. Replace the prefix only
            prefix, suffix = split
            prefix = BiolinkConceptMapper._mappings_prefixes_oxo_to_blm.get(prefix, prefix)
            curie = '{prefix}:{suffix}'.format(prefix=prefix, suffix=suffix)
            return curie
        else:
            # Assume s is a prefix
            return BiolinkConceptMapper._mappings_prefixes_oxo_to_blm.get(s, s)

    def __init__(self, biolink_mappings=None, distance=2, local_oxo=False):
        """ Constructor

        Parameters
        ----------
        biolink_mappings: mappings between domain and ontology. See documentation for ConceptMapper.
        distance: maximum allowed total distance (as opposed to OxO distance)
        local_oxo: use local implementation of OxO (default: True)
        """
        self.distance = distance
        self.local_oxo = local_oxo

        if biolink_mappings is None:
            biolink_mappings = BiolinkConceptMapper._default_ontology_map
        self.biolink_mappings = biolink_mappings

        # Convert the biolink_mappings to also be represented using OxO prefixes
        self.biolink_mappings_as_oxo = dict()
        for blm_category, prefixes in biolink_mappings.items():
            oxo_prefixes = [BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(prefix)
                            for prefix in prefixes]
            # remove None from list
            oxo_prefixes = [p for p in oxo_prefixes if p is not None]
            self.biolink_mappings_as_oxo[blm_category] = oxo_prefixes

        # Define target ontologies per OMOP domain based on mappings between Biolink categories and OMOP domains
        # Since Biolink categories have n:n mappings to OMOP domains, merge the target ontologies by OMOP domain
        oxo_mappings_set = defaultdict(set)
        for blm_category, prefixes in list(self.biolink_mappings_as_oxo.items()):
            omop_domain_classes = map_blm_class_to_omop_domain(blm_category)
            if omop_domain_classes is None or not omop_domain_classes:
                continue

            for omop_domain_class in omop_domain_classes:
                oxo_mappings_set[omop_domain_class.domain_id].union(prefixes)

        oxo_mappings = dict()
        for domain, prefix_set in oxo_mappings_set.items():
            oxo_mappings[domain] = list(prefix_set)

        self._oxo_concept_mapper = ConceptMapper(oxo_mappings, self.distance, self.local_oxo)

    def __repr__(self):
        """ Used in flask cache

        Returns
        -------
        String repr
        """
        d = {
            'biolink_mappings': self.biolink_mappings,
            'distance': self.distance,
            'local_oxo': self.local_oxo
        }
        return str(d)

    @cache.memoize(timeout=7257600, cache_none=True, unless=lambda: BiolinkConceptMapper.force_update_cache)
    def map_to_omop(self, curies: List[str]) -> Tuple[Dict[str, Mapping], Optional[Dict[str, Any]]]:
        """ Map to OMOP concept from ontology

        Parameters
        ----------
        curies: list of CURIEs

        Returns
        -------
        Tuple (Dict of Mapping objects, normalized curies from SRI)
        """
        # Get equivalent identifiers from SRI Node Normalizer
        normalized_nodes = SriNodeNormalizer.get_normalized_nodes(curies)

        omop_mappings = dict()
        for curie in curies:
            # First, try mapping via OxO on the provided CURIE
            oxo_curie = BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(curie)
            mapping = self._oxo_concept_mapper.map_to_omop(oxo_curie)

            if mapping is None and normalized_nodes is not None and curie in normalized_nodes and \
                    normalized_nodes[curie] is not None:
                # Try OxO on each of the equivalent identifiers from SRI Node Normalizer
                equivalent_ids = normalized_nodes[curie]['equivalent_identifiers']
                for identifier in equivalent_ids:
                    # The original CURIE was already tried above, skip it
                    if identifier == curie:
                        continue

                    oxo_curie = BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(identifier['identifier'])
                    mapping = self._oxo_concept_mapper.map_to_omop(oxo_curie)
                    if mapping is not None:
                        # Find the input label from SRI Normalizer
                        input_label = None
                        for sri_mapping in equivalent_ids:
                            if sri_mapping['identifier'] == curie:
                                input_label = sri_mapping.get('label')
                                break

                        # Edit the mapping object to add the SRI Node Normalizer as the first step
                        mapping.add_history(input_id=curie, output_id=mapping.input_id, source='SRI Normalizer',
                                            input_label=input_label, output_label=identifier.get('label'),
                                            distance=1, index=0)
                        mapping.input_id = curie
                        mapping.input_label = input_label

                        break

            omop_mappings[curie] = mapping

        return omop_mappings, normalized_nodes

    @cache.memoize(timeout=7257600, cache_none=True, unless=lambda: BiolinkConceptMapper.force_update_cache)
    def map_from_omop(self, concept_id: int, blm_category: str) -> Tuple[Optional[Mapping], Optional[List]]:
        """ Map from OMOP concept to appropriate domain-specific ontology.

        Parameters
        ----------
        concept_id: OMOP concept ID
        blm_category: biolink model category of the concept

        Returns
        -------
        tuple: (Mapping object or None, list of categories or None)
        """
        if blm_category not in self.biolink_mappings_as_oxo:
            # No target ontologies defined for this Biolink category
            return None, None

        # Get mappings from ConceptMapper
        target_ontologies = self.biolink_mappings_as_oxo[blm_category]
        mappings = self._oxo_concept_mapper.map_from_omop_to_target(concept_id, target_ontologies)

        if mappings is None:
            return None, None

        # For each of the mappings, change the prefix to the Biolink Model convention
        for mapping in mappings:
            # Convert from OxO prefix to BLM prefix. If the prefix isn't in the mappings between BLM and OxO, keep the
            # OxO prefix
            mapping.output_id = BiolinkConceptMapper.map_oxo_prefixes_to_blm_prefixes(mapping.output_id)

        # Get the canonical BLM ID for each curie from the OxO mapping
        curies = [x.output_id for x in mappings]
        normalized_nodes = SriNodeNormalizer.get_normalized_nodes(curies)

        # Find the mapping with the shortest distance canonical mapping
        if normalized_nodes is not None:
            for d in range(self.distance + 1):
                # Get all mappings with the current distance
                m_d = [m for m in mappings if m.get_distance() == d]
                for m in m_d:
                    cm_target_curie = m.output_id
                    if cm_target_curie in normalized_nodes and normalized_nodes[cm_target_curie] is not None:
                        normalized_node = normalized_nodes[cm_target_curie]
                        canonical_node = normalized_node['id']
                        normalized_categories = normalized_node['type']

                        # If SRI normalizer suggests a different ID as canonical, add the mapping provenance
                        if canonical_node['identifier'] != m.output_id:
                            m.add_history(input_id=m.output_id, output_id=canonical_node['identifier'],
                                          source='SRI Normalizer', input_label=m.output_label,
                                          output_label=canonical_node.get('label'), distance=1)
                            m.output_id = canonical_node['identifier']
                        if 'label' in canonical_node:
                            m.output_label = canonical_node['label']

                        return m, normalized_categories

        # Did not find any canonical nodes from SRI Node Normalizer
        # Choose one of the mappings to be the main identifier for the node. Prioritize distance first, and then
        # choose by the order of prefixes listed in the Concept Mapper.
        blm_prefixes = self.biolink_mappings.get(blm_category, [])
        for d in range(self.distance + 1):
            # Get all mappings with the current distance
            m_d = [m for m in mappings if m.get_distance() == d]

            # Look for the first matching prefix in the list of biolink prefixes
            for prefix in blm_prefixes:
                for m in m_d:
                    if m.output_id.split(':')[0] == prefix:
                        # Found the priority prefix with the shortest distance. Return the mapping
                        return m, None

        return None, None

    @staticmethod
    def clear_cache():
        """ Clears the Flask cache for BiolinkConceptMapper
        """
        cache.delete_memoized(BiolinkConceptMapper.map_to_omop)
        cache.delete_memoized(BiolinkConceptMapper.map_from_omop)

    @staticmethod
    def build_cache_map_from() -> Tuple[str, int]:
        """ Calls the BiolinkConceptMapper's map_from_omop on all concepts with data in COHD to build the cache

        This function starts another thread to run the build.

        Returns
        -------
        Number of concepts
        """
        thread = threading.Thread(target=BiolinkConceptMapper._build_cache_map_from, daemon=True)
        thread.start()
        return 'Build started.', 200

    # Flag to indicate that COHD is currently in the process of rebuilding the cache
    rebuilding_cache = False

    @staticmethod
    def _build_cache_map_from() -> int:
        """ Calls the BiolinkConceptMapper's map_from_omop on all concepts with data in COHD to build the cache

        Returns
        -------
        Number of concepts
        """
        # Check if there is already a thread running this build
        if BiolinkConceptMapper.rebuilding_cache:
            # There is already a thread running a build
            print('_build_cache_map_from already running. Will not start another thread.')
            return

        BiolinkConceptMapper.rebuilding_cache = True
        print(f'{datetime.now()}: Building cache for BiolinkConceptMapper::map_from_omop')

        mapper = BiolinkConceptMapper(biolink_mappings=None, distance=CohdTrapi.default_mapping_distance,
                                      local_oxo=CohdTrapi.default_local_oxo)
        concepts = query_active_concepts()
        error_count = 0
        for i, concept in enumerate(concepts):
            try:
                BiolinkConceptMapper.force_update_cache = True
                blm_category = map_omop_domain_to_blm_class(concept['domain_id'], concept['concept_class_id'])
                mapper.map_from_omop(concept['concept_id'], blm_category)
            except pymysql.err.Error as e:
                # Occasionally, MySQL server has issues. Log the issue and move on
                print(e)
                error_count += 1
                if error_count >= 10:
                    # Stop if we encounter too many errors
                    print('BiolinkConceptMapper::build_cache_map_from encountered multiple consecutive errors. '
                          'Stopping build. ')
                    break
            else:
                # Reset the error count
                error_count = 0
            finally:
                BiolinkConceptMapper.force_update_cache = False

            if i % 1000 == 0:
                print(f'{datetime.now()}: {i} / {len(concepts)} concepts mapped')

        print(f'{datetime.now()}: Cache build complete.')

        BiolinkConceptMapper.rebuilding_cache = False
        return len(concepts)


class SriNodeNormalizer:
    base_url = 'https://nodenormalization-sri.renci.org/1.1/'
    endpoint_get_normalized_nodes = 'get_normalized_nodes'

    @staticmethod
    def get_normalized_nodes(curies: List[str]) -> Optional[Dict[str, Any]]:
        """ Straightforward call to get_normalized_nodes. Returns json from response.

        Parameters
        ----------
        curies - list of curies

        Returns
        -------
        JSON response from endpoint or None. Each input curie will be a key in the response. If no normalized node is
        found, the entry will be null.
        """
        url = urljoin(SriNodeNormalizer.base_url, SriNodeNormalizer.endpoint_get_normalized_nodes)
        response = requests.post(url=url, json={'curies': curies})
        if response.status_code == 200:
            return response.json()
        else:
            return None

    @staticmethod
    def get_canonical_identifiers(curies: List[str]) -> Union[Dict[str, Union[str, None]], None]:
        """ Retrieve the canonical identifier

        Parameters
        ----------
        curies - list of CURIES

        Returns
        -------
        dict of canonical identifiers for each curie. If curie not found, then None
        """
        j = SriNodeNormalizer.get_normalized_nodes(curies)
        if j is None:
            return None

        canonical = dict()
        for curie in curies:
            if curie in j and j[curie] is not None:
                canonical[curie] = j[curie]['id']
            else:
                canonical[curie] = None
        return canonical


class OntologyKP:
    base_url = 'https://stars-app.renci.org/sparql-kp/'
    endpoint_query = 'query'

    @staticmethod
    def get_descendants(curies: List[str], categories: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """ Get descendant CURIEs from Ontology KP

        Parameters
        ----------
        curies - list of curies
        categories - list of biolink categories, or None

        Returns
        -------
        All knowledge graph nodes returned by the Ontology KP. If any errors, an emtpy dict is returned.
        """
        # Ontology KP doesn't seem to like it when categories is null. Replace it with NamedThing for functionally
        # equivalent TRAPI
        if categories is None:
            categories = ['biolink:NamedThing']

        try:
            # Query Ontology KP for descendants
            m = {
                "message": {
                    "query_graph": {
                        "nodes": {
                            "a": {
                                "ids": curies
                            },
                            "b": {
                                "categories": categories
                            }
                        },
                        "edges": {
                            "ab": {
                                "subject": "b",
                                "object": "a",
                                "predicate": "biolink:subclass_of"
                            }
                        }
                    }
                }
            }
            url = urljoin(OntologyKP.base_url, OntologyKP.endpoint_query)
            response = requests.post(url=url, json=m)
            if response.status_code == 200:
                j = response.json()
                if 'message' in j and 'knowledge_graph' in j['message']:
                    nodes = j['message']['knowledge_graph'].get('nodes')
                    if nodes is not None:
                        # Return all nodes in KG, including reflexive CURIE node
                        return nodes
                    else:
                        # Return an empty dict, indicating no descendants found
                        return dict()
            else:
                logging.warning(f'Ontology KP returned status code {response.status_code}: {response.content}')
        except requests.RequestException:
            # Return None, indicating an error occurred
            logging.warning('Encountered an RequestException when querying descendants from Ontology KP')
            return None

        # Return None, indicating an error occurred
        return None


def sort_cohd_results(cohd_results, sort_field=None, ascending=None):
    """ Sort the COHD results

    Parameters
    ----------
    cohd_results
    sort_field - String: name of dictionary key to sort by
    ascending - Bool:

    Returns
    -------
    Sorted COHD results
    """
    if cohd_results is None or len(cohd_results) == 0:
        return cohd_results

    if sort_field is None or len(sort_field) == 0:
        # See what fields are in the first result, assume the rest are the same
        r = cohd_results[0]
        if 'p-value' in r:
            sort_field = 'p-value'
            if ascending is None:
                ascending = False
        elif 'ln_ratio' in r:
            sort_field = 'ln_ratio'
            if ascending is None:
                ascending = False
        elif 'relative_frequency' in r:
            sort_field  = 'relative_frequency'
            if ascending is None:
                ascending = False

    sort_values = [x[sort_field] for x in cohd_results]
    results_sorted = [cohd_results[i] for i in argsort(sort_values)]
    if not ascending:
        results_sorted = list(reversed(results_sorted))
    return results_sorted
