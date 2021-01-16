from abc import ABC, abstractmethod

from .cohd_utilities import ln_ratio_ci, ci_significance
from .omop_xref import ConceptMapper


class CohdTrapi(ABC):
    """
    Abstract class for TRAPI endpoint implementation conforming to NCATS Biodmedical Data Translator Reasoner API Spec.
    This abstract class will be implemented for different versions of TRAPI
    """

    @abstractmethod
    def __init__(self, request):
        """ Constructor should take a flask request object """
        assert request is not None, 'cohd_trapi.py::CohdTrapi::__init__() - Bad request'

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
    default_min_cooccurrence = 0
    default_confidence_interval = 0.99
    default_dataset_id = 3
    default_local_oxo = True
    default_mapping_distance = 3
    default_biolink_only = True
    supported_query_methods = ['relativeFrequency', 'obsExpRatio', 'chiSquare']
    # set of edge types that are supported by the COHD Reasoner
    supported_edge_types = {
        'biolink:correlated_with',  # Currently, COHD models all relations using biolink:correlated_with
        'biolink:related_to',  # Ancestor of biolink:correlated_with
        # Allow edge without biolink prefix
        'correlated_with',
        'related_to',
        # Old documentation incorrectly suggested using 'association'. Permit this for now, but remove in future
        'biolink:association',
        'association',
    }


class ResultCriteria:
    """
    Stores a defined criterion to be applied to a COHD result
    """

    def __init__(self, function, kargs):
        """ Constructor

        Parameters
        ----------
        function: function
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


def criteria_confidence(cohd_result, alpha):
    """ Checks the confidence interval of the result for significance using alpha. Only applies to observed-expected
    frequency ratio. Returns True for all other types of results.

    Parameters
    ----------
    cohd_result
    alpha

    Returns
    -------
    True if significant
    """
    if 'ln_ratio' in cohd_result:
        # obsExpFreq
        ci = ln_ratio_ci(cohd_result['observed_count'], cohd_result['ln_ratio'], alpha)
        return ci_significance(ci)
    else:
        # relativeFrequency doesn't have a good cutoff for confidence interval, and chiSquare uses
        # p-value for signficance, so allow methods other than obsExpRatio to pass
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
        'biolink:device': 'biolink:Device',
        'biolink:disease': 'biolink:Disease',
        'biolink:drug': 'biolink:Drug',
        'biolink:phenomenon': 'biolink:Phenomenon',
        'biolink:population_of_individual_organisms': 'biolink:PopulationOfIndividualOrganisms',
        'biolink:procedure': 'biolink:Procedure'
    }
    blm_category = supported_type_conversions.get(blm_category, blm_category)

    return blm_category


def map_blm_class_to_omop_domain(node_type):
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
        'biolink:Device': ['Device'],
        'biolink:Disease': ['Condition'],
        'biolink:Drug': ['Drug'],
        'biolink:Phenomenon': ['Measurement', 'Observation'],
        'biolink:PopulationOfIndividualOrganisms': ['Ethnicity', 'Gender', 'Race'],
        'biolink:Procedure': ['Procedure']
    }
    return mappings.get(node_type)


def map_omop_domain_to_blm_class(domain):
    """ Maps the OMOP domain_id to Biolink Model class, e.g., 'Condition' to 'biolink:Disease'

    Parameters
    ----------
    domain - OMOP domain, e.g., 'Condition'

    Returns
    -------
    If normalized successfully: Biolink Model semantic type. If no mapping found, use NamedThing
    """
    mappings = {
        'Condition': 'biolink:Disease',
        'Device': 'biolink:Device',
        'Drug': 'biolink:Drug',
        'Ethnicity': 'biolink:PopulationOfIndividualOrganisms',
        'Gender': 'biolink:PopulationOfIndividualOrganisms',
        'Measurement': 'biolink:Phenomenon',
        'Observation': 'biolink:Phenomenon',
        'Procedure': 'biolink:Procedure',
        'Race': 'biolink:PopulationOfIndividualOrganisms'
    }
    default_type = 'named_thing'
    return mappings.get(domain, default_type)


class BiolinkConceptMapper:
    """ Maps between OMOP concepts and Biolink Model

    When mapping from OMOP conditions to Biolink Model diseases, since SNOMED-CT, ICD10CM, ICD9CM, and MedDRA are now
    included in Biolink Model, map to these source vocabularies using the OMOP concept definitions. When mapping from
    other ontologies to OMOP, leverage the OxO as well.
    """

    _mappings_prefixes_blm_to_oxo = {
        # Disease prefixes
        'MONDO': 'MONDO',
        'DOID': 'DOID',
        'OMIM': 'OMIM',
        'ORPHANET': 'Orphanet',
        'ORPHA': None,
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
        'HP': 'HP',
        'MP': 'MP',
        # Drug prefixes
        'PHARMGKB.DRUG': None,
        'CHEBI': 'CHEBI',
        'CHEMBL.COMPOUND': None,
        'DRUGBANK': 'DrugBank',
        'PUBCHEM.COMPOUND': 'PubChem_Compound',
        'HMDB': 'HMDB',
        'INCHI': None,
        'UNII': None,
        'KEGG': 'KEGG',
        'gtpo': None,
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
        # Drug prefixes
        'CHEBI': 'CHEBI',
        'DrugBank': 'DRUGBANK',
        'PubChem_Compound': 'PUBCHEM.COMPOUND',
        'HMDB': 'HMDB',
        'KEGG': 'KEGG',
        # Procedure prefixes
    }

    _default_ontology_map = {
        'biolink:Disease': ['MONDO', 'DOID', 'OMIM', 'ORPHANET', 'ORPHA', 'EFO', 'UMLS', 'MESH', 'MEDDRA',
                            'NCIT', 'SNOMEDCT', 'medgen', 'ICD10', 'ICD9', 'ICD0', 'HP', 'MP'],
        # Note: for Drug, also map to some of the prefixes specified in ChemicalSubstance
        'biolink:Drug': ['PHARMGKB.DRUG', 'CHEBI', 'CHEMBL.COMPOUND', 'DRUGBANK', 'PUBCHEM.COMPOUND', 'MESH',
                         'HMDB', 'INCHI', 'UNII', 'KEGG', 'gtpo'],
        # Note: There are currently no prefixes allowed for Procedure in Biolink, so use some standard OMOP mappings
        'biolink:Procedure': ['ICD10PCS', 'SNOMEDCT'],
        '_DEFAULT': []
    }

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

    def __init__(self, biolink_mappings=None, distance=2, local_oxo=True):
        """ Constructor

        Parameters
        ----------
        biolink_mappings: mappings between domain and ontology. See documentation for ConceptMapper.
        distance: maximum allowed total distance (as opposed to OxO distance)
        local_oxo: use local implementation of OxO (default: True)
        """
        if biolink_mappings is None:
            biolink_mappings = BiolinkConceptMapper._default_ontology_map

        self.biolink_mappings = biolink_mappings
        self.distance = distance
        self.local_oxo = local_oxo

        # Convert Biolink Model prefix conventions to OxO conventions
        oxo_mappings = dict()
        for blm_type, prefixes in list(self.biolink_mappings.items()):
            omop_domains = map_blm_class_to_omop_domain(blm_type)
            if omop_domains is None or not omop_domains:
                continue

            # Map each prefix
            domain_mappings = [BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(prefix) for prefix in prefixes]
            # Remove None from list
            domain_mappings = [m for m in domain_mappings if m is not None]
            for omop_domain in omop_domains:
                oxo_mappings[omop_domain] = domain_mappings

        self._oxo_concept_mapper = ConceptMapper(oxo_mappings, self.distance, self.local_oxo)

    def map_to_omop(self, curie):
        """ Map to OMOP concept from ontology

        Parameters
        ----------
        curie: CURIE

        Returns
        -------
        Dict like:
        {
            "omop_concept_name": "Osteoarthritis",
            "omop_concept_id": 80180,
            "distance": 2
        }
        or None
        """
        # Convert from Biolink Model prefix to OxO prefix. If the prefix isn't in the mappings between BLM and OxO, try
        # the mapping with the original prefix
        oxo_curie = BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(curie)

        # Get mappings from ConceptMapper
        return self._oxo_concept_mapper.map_to_omop(oxo_curie)

    def map_from_omop(self, concept_id, domain_id=None):
        """ Map from OMOP concept to appropriate domain-specific ontology.

        Parameters
        ----------
        concept_id: OMOP concept ID
        domain_id: OMOP concept's domain. Will look it up if not specified

        Returns
        -------
        Array of dict like:
        [{
            "target_curie": "UMLS:C0154091",
            "target_label": "Carcinoma in situ of bladder",
            "distance": 1
        }]
        or None
        """
        # Get mappings from ConceptMapper
        mappings = self._oxo_concept_mapper.map_from_omop(concept_id, domain_id)

        # For each of the mappings, change the prefix to the Biolink Model convention
        for mapping in mappings:
            # Convert from OxO prefix to BLM prefix. If the prefix isn't in the mappings between BLM and OxO, keep the
            # OxO prefix
            mapping['target_curie'] = BiolinkConceptMapper.map_oxo_prefixes_to_blm_prefixes(mapping['target_curie'])

        return mappings
