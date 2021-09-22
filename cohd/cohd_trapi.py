from abc import ABC, abstractmethod
import logging
from typing import Any, Iterable, Optional, Dict, List, Tuple
from enum import Enum
from numpy import argsort

from .cohd_utilities import ln_ratio_ci, ci_significance


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
    default_min_cooccurrence = 0
    default_confidence_interval = 0.99
    default_dataset_id = 3
    default_local_oxo = False
    default_mapping_distance = 3
    default_biolink_only = True
    default_max_results = 500
    default_log_level = logging.WARNING
    limit_max_results = 500
    supported_query_methods = ['relativeFrequency', 'obsExpRatio', 'chiSquare']

    # Deprecated. Only used in old versions of cohd_trapi_VERSION
    # Set of edge types that are supported by the COHD Reasoner. This list is in preferred order, most preferred first
    supported_edge_types = [
        'biolink:correlated_with',  # Currently, COHD models all relations using biolink:correlated_with
    ]

    # Mapping for which predicate should be used for each COHD analysis method. For now, it's all
    # has_real_world_evidence_of_association_with
    default_predicate = 'biolink:has_real_world_evidence_of_association_with'
    method_predicates = {
        'obsExpRatio': default_predicate,
        'relativeFrequency': default_predicate,
        'chiSquare': default_predicate
    }

    _INFORES_ID = 'infores:cohd'
    _SERVICE_NAME = 'COHD'

    def _get_kg_predicate(self) -> str:
        """ Determines which predicate should be used to represent the COHD analysis

        As of 2021-08-19, this may get deprecated soon with development of new methods of modeling results as attributes
        on the same edge rather than as separate edges with separate predicates

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
    if 'ln_ratio_ci' in cohd_result:
        # obsExpFreq
        return ci_significance(cohd_result['ln_ratio_ci'])
    elif 'ln_ratio' in cohd_result:
        # obsExpFreq
        ci = ln_ratio_ci(cohd_result['concept_pair_count'], cohd_result['ln_ratio'], confidence)
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
        'biolink:device': 'biolink:Device',
        'biolink:disease': 'biolink:Disease',
        'biolink:disease_or_phenotypic_feature': 'biolink:DiseaseOrPhenotypicFeature',
        'biolink:drug': 'biolink:Drug',
        'biolink:phenomenon': 'biolink:Phenomenon',
        'biolink:phenotypic_feature': 'biolink:PhenotypicFeature',
        'biolink:population_of_individual_organisms': 'biolink:PopulationOfIndividualOrganisms',
        'biolink:procedure': 'biolink:Procedure',
        'biolink:small_molecule': 'biolink:SmallMolecule'
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


def score_cohd_result(cohd_result):
    """ Get a score for a cohd result. We will use the absolute value of the smaller bound of the ln_ratio confidence
    interval. For confidence intervals that span 0 (e.g., [-.5, .5]), the score will be 0. For positive confidence
    intervals, use the lower bound. For negative confidence intervals, use the upper bound. If the ln_ratio_ci is not
    found, the score is 0.

    Parameters
    ----------
    cohd_result

    Returns
    -------
    score (float)
    """
    if 'ln_ratio_ci' in cohd_result:
        ci = cohd_result['ln_ratio_ci']
        if ci[0] > 0:
            score = ci[0]
        elif ci[1] < 0:
            score = abs(ci[1])
        else:
            score = 0
    else:
        score = 0
    return score


def sort_cohd_results(cohd_results, sort_field='ln_ratio_ci', ascending=False):
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

    if sort_field in ['p-value', 'ln_ratio', 'relative_frequency']:
        sort_values = [x[sort_field] for x in cohd_results]
    elif sort_field == 'ln_ratio_ci':
        sort_values = [score_cohd_result(x) for x in cohd_results]
    elif sort_field in ['relative_frequency_1_ci', 'relative_frequency_2_ci']:
        sort_values = [x[sort_field][0] for x in cohd_results]
    else:
        sort_values = [score_cohd_result(x) for x in cohd_results]
    results_sorted = [cohd_results[i] for i in argsort(sort_values)]
    if not ascending:
        results_sorted = list(reversed(results_sorted))
    return results_sorted
