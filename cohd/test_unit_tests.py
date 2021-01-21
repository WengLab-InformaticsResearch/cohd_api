"""
This test module tests some of the utility functions supporting the COHD API
"""
from . import cohd_utilities
from . import omop_xref
import numpy as np
import numbers
import requests
from time import sleep
from collections import defaultdict


def _isnumeric(number_list):
    """ Checks if all elements in x are numbers

    Parameters
    ----------
    number_list - iterable

    Returns
    -------
    True if all elements are numbers
    """
    return all(isinstance(n, numbers.Number) for n in number_list)


def _crr(x, y, dp=4):
    """ Compares results of iterables of numbers for equality rounded to a decimal place

    Parameters
    ----------
    x - iterable of numbers (e.g., list, tuple, etc)
    y - iterable of numbers (e.g., list, tuple, etc)
    dp - decimal places to round to

    Returns
    -------
    True if x and y are equal up to the specified dceimal plcae
    """
    return (len(x) == len(y)) and all(np.round(x, dp) == np.round(y, dp))


# ######################################################################################################################
# This section tests cohd_utilities.py
# ######################################################################################################################


def test_cohd_double_poisson_ci():
    """ Tests cohd_utiltities.double_poisson_ci.
    Checks the results with multiple parameters have the expected format and match the expected values.
    Each result should be a tuple with two numeric values giving the lower and upper bounds of the confidence interval.

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    x = cohd_utilities.double_poisson_ci(50)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (29.0, 75.0), dp=1)

    x = cohd_utilities.double_poisson_ci(50, 0.99)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (29.0, 75.0), dp=1)

    x = cohd_utilities.double_poisson_ci(50, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (33.0, 68.0), dp=1)

    x = cohd_utilities.double_poisson_ci(5000)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (4769.0, 5235.0), dp=1)

    x = cohd_utilities.double_poisson_ci(5000, 0.99)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and  _crr(x, (4769.0, 5235.0), dp=1)

    x = cohd_utilities.double_poisson_ci(5000, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (4829.0, 5173.0), dp=1)


def test_ln_ratio_ci():
    """ Tests cohd_utiltities.ln_ratio_ci.
    Checks the results with multiple parameters have the expected format and match the expected values.
    Each result should be a tuple with two numeric values giving the lower and upper bounds of the confidence interval.

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    x = cohd_utilities.ln_ratio_ci(50, 2.0)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (1.4552728245583282, 2.4054651081081646))

    x = cohd_utilities.ln_ratio_ci(50, 2.0, 0.99)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (1.4552728245583282, 2.4054651081081646))

    x = cohd_utilities.ln_ratio_ci(50, 2.0, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (1.584484556038334, 2.3074846997479606))

    x = cohd_utilities.ln_ratio_ci(50, 5.0, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (4.584484556038334, 5.3074846997479606))

    x = cohd_utilities.ln_ratio_ci(5000, 2.0)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (1.9526987268819869, 2.0459289318883997))

    x = cohd_utilities.ln_ratio_ci(5000, 2.0, 0.99)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (1.9526987268819869, 2.0459289318883997))

    x = cohd_utilities.ln_ratio_ci(5000, 2.0, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (1.9652014944573044, 2.0340148785872776))

    x = cohd_utilities.ln_ratio_ci(5000, 5.0, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (4.965201494457304, 5.034014878587278))


def test_rel_freq_ci():
    """ Tests cohd_utiltities.rel_freq_ci.
    Checks the results with multiple parameters have the expected format and match the expected values.
    Each result should be a tuple with two numeric values giving the lower and upper bounds of the confidence interval.

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    dp = 6  # decimal places to check results

    x = cohd_utilities.rel_freq_ci(50, 5000)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.0055396370582617, 0.015726567414552316), dp)

    x = cohd_utilities.rel_freq_ci(50, 5000, 0.99)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.0055396370582617, 0.015726567414552316), dp)

    x = cohd_utilities.rel_freq_ci(50, 5000, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.006379277015271603, 0.01408159039138538), dp)

    x = cohd_utilities.rel_freq_ci(50, 100, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.264, 0.8831168831168831), dp)

    x = cohd_utilities.rel_freq_ci(5000, 500000)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.009493796881718718, 0.01051889180912883), dp)

    x = cohd_utilities.rel_freq_ci(5000, 500000, 0.99)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.009493796881718718, 0.01051889180912883), dp)

    x = cohd_utilities.rel_freq_ci(5000, 500000, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.009624852009678667, 0.010381713093040057), dp)

    x = cohd_utilities.rel_freq_ci(5000, 10000, 0.95)
    assert isinstance(x, tuple) and len(x) == 2 and _isnumeric(x) and _crr(x, (0.47135187896534897, 0.5301834580301322), dp)


def test_ci_significance():
    """ Tests cohd_utilities.ci_significance.
    Checks the results with multiple parameters have the expected format and match the expected values.
    Each result should be a boolean indicating 1) if only a single CI is given, whether the CI includes 0 or 2) if two
    CIs are given, whether the CIs overlap.

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    # Single CI not including 0, should be true
    x = cohd_utilities.ci_significance([10, 20])
    assert isinstance(x, bool) and x

    # Single CI not including 0, should be true
    x = cohd_utilities.ci_significance([-20, -10])
    assert isinstance(x, bool) and x

    # Single CI including 0, should be false
    x = cohd_utilities.ci_significance([-20, 10])
    assert isinstance(x, bool) and not x

    # Two CIs not overlapping, should be true
    x = cohd_utilities.ci_significance([-20, -10], [-9, -5])
    assert isinstance(x, bool) and x

    # Two CIs not overlapping, should be true
    x = cohd_utilities.ci_significance([5, 9], [10, 20])
    assert isinstance(x, bool) and x

    # Two CIs overlapping, should be false
    x = cohd_utilities.ci_significance([-20, -10], [-11, -5])
    assert isinstance(x, bool) and not x

    # Two CIs overlapping, should be false
    x = cohd_utilities.ci_significance([5, 11], [10, 20])
    assert isinstance(x, bool) and not x


def test_omop_concept_uri():
    """ Tests cohd_utilities.omop_concept_uri
    Checks the expected format of the returned URI and checks the expected response type from fetching the URI

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    # Check a valid OMOP concept ID
    x = cohd_utilities.omop_concept_uri('313217')
    # Check that the URI is formatted correctly
    assert x == 'http://api.ohdsi.org/WebAPI/vocabulary/concept/313217'
    # The URI should have a valid response
    try:
        response = requests.get(x, timeout=5)
        assert response.status_code == requests.status_codes.codes.OK
    except requests.exceptions.ConnectionError:
        # OHDSI API not always stable. Ignore connection errors
        pass

    # Check a valid OMOP concept ID passed in as an integer
    x = cohd_utilities.omop_concept_uri(313217)
    # Check that the URI is formatted correctly
    assert x == 'http://api.ohdsi.org/WebAPI/vocabulary/concept/313217'
    # The URI should have a valid response
    try:
        response = requests.get(x, timeout=5)
        assert response.status_code == requests.status_codes.codes.OK
    except requests.exceptions.ConnectionError:
        # OHDSI API not always stable. Ignore connection errors
        pass

    # Check an invalid OMOP concept ID
    x = cohd_utilities.omop_concept_uri('313217000000')
    # Check that the URI is formatted correctly
    assert x == 'http://api.ohdsi.org/WebAPI/vocabulary/concept/313217000000'
    # The URI should have a missing response
    try:
        response = requests.get(x, timeout=5)
        assert response.status_code == requests.status_codes.codes.OK
    except requests.exceptions.ConnectionError:
        # OHDSI API not always stable. Ignore connection errors
        pass


def test_omop_concept_curie():
    """ Tests cohd_utilities.omop_concept_curie
    Checks the expected format of the returned CURIE

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    # Check that a concept ID passed as a string is formatted properly
    x = cohd_utilities.omop_concept_curie('313217')
    assert x == 'OMOP:313217'

    # Check that a concept ID passed as an integer is formatted properly
    x = cohd_utilities.omop_concept_curie(313217)
    # Check that the URI is formatted correctly
    assert x == 'OMOP:313217'


# ######################################################################################################################
# This section tests omop_xref.py
# Note: this can only test the functions that don't rely on the SQL database
# ######################################################################################################################
def test_omop_vocab_to_oxo_prefix():
    """ Tests omop_xref.omop_vocab_to_oxo_prefix
    Checks that omop_vocab_to_oxo_prefix produces the expected OxO prefixes

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    assert omop_xref.omop_vocab_to_oxo_prefix('ICD9CM') == 'ICD9CM' and \
           omop_xref.omop_vocab_to_oxo_prefix('ICD10CM') == 'ICD10CM' and \
           omop_xref.omop_vocab_to_oxo_prefix('SNOMED') == 'SNOMEDCT' and \
           omop_xref.omop_vocab_to_oxo_prefix('MeSH') == 'MeSH'


def test_oxo_search():
    """ Tests omop_xref.oxo_search
    Uses oxo_search to make multiple requests to OxO and checks that the results have the expected formats, expected
    number of search results (i.e., one search result for each CURIE queried), and checks how the number of mappings
    compare across different calls with different parameter settings (i.e., more mappings when larger distances are
    used).

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    oxo_response_keys = ['_links', '_embedded', 'page']

    # Check oxo_search with a known CURIE that should produce matches
    json = omop_xref.oxo_search('DOID:8398', distance=2)
    # Check the general response format
    assert json is not None and isinstance(json, dict) and all(k in json for k in oxo_response_keys)
    # Searched for 1 CURIE, expect searchResults with length 1
    assert len(json['_embedded']['searchResults']) == 1
    # Check that the query produced mappings, but don't verify the mappings themselves
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) > 0
    # Keep track of the number of results for comparison against following queries
    comparison_length = len(json['_embedded']['searchResults'][0]['mappingResponseList'])

    # Sleep for 2 seconds so we don't overload OxO
    sleep(2)

    # Check oxo_search with a known CURIE with a shorter distance and check that there are fewer results
    json = omop_xref.oxo_search('DOID:8398', distance=1)
    # Check the general response format
    assert json is not None and isinstance(json, dict) and all(k in json for k in oxo_response_keys)
    # Searched for 1 CURIE, expect searchResults with length 1
    assert len(json['_embedded']['searchResults']) == 1
    # Check that the query produced mappings, but don't verify the mappings themselves
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) > 0
    # Keep track of the number of results for comparison against following queries
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) <= comparison_length

    # Sleep for 2 seconds so we don't overload OxO
    sleep(2)

    # Check oxo_search with a known CURIE with a longer distance and check that there are fewer results
    json = omop_xref.oxo_search('DOID:8398', distance=3)
    # Check the general response format
    assert json is not None and isinstance(json, dict) and all(k in json for k in oxo_response_keys)
    # Searched for 1 CURIE, expect searchResults with length 1
    assert len(json['_embedded']['searchResults']) == 1
    # Check that the query produced mappings, but don't verify the mappings themselves
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) > 0
    # Keep track of the number of results for comparison against following queries
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) >= comparison_length

    # Sleep for 2 seconds so we don't overload OxO
    sleep(2)

    # Check oxo_search with a known CURIE with a restricted mapping targets and check that there are fewer results
    json = omop_xref.oxo_search('DOID:8398', mapping_targets=['ICD10CM'], distance=2)
    # Check the general response format
    assert json is not None and isinstance(json, dict) and all(k in json for k in oxo_response_keys)
    # Searched for 1 CURIE, expect searchResults with length 1
    assert len(json['_embedded']['searchResults']) == 1
    # Check that the query produced mappings, but don't verify the mappings themselves
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) > 0
    # Keep track of the number of results for comparison against following queries
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) <= comparison_length

    # Sleep for 2 seconds so we don't overload OxO
    sleep(2)

    # Check oxo_search with one valid CURIE and one fake CURIE
    json = omop_xref.oxo_search(['DOID:8398', 'DOID:83980000'], distance=1)
    # Check the general response format
    assert json is not None and isinstance(json, dict) and all(k in json for k in oxo_response_keys)
    # Searched for 2 CURIEs, expect searchResults with length 2
    assert len(json['_embedded']['searchResults']) == 2
    # Check that the first query produced mappings and the second query produced no mappings
    assert len(json['_embedded']['searchResults'][0]['mappingResponseList']) > 0 and \
           len(json['_embedded']['searchResults'][1]['mappingResponseList']) == 0


def test_xref_best_from():
    """ Tests omop_xref._xref_best_from
    Test _xref_best_from by passing in a list of mappings with multiple mappings in each target ontology. Make sure that
    the result only produces one mapping in each target ontology. Since the method of choosing the best mapping for each
    ontology is not specified, do not check which actual mappings were chosen.

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    mappings = [
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "N/A (OMOP mapping)",
          "intermediate_oxo_label": "N/A (OMOP mapping)",
          "omop_distance": 0,
          "oxo_distance": 0,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:92546004",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 0
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "N/A (OMOP mapping)",
          "intermediate_oxo_label": "N/A (OMOP mapping)",
          "omop_distance": 1,
          "oxo_distance": 0,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD10CM:D09.0",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "N/A (OMOP mapping)",
          "intermediate_oxo_label": "N/A (OMOP mapping)",
          "omop_distance": 1,
          "oxo_distance": 0,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD9CM:233.7",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "154638003",
          "intermediate_omop_concept_id": 40320711,
          "intermediate_omop_concept_name": "Ca in situ bladder (& papilloma)",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "N/A (OMOP mapping)",
          "intermediate_oxo_label": "N/A (OMOP mapping)",
          "omop_distance": 1,
          "oxo_distance": 0,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:154638003",
          "target_label": "Ca in situ bladder (& papilloma)",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "N/A (OMOP mapping)",
          "intermediate_oxo_label": "N/A (OMOP mapping)",
          "omop_distance": 1,
          "oxo_distance": 0,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:269650008",
          "target_label": "Bladder Ca in situ",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_label": "N/A (OMOP-UMLS mapping)",
          "omop_distance": 1,
          "oxo_distance": 0,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C0154091",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:92546004",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 0,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "DOID:9053",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:92546004",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 0,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C0154091",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 1
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "DOID:9053",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:92546004",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 0,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD9CM:233.7",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:92546004",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 0,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "MONDO:0004703",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:92546004",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 0,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD10CM:D09.0",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD9CM:233.7",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C1536825",
          "target_label": "Ca in situ bladder (& papilloma)",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "MONDO:0004703",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "NCIT:C3644",
          "target_label": "Stage 0is Bladder Urothelial Carcinoma",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:92546004",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "92546004",
          "intermediate_omop_concept_id": 192855,
          "intermediate_omop_concept_name": "Cancer in situ of urinary bladder",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:92546004",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 0,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "NCIT:C3644",
          "target_label": "Stage 0is Bladder Urothelial Carcinoma",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD10CM:D09.0",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "154638003",
          "intermediate_omop_concept_id": 40320711,
          "intermediate_omop_concept_name": "Ca in situ bladder (& papilloma)",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:154638003",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C1536825",
          "target_label": "Ca in situ bladder (& papilloma)",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "DOID:9053",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "MONDO:0004703",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "DOID:9053",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "MONDO:0004703",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C0154091",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C0154091",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 1,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "DOID:9053",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 2
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD9CM:233.7",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "NCIT:C3644",
          "target_label": "Stage 0is Bladder Urothelial Carcinoma",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:269650008",
          "target_label": "Ca in situ bladder (& papilloma)",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_concept_name": "N/A (OMOP-UMLS mapping)",
          "intermediate_omop_vocabulary_id": "N/A (OMOP-UMLS mapping)",
          "intermediate_oxo_curie": "UMLS:C0154091",
          "intermediate_oxo_label": "Cancer in situ of urinary bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:269650008",
          "target_label": "Ca in situ bladder (& papilloma)",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD10CM:D09.0",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:269650008",
          "target_label": "Ca in situ bladder (& papilloma)",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "MONDO:0004703",
          "target_label": "bladder carcinoma in situ",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "NCIT:C3644",
          "target_label": "Stage 0is Bladder Urothelial Carcinoma",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD10CM:D09.0",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "UMLS:C0154091",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "ICD9CM:233.7",
          "target_label": "Carcinoma in situ of bladder",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "269650008",
          "intermediate_omop_concept_id": 40353074,
          "intermediate_omop_concept_name": "Bladder Ca in situ",
          "intermediate_omop_vocabulary_id": "SNOMED",
          "intermediate_oxo_curie": "SNOMEDCT:269650008",
          "intermediate_oxo_label": "Ca in situ bladder (& papilloma)",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "NCIT:C3644",
          "target_label": "Stage 0is Bladder Urothelial Carcinoma",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "D09.0",
          "intermediate_omop_concept_id": 35206494,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD10CM",
          "intermediate_oxo_curie": "ICD10CM:D09.0",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:92546004",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 3
        },
        {
          "intermediate_omop_concept_code": "233.7",
          "intermediate_omop_concept_id": 44824068,
          "intermediate_omop_concept_name": "Carcinoma in situ of bladder",
          "intermediate_omop_vocabulary_id": "ICD9CM",
          "intermediate_oxo_curie": "ICD9CM:233.7",
          "intermediate_oxo_label": "Carcinoma in situ of bladder",
          "omop_distance": 1,
          "oxo_distance": 2,
          "source_omop_concept_code": "92546004",
          "source_omop_concept_id": 192855,
          "source_omop_concept_name": "Cancer in situ of urinary bladder",
          "source_omop_vocabulary_id": "SNOMED",
          "target_curie": "SNOMEDCT:92546004",
          "target_label": "Cancer in situ of urinary bladder",
          "total_distance": 3
        }
      ]

    best_mappings = omop_xref._xref_best_from(mappings)

    # Check that the number of mappings is reduced
    assert len(best_mappings) < len(mappings)

    # Check that each target ontology only has one mapping
    target_ontology_count = defaultdict(lambda: 0)
    for best_mapping in best_mappings:
        prefix, _ = best_mapping['target_curie'].split(':')
        target_ontology_count[prefix] += 1
    assert all(v == 1 for v in list(target_ontology_count.values()))

    # Check that each of the best mappings was one of the original mappings
    assert all(bm in mappings for bm in best_mappings)


def test_xref_best_to():
    """ Tests omop_xref._xref_best_to
    Test _xref_best_to by passing in a list of mappings with multiple mappings to OMOP concepts. Make sure that the
    result only produces one mapping and that mapping came from the original list of mappings. Since the method of
    choosing the best mapping for each is not specified, do not check which actual mappings were chosen.

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    mappings = mappings = [
        {
          "intermediate_oxo_id": "SNOMEDCT:396275006",
          "intermediate_oxo_label": "Osteoarthritis",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 0,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 80180,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 2
        },
        {
          "intermediate_oxo_id": "UMLS:C0157946",
          "intermediate_oxo_label": "Osteoarthrosis, localized, not specified whether primary or secondary",
          "omop_concept_name": "Osteoarthrosis, localized, not specified whether primary or secondary",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 36569338,
          "oxo_distance": 1,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 2
        },
        {
          "intermediate_oxo_id": "ICD9CM:715.3",
          "intermediate_oxo_label": "",
          "omop_concept_name": "Localized osteoarthrosis uncertain if primary OR secondary",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 72990,
          "oxo_distance": 1,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 2
        },
        {
          "intermediate_oxo_id": "MeSH:D010003",
          "intermediate_oxo_label": "Osteoarthritis",
          "omop_concept_name": "Degenerative polyarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 4025957,
          "oxo_distance": 1,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 2
        },
        {
          "intermediate_oxo_id": "ICD10CM:M15",
          "intermediate_oxo_label": "",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 80180,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0086743",
          "intermediate_oxo_label": "Osteoarthrosis Deformans",
          "omop_concept_name": "Osteoarthritis deformans",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 36569386,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0086743",
          "intermediate_oxo_label": "Osteoarthrosis Deformans",
          "omop_concept_name": "Osteoarthritis deformans",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 4110738,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0029408",
          "intermediate_oxo_label": "Osteoarthritis",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 36516824,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0029408",
          "intermediate_oxo_label": "Osteoarthritis",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Drug",
          "omop_standard_concept_id": 4344441,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0029408",
          "intermediate_oxo_label": "Osteoarthritis",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 80180,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0029408",
          "intermediate_oxo_label": "Osteoarthritis",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Meas Value",
          "omop_standard_concept_id": 45884766,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "ICD10CM:M19",
          "intermediate_oxo_label": "",
          "omop_concept_name": "Osteoarthritis",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 80180,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0409959",
          "intermediate_oxo_label": "Osteoarthritis, Knee",
          "omop_concept_name": "Osteoarthritis knees",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 36569323,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0409959",
          "intermediate_oxo_label": "Osteoarthritis, Knee",
          "omop_concept_name": "Osteoarthritis, Knee",
          "omop_distance": 1,
          "omop_domain_id": "Drug",
          "omop_standard_concept_id": 4268196,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        },
        {
          "intermediate_oxo_id": "UMLS:C0409959",
          "intermediate_oxo_label": "Osteoarthritis, Knee",
          "omop_concept_name": "Osteoarthritis of knee",
          "omop_distance": 1,
          "omop_domain_id": "Condition",
          "omop_standard_concept_id": 4079750,
          "oxo_distance": 2,
          "source_oxo_id": "DOID:8398",
          "source_oxo_label": "osteoarthritis",
          "total_distance": 3
        }
      ]
    x = omop_xref._xref_best_to(mappings)

    # Check that there is only one mapping returned
    assert len(x) == 1

    # Check that the returned mapping was one of the original mappings
    assert x[0] in mappings
