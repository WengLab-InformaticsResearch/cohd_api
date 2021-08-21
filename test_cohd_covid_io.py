"""
This test module tests the COHD API by making requests to cohd.io/api and checking the schema of the response JSONs and
checking the results against known values.

Intended to be run with pytest: pytest -s test_cohd_covid_io.py
"""
from collections import namedtuple
from pprint import pformat

from notebooks.cohd_helpers import cohd_requests as cr
from cohd.trapi import reasoner_validator_11x

""" 
tuple for storing pairs of (key, type) for results schemas
"""
_s = namedtuple('_s', ['key', 'type'])

# Test the cohd.covid.io server
cr.server = 'https://covid.cohd.io/api'

# Proxy for main TRAPI version
reasoner_validator = reasoner_validator_11x
translator_query = cr.translator_query_110


def check_results_schema(json, schema):
    """ Checks that the json response contains a result object, and each result in the results array conforms to the
    schema.

    Parameters
    ----------
    json - json dict to check
    schema - defined schema to check against. schema is a list of _s tuples with 'key' specifying the field name and
             'type' specifying the expected data type of the value

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    # Check that the response JSON has a results array
    assert json is not None and json.get('results') is not None

    # Check that each results entry conforms to the specified schema
    for result in json['results']:
        check_schema(result, schema)


def check_schema(json, schema):
    """ Checks that the json object conforms to the schema.

    Parameters
    ----------
    json - json dict to check
    schema - defined schema to check against. schema is a list of _s tuples with 'key' specifying the field name and
             'type' specifying the expected data type of the value

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    for s in schema:
        assert s.key in json and isinstance(json[s.key], s.type)


def check_result_values(json, expected_results):
    """ Check that the json response contains the results in expected_results. The matching result object must be a
    superset of the expected result, i.e., it must have all of the fields and values defined in expected_values but may
    also have additional fields. Each expected result must match at least one result object.

    Parameters
    ----------
    json - json dict to check
    expected_results - list of results (dict) to check

    Returns
    -------
    No return value. Asserts will be triggered upon failure.
    """
    results = json.get('results')
    assert results is not None
    for er in expected_results:
        # Check that there is at least one result that is a superset of the expected result
        assert any(er.items() <= r.items() for r in results), 'item in expected_results not found in json:\n\njson:\n' \
                + pformat(results) + '\n\nexpected_results:\n' + pformat(er)


def test_datasets():
    """ Check the /metadata/datasets endpoint.
    Checks that the response json conforms to the expected schema.
    Checks that there are at least 3 data sets described.
    """
    print(f'test_cohd_covid_io: testing /metadata/datasets on {cr.server}..... ')
    json, df = cr.datasets()

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('dataset_name', str),
              _s('dataset_description', str)]
    check_results_schema(json, schema)

    # There should be at least three data sets described in the results
    assert len(json['results']) >= 3
    print('...passed')


def test_domain_counts():
    """ Check the /metadata/domainCounts endpoint for dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /metadata/domainCounts on {cr.server}..... ')
    json, df = cr.domain_counts(dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('domain_id', str),
              _s('count', int)]
    check_results_schema(json, schema)

    # There should be 3 results
    assert len(json['results']) == 3

    # Spot check a few of the entries against expected values: 10159 condition concepts and 8270 procedure concepts in
    # data set 1
    expected_results = [
        {
          "count": 290,
          "dataset_id": 1,
          "domain_id": "Condition"
        },
        {
          "count": 71,
          "dataset_id": 1,
          "domain_id": "Procedure"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_domain_pair_counts():
    """ Check the /metadata/domainPairCounts endpoint for dataset 2
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /metadata/domainPairCounts on {cr.server}..... ')
    json, df = cr.domain_pair_counts(dataset_id=2)

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('domain_id_1', str),
              _s('domain_id_2', str),
              _s('count', int)]
    check_results_schema(json, schema)

    # There should be 6 results
    assert len(json['results']) == 6

    # Spot check a few of the entries against expected values in data set 2
    expected_results = [
        {
            "count": 144303,
            "dataset_id": 2,
            "domain_id_1": "Condition",
            "domain_id_2": "Condition"
        },
        {
          "count": 182128,
          "dataset_id": 2,
          "domain_id_1": "Drug",
          "domain_id_2": "Procedure"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_visitCount():
    """ Check the /metadata/visitCount endpoint for dataset 2
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /metadata/visitCount on {cr.server}..... ')
    json, df = cr.visit_count(dataset_id=2)
    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('count', int)]
    check_results_schema(json, schema)

    # There should be a single result
    assert len(json['results']) == 1

    # Spot check a few of the entries against expected values: dataset 3 has 1731858 patients
    expected_results = [
        {
          "count": 314680,
          "dataset_id": 2
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_conceptAncestors():
    """ Check the /omop/conceptAncestors endpoint with concept_id=40184084, vocabulary_id=RxNorm,
    concept_class_id=Ingredient, and dataset_id=4
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/conceptAncestors on {cr.server}..... ')
    json, df = cr.concept_ancestors(concept_id=40184084, dataset_id=4, vocabulary_id='RxNorm',
                                 concept_class_id='Ingredient')

    # Check that the results adhere to the expected schema
    schema = [_s('ancestor_concept_id', int),
              _s('concept_class_id', str),
              _s('concept_code', str),
              _s('concept_count', int),
              _s('concept_name', str),
              _s('domain_id', str),
              _s('max_levels_of_separation', int),
              _s('min_levels_of_separation', int),
              _s('standard_concept', object),  # Nullable string field, can be None
              _s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be a single result
    assert len(json['results']) == 1

    # Spot check a few of the entries against expected values:
    expected_results = [
        {
            "ancestor_concept_id": 1777087,
            "concept_class_id": "Ingredient",
            "concept_code": "5521",
            "concept_count": 1689,
            "concept_name": "Hydroxychloroquine",
            "domain_id": "Drug",
            "max_levels_of_separation": 3,
            "min_levels_of_separation": 2,
            "standard_concept": "S",
            "vocabulary_id": "RxNorm"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_conceptDescendants():
    """ Check the /omop/conceptDescendants endpoint with concept_id=313217 and dataset_id=6
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/conceptDescendants on {cr.server}..... ')
    json, df = cr.concept_descendants(concept_id=313217, dataset_id=5)

    # Check that the results adhere to the expected schema
    schema = [_s('concept_class_id', str),
              _s('concept_code', str),
              _s('concept_count', int),
              _s('concept_name', str),
              _s('descendant_concept_id', int),
              _s('domain_id', str),
              _s('max_levels_of_separation', int),
              _s('min_levels_of_separation', int),
              _s('standard_concept', object),  # Nullable string field, can be None
              _s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be at least 1 result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values:
    expected_results = [
        {
            "concept_class_id": "Clinical Finding",
            "concept_code": "49436004",
            "concept_count": 34622,
            "concept_name": "Atrial fibrillation",
            "descendant_concept_id": 313217,
            "domain_id": "Condition",
            "max_levels_of_separation": 0,
            "min_levels_of_separation": 0,
            "standard_concept": "S",
            "vocabulary_id": "SNOMED"
        },
        {
            "concept_class_id": "Clinical Finding",
            "concept_code": "440028005",
            "concept_count": 12,
            "concept_name": "Permanent atrial fibrillation",
            "descendant_concept_id": 4232691,
            "domain_id": "Condition",
            "max_levels_of_separation": 1,
            "min_levels_of_separation": 1,
            "standard_concept": "S",
            "vocabulary_id": "SNOMED"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_concepts():
    """ Check the /omop/concepts endpoint with concept_id=192855,2008271
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/concepts on {cr.server}..... ')
    json, df = cr.concept([192855, 2008271])

    # Check that the results adhere to the expected schema
    schema = [_s('concept_class_id', str),
              _s('concept_code', str),
              _s('concept_id', int),
              _s('concept_name', str),
              _s('domain_id', str),
              _s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be two results
    assert len(json['results']) == 2

    # Check that the definitions for concepts 192855 (Cancer in situ of urinary bladder) and 2008271 (Injection or
    # infusion of cancer chemotherapeutic substance) are returned
    expected_results = [
        {
            "concept_class_id": "Clinical Finding",
            "concept_code": "92546004",
            "concept_id": 192855,
            "concept_name": "Cancer in situ of urinary bladder",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED"
        },
        {
            "concept_class_id": "4-dig billing code",
            "concept_code": "99.25",
            "concept_id": 2008271,
            "concept_name": "Injection or infusion of cancer chemotherapeutic substance",
            "domain_id": "Procedure",
            "vocabulary_id": "ICD9Proc"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_findConceptIDs():
    """ Check the /omop/findConceptIDs endpoint. Search for infection condition concepts in dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/findConceptIDs on {cr.server}.... ')
    json, df = cr.find_concept(concept_name='infection', dataset_id=1, domain='Condition', min_count=1)

    # Check that the results adhere to the expected schema
    schema = [_s('concept_class_id', str),
              _s('concept_code', str),
              _s('concept_count', int),
              _s('concept_id', int),
              _s('concept_name', str),
              _s('domain_id', str),
              _s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values:
    expected_results = [
        {
            "concept_class_id": "Clinical Finding",
            "concept_code": "195742007",
            "concept_count": 1153,
            "concept_id": 4307774,
            "concept_name": "Acute lower respiratory tract infection",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_mapFromStandardConceptID():
    """ Check the /omop/mapFromStandardConceptID endpoint. Get ICD9CM concepts that map to OMOP standard concept 72990
    (Localized osteoarthrosis uncertain if primary OR secondary)
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/mapFromStandardConceptID on {cr.server}..... ')
    json, df = cr.map_from_standard_concept_id(concept_id=72990, vocabulary_id='ICD9CM')

    # Check that the results adhere to the expected schema
    schema = [_s('concept_class_id', str),
              _s('concept_code', str),
              _s('concept_id', int),
              _s('concept_name', str),
              _s('domain_id', str),
              _s('standard_concept', object),  # Nullable string field, can be None
              _s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values: 72990 Localized osteoarthrosis uncertain if primary OR
    # secondary) maps to multiple ICD9CM codes, including 715.32 and 715.33
    expected_results = [
        {
            "concept_class_id": "5-dig billing code",
            "concept_code": "715.32",
            "concept_id": 44829196,
            "concept_name": "Osteoarthrosis, localized, not specified whether primary or secondary, upper arm",
            "domain_id": "Condition",
            "standard_concept": None,
            "vocabulary_id": "ICD9CM"
        },
        {
            "concept_class_id": "5-dig billing code",
            "concept_code": "715.33",
            "concept_id": 44828037,
            "concept_name": "Osteoarthrosis, localized, not specified whether primary or secondary, forearm",
            "domain_id": "Condition",
            "standard_concept": None,
            "vocabulary_id": "ICD9CM"
        },
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_mapToStandardConceptID():
    """ Check the /omop/mapToStandardConceptID endpoint. Try mapping from ICD9CM:715.3 (Osteoarthrosis, localized, not
    specified whether primary or secondary) to OMOP
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/mapToStandardConceptID on {cr.server}..... ')
    json, df = cr.map_to_standard_concept_id(concept_code='715.3', vocabulary_id='ICD9CM')

    # Check that the results adhere to the expected schema
    schema = [_s('source_concept_code', str),
              _s('source_concept_id', int),
              _s('source_concept_name', str),
              _s('source_vocabulary_id', str),
              _s('standard_concept_code', str),
              _s('standard_concept_id', int),
              _s('standard_concept_name', str),
              _s('standard_domain_id', str),
              _s('standard_vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be one result
    assert len(json['results']) == 1

    # ICD9CM:715.3 (Osteoarthrosis, localized, not specified whether primary or secondary) should map to OMOP:72990
    # (Localized osteoarthrosis uncertain if primary OR secondary)
    expected_results = [
        {
            "source_concept_code": "715.3",
            "source_concept_id": 44834979,
            "source_concept_name": "Osteoarthrosis, localized, not specified whether primary or secondary",
            "source_vocabulary_id": "ICD9CM",
            "standard_concept_code": "90860001",
            "standard_concept_id": 72990,
            "standard_concept_name": "Localized osteoarthrosis uncertain if primary OR secondary",
            "standard_domain_id": "Condition",
            "standard_vocabulary_id": "SNOMED"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_vocabularies():
    """ Check the /omop/vocabularies endpoint to retrieve the list of vocabularies used.
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/vocabularies on {cr.server}..... ')
    json, df = cr.vocabularies()

    # Check that the results adhere to the expected schema
    schema = [_s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be exactly 78 vocabularies
    assert len(json['results']) == 78

    # Spot check a few of the entries against expected vocabularies:
    expected_results = [
        {"vocabulary_id": "ATC"},
        {"vocabulary_id": "CPT4"},
        {"vocabulary_id": "HCPCS"},
        {"vocabulary_id": "ICD10CM"},
        {"vocabulary_id": "ICD10PCS"},
        {"vocabulary_id": "ICD9CM"},
        {"vocabulary_id": "ICD9Proc"},
        {"vocabulary_id": "LOINC"},
        {"vocabulary_id": "NDC"},
        {"vocabulary_id": "NDFRT"},
        {"vocabulary_id": "PCORNet"},
        {"vocabulary_id": "RxNorm"},
        {"vocabulary_id": "SNOMED"}
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_xrefFromOMOP():
    """ Check the /omop/xrefFromOMOP endpoint. Try mapping from OMOP:192855 (Cancer in situ of urinary bladder) to UMLS
    with max distance 2.
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/xrefFromOMOP on {cr.server}..... ')
    json, df = cr.xref_from_omop(concept_id=192855, mapping_targets=['UMLS'], distance=2, local=True, recommend=False)

    # Check that the results adhere to the expected schema
    schema = [_s('intermediate_omop_concept_code', str),
              _s('intermediate_omop_concept_id', object),  # Sometimes int, sometimes string when OMOP-UMLS mapping used
              _s('intermediate_omop_concept_name', str),
              _s('intermediate_omop_vocabulary_id', str),
              _s('intermediate_oxo_curie', str),
              _s('intermediate_oxo_label', str),
              _s('omop_distance', int),
              _s('oxo_distance', int),
              _s('source_omop_concept_code', str),
              _s('source_omop_concept_id', int),
              _s('source_omop_concept_name', str),
              _s('source_omop_vocabulary_id', str),
              _s('target_curie', str),
              _s('target_label', str),
              _s('total_distance', int)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values: OMOP:192855 (Cancer in situ of urinary bladder) should
    # map to UMLS:C0154091 (Carcinoma in situ of bladder) through a couple different paths
    expected_results = [
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
            "target_label": "Carcinoma in situ of bladder",
            "total_distance": 2
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_xrefToOMOP():
    """ Check the /omop/xrefToOMOP endpoint. Try to map DOID:8398 (osteoarthritis) to OMOP using the local OxO
    implementation, recommended mapping, and max distance 2
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /omop/xrefToOMOP on {cr.server}..... ')
    json, df = cr.xref_to_omop(curie='DOID:8398', distance=2, local=True, recommend=True)

    # Check that the results adhere to the expected schema
    schema = [_s('intermediate_oxo_id', str),
              _s('intermediate_oxo_label', str),
              _s('omop_concept_name', str),
              _s('omop_distance', int),
              _s('omop_domain_id', str),
              _s('omop_standard_concept_id', int),
              _s('oxo_distance', int),
              _s('source_oxo_id', str),
              _s('source_oxo_label', str),
              _s('total_distance', int)]
    check_results_schema(json, schema)

    # There should be one result
    assert len(json['results']) == 1

    # With recommend=True, there should be exactly one mapping from DOID:8398 (osteoarthritis) to OMOP:80180
    expected_results = [
        {
            "intermediate_oxo_id": "SNOMEDCT:396275006",
            "intermediate_oxo_label": "Osteoarthritis (disorder)",
            "omop_concept_name": "Osteoarthritis",
            "omop_distance": 0,
            "omop_domain_id": "Condition",
            "omop_standard_concept_id": 80180,
            "oxo_distance": 2,
            "source_oxo_id": "DOID:8398",
            "source_oxo_label": "osteoarthritis",
            "total_distance": 2
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_singleConceptFreq():
    """ Check the /frequencies/singleConceptFreq endpoint. Gets the single concept prevalence for OMOP:4110056 (Chronic
    obstructive pulmonary disease with acute lower respiratory infection) from dataset 2
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /frequencies/singleConceptFreq on {cr.server}..... ')
    json, df = cr.concept_frequency(concept_ids=[313217], dataset_id=2)

    # Check that the results adhere to the expected schema
    schema = [_s('concept_id', int),
              _s('dataset_id', int),
              _s('concept_count', int),
              _s('concept_frequency', float)]
    check_results_schema(json, schema)

    # There should be one result
    assert len(json['results']) == 1

    # Check against the known expected count
    expected_results = [
        {
            "concept_count": 26047,
            "concept_frequency": 0.08277297572136774,
            "concept_id": 313217,
            "dataset_id": 2
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_mostFrequentConcepts():
    """ Check the /frequencies/mostFrequentConcepts endpoints. Get the most frequent 50 procedures for dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /frequencies/mostFrequentConcepts on {cr.server}..... ')
    json, df = cr.most_frequent_concepts(limit=50, dataset_id=1, domain_id='Procedure')

    # Check that the results adhere to the expected schema
    schema = [_s('concept_class_id', str),
              _s('concept_count', int),
              _s('concept_frequency', float),
              _s('concept_id', int),
              _s('concept_name', str),
              _s('dataset_id', int),
              _s('domain_id', str),
              _s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be exactly 50 results
    assert len(json['results']) == 50

    # Spot check a few of the entries against expected values:
    expected_results = [
        {
            "concept_class_id": "CPT4",
            "concept_count": 3853,
            "concept_frequency": 0.79508873297565,
            "concept_id": 700360,
            "concept_name": "Infectious agent detection by nucleic acid (DNA or RNA); severe acute respiratory syndrome coronavirus 2 (SARS-CoV-2) (Coronavirus disease [COVID-19]), amplified probe technique",
            "dataset_id": 1,
            "domain_id": "Procedure",
            "vocabulary_id": "CPT4"
        },
        {
            "concept_class_id": "CPT4",
            "concept_count": 3365,
            "concept_frequency": 0.6943871234007429,
            "concept_id": 725068,
            "concept_name": "Radiologic examination, chest; single view",
            "dataset_id": 1,
            "domain_id": "Procedure",
            "vocabulary_id": "CPT4"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_pairedConceptFreq():
    """ Check the /frequencies/pairedConceptFreq endpoint. Get the co-occurrence counts for 40184084 (Hydroxychloroquin)
    and 437663 (Fever) from dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /frequencies/pairedConceptFreq on {cr.server}..... ')
    json, df = cr.paired_concepts_frequency(concept_id_1=40184084, concept_id_2=437663, dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('concept_id_1', int),
              _s('concept_id_2', int),
              _s('concept_count', int),
              _s('concept_frequency', float)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Check against the known expected count:
    expected_results = [
        {
            "concept_count": 717,
            "concept_frequency": 0.14795707800247626,
            "concept_id_1": 437663,
            "concept_id_2": 40184084,
            "dataset_id": 1
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_associatedConceptFreq():
    """ Check the /frequencies/associatedConceptFreq endpoint. Get concepts associated with 40184084 (Fever) from
    dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /frequencies/associatedConceptFreq on {cr.server}..... ')
    json, df = cr.associated_concepts_freq(concept_id=40184084, dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('associated_concept_id', int),
              _s('associated_concept_name', str),
              _s('associated_domain_id', str),
              _s('concept_count', int),
              _s('concept_frequency', float),
              _s('concept_id', int),
              _s('dataset_id', int)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values:
    expected_results = [
        {
            "associated_concept_id": 725068,
            "associated_concept_name": "Radiologic examination, chest; single view",
            "associated_domain_id": "Procedure",
            "concept_count": 1589,
            "concept_frequency": 0.3278992983904251,
            "concept_id": 40184084,
            "dataset_id": 1
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_associatedConceptDomainFreq():
    """ Check the /frequencies/associatedConceptDomainFreq endpoint. Get condition concepts associated with 40184084
    (Fever) from dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /frequencies/associatedConceptDomainFreq on {cr.server}..... ')
    json, df = cr.associated_concept_domain_freq(concept_id=40184084, domain_id='Condition', dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('associated_concept_id', int),
              _s('associated_concept_name', str),
              _s('associated_domain_id', str),
              _s('concept_count', int),
              _s('concept_frequency', float),
              _s('concept_id', int),
              _s('dataset_id', int)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values:
    expected_results = [
        {
            "associated_concept_id": 4100065,
            "associated_concept_name": "Disease due to Coronaviridae",
            "associated_domain_id": "Condition",
            "concept_count": 875,
            "concept_frequency": 0.1805612876599257,
            "concept_id": 40184084,
            "dataset_id": 1
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_chiSquare():
    """ Check the /association/chiSquare endpoint. Get chi-square results between 40241504 (Dexamethasone phosphate 4
    MG/ML Injectable Solution) and 46271075 (Acute hypoxemic respiratory failure) from dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /association/chiSquare on {cr.server}..... ')
    json, df = cr.chi_square(concept_id_1=40241504, concept_id_2=46271075, dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('chi_square', float),
              _s('concept_id_1', int),
              _s('concept_id_2', int),
              _s('dataset_id', int),
              _s('n', int),
              _s('n_c1', int),
              _s('n_c1_c2', int),
              _s('n_c1_~c2', int),
              _s('n_c2', int),
              _s('n_~c1_c2', int),
              _s('n_~c1_~c2', int),
              _s('p-value', float)]
    check_results_schema(json, schema)

    # There should be one result
    assert len(json['results']) == 1

    # Chi-square
    expected_results = [
        {
            "adj_p-value": 1,
            "chi_square": 1.1421935194425,
            "concept_id_1": 40241504,
            "concept_id_2": 46271075,
            "dataset_id": 1,
            "n": 4846,
            "n_c1": 164,
            "n_c1_c2": 20,
            "n_c1_~c2": 144,
            "n_c2": 473,
            "n_~c1_c2": 453,
            "n_~c1_~c2": 4229,
            "p-value": 0.28518930254495506
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_obsExpRatio():
    """ Check the /association/obsExpRatio endpoint. Get observed-expected frequency ratio between 40241504
    (Dexamethasone phosphate 4 MG/ML Injectable Solution) and 46271075 (Acute hypoxemic respiratory failure) from
    dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /association/obsExpRatio on {cr.server}..... ')
    json, df = cr.obs_exp_ratio(concept_id_1=40241504, concept_id_2=46271075, dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('concept_id_1', int),
              _s('concept_id_2', int),
              _s('dataset_id', int),
              _s('expected_count', float),
              _s('ln_ratio', float),
              _s('observed_count', int)]
    check_results_schema(json, schema)

    # There should be one result
    assert len(json['results']) == 1

    # Insulin and type-2 diabetes should be highly associated (high ln_ratio)
    expected_results = [
        {
            "concept_id_1": 40241504,
            "concept_id_2": 46271075,
            "confidence_interval": [
                -0.8271427658843471,
                0.8104660235164496
            ],
            "dataset_id": 1,
            "expected_count": 16.00742880726372,
            "ln_ratio": 0.2226793586143306,
            "observed_count": 20
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_relativeFrequency():
    """ Check the /association/relativeFrequency endpoint. Get relative frequency between 40241504 (Dexamethasone
    phosphate 4 MG/ML Injectable Solution) and conditions from dataset 3
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print(f'test_cohd_covid_io: testing /association/relativeFrequency on {cr.server}..... ')
    json, df = cr.relative_frequency(concept_id_1=40241504, domain_id='Condition', dataset_id=3)

    # Check that the results adhere to the expected schema
    schema = [_s('concept_2_count', int),
              _s('concept_2_domain', str),
              _s('concept_2_name', str),
              _s('concept_id_1', int),
              _s('concept_id_2', int),
              _s('concept_pair_count', int),
              _s('dataset_id', int),
              _s('relative_frequency', float)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Spot check a few of the entries against expected values: 4099217 (Type 2 diabetes mellitus with gangrene) and
    # 4227657 (Diabetic skin ulcer) should be among the results with high relative frequency
    expected_results = [
        {
            "concept_2_count": 267,
            "concept_2_domain": "Condition",
            "concept_2_name": "Hyperkalemia",
            "concept_id_1": 40241504,
            "concept_id_2": 434610,
            "concept_pair_count": 17,
            "confidence_interval": [
                0.015527950310559006,
                0.14883720930232558
            ],
            "dataset_id": 3,
            "relative_frequency": 0.06367041198501873
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_translator_query():
    """ Check the /translator/query endpoint. Primarily checks that the major objects adhere to the schema
    """
    print(f'test_cohd_covid_io: testing /translator/query on {cr.server}..... ')
    resp, query = translator_query(node_1_curies=['MONDO:0021113'], node_2_categories=['biolink:Disease'],
                                   method='obsExpRatio', dataset_id=4, confidence_interval=0.99,
                                   min_cooccurrence=50, threshold=0.5, max_results=10, local_oxo=False,
                                   timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be at least 1 result
    assert len(json['message']['results']) > 0

    print('...passed')
