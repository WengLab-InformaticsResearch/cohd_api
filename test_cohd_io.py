"""
This test module tests the COHD API by making requests to cohd.io/api and checking the schema of the response JSONs and
checking the results against known values.
"""
from notebooks.cohd_requests import *
from collections import namedtuple
import requests

""" 
tuple for storing pairs of (key, type) for results schemas
"""
_s = namedtuple('_s', ['key', 'type'])


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
        assert any(er.items() <= r.items() for r in results)


def test_datasets():
    """ Check the /metadata/datasets endpoint.
    Checks that the response json conforms to the expected schema.
    Checks that there are at least 3 data sets described.
    """
    print('test_cohd_io: testing /metadata/datasets..... ')
    json, df = datasets()

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
    print('test_cohd_io: testing /metadata/domainCounts..... ')
    json, df = domain_counts(dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('domain_id', str),
              _s('count', int)]
    check_results_schema(json, schema)

    # There should be 10 results
    assert len(json['results']) == 10

    # Spot check a few of the entries against expected values: 10159 condition concepts and 8270 procedure concepts in
    # data set 1
    expected_results = [
        {
          "count": 10159,
          "dataset_id": 1,
          "domain_id": "Condition"
        },
        {
          "count": 8270,
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
    print('test_cohd_io: testing /metadata/domainPairCounts..... ')
    json, df = domain_pair_counts(dataset_id=2)

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('domain_id_1', str),
              _s('domain_id_2', str),
              _s('count', int)]
    check_results_schema(json, schema)

    # There should be 50 results
    assert len(json['results']) == 50

    # Spot check a few of the entries against expected values: 5223373 (drug, procedure) concept pairs, and 26680
    # (drug, race) pairs in data set 2
    expected_results = [
        {
          "count": 5223373,
          "dataset_id": 2,
          "domain_id_1": "Drug",
          "domain_id_2": "Procedure"
        },
        {
          "count": 26680,
          "dataset_id": 2,
          "domain_id_1": "Drug",
          "domain_id_2": "Race"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_patientCount():
    """ Check the /metadata/patientCount endpoint for dataset 2
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /metadata/patientCount..... ')
    json, df = patient_count(dataset_id=3)
    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('count', int)]
    check_results_schema(json, schema)

    # There should be a single result
    assert len(json['results']) == 1

    # Spot check a few of the entries against expected values: dataset 3 has 1731858 patients
    expected_results = [
        {
          "count": 1731858,
          "dataset_id": 3
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_conceptAncestors():
    """ Check the /omop/conceptAncestors endpoint with concept_id=19019073, vocabulary_id=RxNorm,
    concept_class_id=Ingredient, and dataset_id=3
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /omop/conceptAncestors..... ')
    json, df = concept_ancestors(concept_id=19019073, dataset_id=3, vocabulary_id='RxNorm',
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

    # Spot check a few of the entries against expected values: 19019073 ("Ibuprofen 600 MG Oral Tablet") has one
    # ingredient ancestor (Ibuprofen) with count 233514 in dataset 3
    expected_results = [
        {
          "ancestor_concept_id": 1177480,
          "concept_class_id": "Ingredient",
          "concept_code": "5640",
          "concept_count": 233514,
          "concept_name": "Ibuprofen",
          "domain_id": "Drug",
          "max_levels_of_separation": 2,
          "min_levels_of_separation": 2,
          "standard_concept": "S",
          "vocabulary_id": "RxNorm"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_conceptDescendants():
    """ Check the /omop/conceptDescendants endpoint with concept_id=313217 and dataset_id=3
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /omop/conceptDescendants..... ')
    json, df = concept_descendants(concept_id=313217, dataset_id=3)

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

    # Spot check a few of the entries against expected values: 313217 ("Atrial fibrillation") has multiple descendants,
    # including itself and 'Atrial fibrillation and flutter' with count 115 in dataset 3
    expected_results = [
        {
          "concept_class_id": "Clinical Finding",
          "concept_code": "49436004",
          "concept_count": 53272,
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
          "concept_code": "195080001",
          "concept_count": 115,
          "concept_name": "Atrial fibrillation and flutter",
          "descendant_concept_id": 4108832,
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
    print('test_cohd_io: testing /omop/concepts..... ')
    json, df = concept([192855, 2008271])

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
    """ Check the /omop/findConceptIDs endpoint. Search for cancer condition concepts in dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /omop/findConceptIDs.... ')
    json, df = find_concept(concept_name='cancer', dataset_id=1, domain='Condition', min_count=1)

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

    # Spot check a few of the entries against expected values: the following concepts should be found - 4331508
    # (Increased cancer antigen 125) and 44782478 (Hereditary cancer-predisposing syndrome)
    expected_results = [
        {
            "concept_class_id": "Clinical Finding",
            "concept_code": "432519008",
            "concept_count": 247,
            "concept_id": 4331508,
            "concept_name": "Increased cancer antigen 125",
            "domain_id": "Condition",
            "vocabulary_id": "SNOMED"
        },
        {
            "concept_class_id": "Clinical Finding",
            "concept_code": "699346009",
            "concept_count": 29,
            "concept_id": 44782478,
            "concept_name": "Hereditary cancer-predisposing syndrome",
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
    print('test_cohd_io: testing /omop/mapFromStandardConceptID..... ')
    json, df = map_from_standard_concept_id(concept_id=72990, vocabulary_id='ICD9CM')

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
    print('test_cohd_io: testing /omop/mapToStandardConceptID..... ')
    json, df = map_to_standard_concept_id(concept_code='715.3', vocabulary_id='ICD9CM')

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
    print('test_cohd_io: testing /omop/vocabularies..... ')
    json, df = vocabularies()

    # Check that the results adhere to the expected schema
    schema = [_s('vocabulary_id', str)]
    check_results_schema(json, schema)

    # There should be exactly 73 vocabularies
    assert len(json['results']) == 73

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
        { "vocabulary_id": "MedDRA"},
        {"vocabulary_id": "MeSH"},
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
    print('test_cohd_io: testing /omop/xrefFromOMOP..... ')
    json, df = xref_from_omop(concept_id=192855, mapping_targets=['UMLS'], distance=2, local=True, recommend=False)

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
    print('test_cohd_io: testing /omop/xrefToOMOP..... ')
    json, df = xref_to_omop(curie='DOID:8398', distance=2, local=True, recommend=True)

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
    print('test_cohd_io: testing /frequencies/singleConceptFreq..... ')
    json, df = concept_frequency(concept_ids=[4110056], dataset_id=2)

    # Check that the results adhere to the expected schema
    schema = [_s('concept_id', int),
              _s('dataset_id', int),
              _s('concept_count', int),
              _s('concept_frequency', float)]
    check_results_schema(json, schema)

    # There should be one result
    assert len(json['results']) == 1

    # Check against the known expected count for OMOP:4110056 (Chronic obstructive pulmonary disease with acute lower
    # respiratory infection)
    expected_results = [
        {
            "concept_count": 804,
            "concept_frequency": 0.00014986632259546102,
            "concept_id": 4110056,
            "dataset_id": 2
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_mostFrequentConcepts():
    """ Check the /frequencies/mostFrequentConcepts endpoints. Get the most frequent 50 procedures for dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /frequencies/mostFrequentConcepts..... ')
    json, df = most_frequent_concepts(limit=50, dataset_id=1, domain_id='Procedure')

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

    # Spot check a few of the entries against expected values: results should include 2213283 (pathology examination)
    # and 2211359 (radiology exam) to be included
    expected_results = [
        {
            "concept_class_id": "CPT4",
            "concept_count": 431281,
            "concept_frequency": 0.2408811062811133,
            "concept_id": 2213283,
            "concept_name": "Level IV - Surgical pathology, gross and microscopic examination Abortion - spontaneous/missed Artery, biopsy Bone marrow, biopsy Bone exostosis Brain/meninges, other than for tumor resection Breast, biopsy, not requiring microscopic evaluation of surgica",
            "dataset_id": 1,
            "domain_id": "Procedure",
            "vocabulary_id": "CPT4"
        },
        {
            "concept_class_id": "CPT4",
            "concept_count": 105725,
            "concept_frequency": 0.05905002761904815,
            "concept_id": 2211359,
            "concept_name": "Radiologic examination, chest; single view, frontal",
            "dataset_id": 1,
            "domain_id": "Procedure",
            "vocabulary_id": "CPT4"
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_pairedConceptFreq():
    """ Check the /frequencies/pairedConceptFreq endpoint. Get the co-occurrence counts for 192855 (Cancer in situ of
    urinary bladder) and 2008271 (Injection or infusion of cancer chemotherapeutic substance) from dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /frequencies/pairedConceptFreq..... ')
    json, df = paired_concepts_frequency(concept_id_1=192855, concept_id_2=2008271, dataset_id=1)

    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('concept_id_1', int),
              _s('concept_id_2', int),
              _s('concept_count', int),
              _s('concept_frequency', float)]
    check_results_schema(json, schema)

    # There should be at least one result
    assert len(json['results']) >= 1

    # Check against the known expected count for 192855 (Cancer in situ of urinary bladder) and 2008271 (Injection or
    # infusion of cancer chemotherapeutic substance) from dataset 1
    expected_results = [
        {
            "concept_count": 10,
            "concept_frequency": 0.000005585247351056813,
            "concept_id_1": 192855,
            "concept_id_2": 2008271,
            "dataset_id": 1
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_associatedConceptFreq():
    """ Check the /frequencies/associatedConceptFreq endpoint. Get concepts associated with 19133905 (Penicillin V
    Potassium 500 MG Oral Tablet) from dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /frequencies/associatedConceptFreq..... ')
    json, df = associated_concepts_freq(concept_id=19133905, dataset_id=1)

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

    # Spot check a few of the entries against expected values: 8532 (female) and 44814653 (unknown race) should be
    # among the top results since they're highly prevalent concepts
    expected_results = [
        {
            "associated_concept_id": 8532,
            "associated_concept_name": "FEMALE",
            "associated_domain_id": "Gender",
            "concept_count": 5047,
            "concept_frequency": 0.0028188743380783733,
            "concept_id": 19133905,
            "dataset_id": 1
        },
        {
            "associated_concept_id": 44814653,
            "associated_concept_name": "Unknown",
            "associated_domain_id": "Observation",
            "concept_count": 4742,
            "concept_frequency": 0.0026485242938711407,
            "concept_id": 19133905,
            "dataset_id": 1
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_associatedConceptDomainFreq():
    """ Check the /frequencies/associatedConceptDomainFreq endpoint. Get condition concepts associated with 19133905
    (Penicillin V Potassium 500 MG Oral Tablet) from dataset 1
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /frequencies/associatedConceptDomainFreq..... ')
    json, df = associated_concept_domain_freq(concept_id=19133905, domain_id='Condition', dataset_id=1)

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

    # Spot check a few of the entries against expected values: 320128 (Essential hypertension) and 25297 (Acute
    # pharyngitis)
    expected_results = [
        {
            "associated_concept_id": 320128,
            "associated_concept_name": "Essential hypertension",
            "associated_domain_id": "Condition",
            "concept_count": 2282,
            "concept_frequency": 0.0012745534455111647,
            "concept_id": 19133905,
            "dataset_id": 1
        },
        {
            "associated_concept_id": 25297,
            "associated_concept_name": "Acute pharyngitis",
            "associated_domain_id": "Condition",
            "concept_count": 1962,
            "concept_frequency": 0.0010958255302773467,
            "concept_id": 19133905,
            "dataset_id": 1
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_chiSquare():
    """ Check the /association/chiSquare endpoint. Get chi-square results between 19078558 (Insulin, Aspart, Human 100
    UNT/ML Injectable Solution) and 4193704 (Type 2 diabetes mellitus without complication) from dataset 3
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /association/chiSquare..... ')
    json, df = chi_square(concept_id_1=19078558, concept_id_2=4193704, dataset_id=3)

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

    # Chi-square between insulin and type-2 diabetes should be significant
    expected_results = [
        {
            "chi_square": 235425.62341513575,
            "concept_id_1": 4193704,
            "concept_id_2": 19078558,
            "dataset_id": 3,
            "n": 1731858,
            "n_c1": 65629,
            "n_c1_c2": 17725,
            "n_c1_~c2": 47904,
            "n_c2": 32322,
            "n_~c1_c2": 14597,
            "n_~c1_~c2": 1651632,
            "p-value": 0
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_obsExpRatio():
    """ Check the /association/obsExpRatio endpoint. Get observed-expected frequency ratio between between 19078558
    (Insulin, Aspart, Human 100 UNT/ML Injectable Solution) and 4193704 (Type 2 diabetes mellitus without complication)
    from dataset 3
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /association/obsExpRatio..... ')
    json, df = obs_exp_ratio(concept_id_1=19078558, concept_id_2=4193704, dataset_id=3)

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
            "concept_id_1": 4193704,
            "concept_id_2": 19078558,
            "dataset_id": 3,
            "expected_count": 1224.846689509186,
            "ln_ratio": 2.672160387674666,
            "observed_count": 17725
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_relativeFrequency():
    """ Check the /association/relativeFrequency endpoint. Get conditions where 19078558 (insulin) has high relative frequency in dataset 3
    Checks the response json conforms to the expected schema and includes the expected results (see expected_results).
    """
    print('test_cohd_io: testing /association/relativeFrequency..... ')
    json, df = relative_frequency(concept_id_1=19078558, domain_id='Condition', dataset_id=3)

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
            "concept_2_count": 261,
            "concept_2_domain": "Condition",
            "concept_2_name": "Type 2 diabetes mellitus with gangrene",
            "concept_id_1": 19078558,
            "concept_id_2": 4099217,
            "concept_pair_count": 246,
            "dataset_id": 3,
            "relative_frequency": 0.9425287356321839
        },
        {
            "concept_2_count": 57,
            "concept_2_domain": "Condition",
            "concept_2_name": "Diabetic skin ulcer",
            "concept_id_1": 19078558,
            "concept_id_2": 4227657,
            "concept_pair_count": 52,
            "dataset_id": 3,
            "relative_frequency": 0.9122807017543859
        }
    ]
    check_result_values(json, expected_results)
    print('...passed')


def test_translator_query():
    """ Check the /translator/query endpoint. Primarily checks that the major objects adhere to the schema
    """
    print('test_cohd_io: testing /translator/query..... ')
    json = translator_query(node_1_curie='DOID:9053', node_2_type='procedure', method='obsExpRatio', dataset_id=3,
                            confidence_interval=0.99, min_cooccurrence=50, threshold=0.5, max_results=10)

    # Check that the JSON response adheres to the 'message' schema by using the Translator ReasonerStdAPI Validator
    url_validate_message = u'http://transltr.io:7071/validate_message'
    validation_response = requests.post(url_validate_message, json=json)
    # If the response is properly formatted, we should have received a 200 (OK) status code and "Successfully validated"
    # in the response body
    assert validation_response.status_code == requests.status_codes.codes.OK and \
        validation_response.text.strip().lower() == '"successfully validated"'

    # There should be 10 results
    assert len(json['results']) == 10

    print('...passed')

