from notebooks.cohd_requests import *
from collections import namedtuple

# tuple for storing pairs of (key, type) for results schemas
_s = namedtuple('_s', ['key', 'type'])


def check_results_schema(json, schema):
    # Check that the response JSON has a results array
    assert json is not None and 'results' in json and json['results'] is not None

    # Check that each results entry has the following keys: 'dataset_id', 'dataset_description', 'dataset_name'
    for result in json['results']:
        for s in schema:
            assert s.key in result and isinstance(result[s.key], s.type)


def check_result_values(json, expected_values):
    results = json['results']
    for ev in expected_values:
        assert any(ev.items() <= r.items() for r in results)


def test_datasets():
    """ Check the /metadata/datasets endpoint.
    """
    json, df = datasets()
    # Check that the results adhere to the expected schema
    schema = [_s('dataset_id', int),
              _s('dataset_name', str),
              _s('dataset_description', str)]
    check_results_schema(json, schema)
    # There should be at least three data sets described in the results
    assert len(json['results']) >= 3


def test_domain_counts():
    """ Check the /metadata/domainCounts endpoint for dataset 1
    """
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


def test_domain_pair_counts():
    """ Check the /metadata/domainPairCounts endpoint for dataset 2
    """
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



