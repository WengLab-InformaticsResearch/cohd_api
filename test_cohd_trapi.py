"""
This test module tests the COHD API by making requests to cohd.io/api and checking the schema of the response JSONs and
checking the results against known values.

Intended to be run with pytest: pytest -s test_cohd_trapi.py
"""
from collections import namedtuple
from itertools import product
import logging
import requests
import json as j
from bmt import Toolkit
import uuid
from datetime import datetime
import warnings

from notebooks.cohd_helpers import cohd_requests as cr
from cohd.trapi.reasoner_validator_ext import validate_trapi_13x as validate_trapi, validate_trapi_response
from cohd.translator.ontology_kp import OntologyKP

# Choose which server to test
# cr.server = 'https://cohd.io/api'
# cr.server = 'https://cohd-api.ci.transltr.io/api'
# cr.server = 'https://cohd-api.test.transltr.io/api'
cr.server = 'https://cohd-api.transltr.io/api'  # Default to ITRB-Production instance

# Specify what Biolink and TRAPI versions are expected by the server
BIOLINK_VERSION = '3.1.2'
TRAPI_VERSION = '1.3.0'

# Static instance of the Biolink Model Toolkit
bm_toolkit = Toolkit()

"""
tuple for storing pairs of (key, type) for results schemas
"""
_s = namedtuple('_s', ['key', 'type'])

# Proxy for main TRAPI version
translator_query = cr.translator_query_130

_logging_level_from_str = {logging.getLevelName(level): level for level in
                           [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]}


def _validate_trapi_response(response):
    vms = validate_trapi_response(TRAPI_VERSION, BIOLINK_VERSION, response)

    # expect no validation errors
    assert len(vms['errors']) == 0

    # If there are validation warnings, display them, but don't fail
    if len(vms['warnings']) > 0:
        warnings.warn(str(vms['warnings']))


def _print_trapi_log(trapi_response, print_level=logging.WARNING):
    """ Prints TRAPI log on or above log_level """
    logs = trapi_response.get('logs')
    if len(logs) == 0:
        return ''

    log_strs = ['\nTRAPI LOG:']
    for log in logs:
        log_level = log.get('level')
        if log_level in _logging_level_from_str and _logging_level_from_str[log_level] >= print_level:
            log_strs.append(str(log))
    return '\n'.join(log_strs)


def _test_translator_query_subclasses(q1_curie, q2_category, max_results=10):
    """ Check the TRAPI endpoint. Query q1_curies against q2_categories. Check that the responses are all subclasses of
    q2_categories.
    """
    print(f'\ntest_cohd_trapi: testing TRAPI query between {q1_curie} and {q2_category} on {cr.server}..... ')
    resp, query = translator_query(node_1_curies=q1_curie, node_2_categories=q2_category, method='obsExpRatio',
                                   dataset_id=3, confidence_interval=0.99, min_cooccurrence=50, threshold=0.5,
                                   max_results=max_results, local_oxo=True, timeout=300)
    print(query)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10, _print_trapi_log(json)

    # Check that all results are subclasses of qnode2
    descendants = bm_toolkit.get_descendants(q2_category, formatted=True, reflexive=True)
    kg_nodes = json['message']['knowledge_graph']['nodes']
    for result in json['message']['results']:
        obj_node_id = result['node_bindings']['n01'][0]['id']

        assert obj_node_id in kg_nodes, _print_trapi_log(json)
        obj_node = kg_nodes[obj_node_id]

        # Check that at least one of the categories is a descendant of the requested node category
        found = False
        for cat in obj_node['categories']:
            if cat in descendants:
                found = True
                break

        assert found, f"{obj_node['categories']} not a descendant of {q2_category}" + _print_trapi_log(json)


def _test_translator_query_predicates(q1_curie, q2_category, predicates, max_results=10):
    """ Check the TRAPI endpoint. Query q1_curies against q2_categories. Check that the responses are all subclasses of
    q2_categories.
    """
    print('\ntest_cohd_trapi: testing TRAPI query between '
          f'{q1_curie} and {q2_category} with {predicates} on {cr.server}..... ')
    resp, query = translator_query(node_1_curies=q1_curie, node_2_categories=q2_category,
                                   predicates=predicates, method='obsExpRatio')
    print(query)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be at least 1 result
    assert len(json['message']['results']) > 0, _print_trapi_log(json)

    # Check that all results have predicates that are descendants of the specified predicates
    descendants = list()
    for p in predicates:
        descendants.extend(bm_toolkit.get_descendants(p, formatted=True, reflexive=True))
    descendants = list(set(descendants))

    kg_edges = json['message']['knowledge_graph']['edges']
    for result in json['message']['results']:
        edge_id = result['edge_bindings']['e00'][0]['id']

        assert edge_id in kg_edges, _print_trapi_log(json)

        # Check that the predicate is a descendant of the requested predicates
        edge = kg_edges[edge_id]
        predicate = edge['predicate']
        assert predicate in descendants, f"{edge_id}: {predicate} not a descendant of {predicates}" + \
                                         _print_trapi_log(json)


def _test_ontology_kp():
    """ Check if Ontology KP is responding within a desired time """
    issue = False
    t1 = datetime.now()
    r = OntologyKP.get_descendants(curies=['MONDO:0005148'], timeout=None, bypass=True)
    s = (datetime.now() - t1).seconds

    if s > OntologyKP._TIMEOUT:
        warnings.warn(f'OntologyKP::getDescendants took {s} seconds to respond, which is longer than the default timeout ({OntologyKP._TIMEOUT} seconds).')
        issue = True

    if r is None:
        warnings.warn(f'OntologyKP::getDescendants had an error.')
        issue = True
    elif len(r[0]) < 2:
        warnings.warn(f'OntologyKP::getDescendants returned {len(r[0])} descendant nodes for T2DM.')
        issue = True

    return issue


_ontology_kp_issue = _test_ontology_kp()


def test_trapi_version():
    """ Simply check what version is deployed
    """
    print(f'\ntest_cohd_io: testing /translator/version on {cr.server}..... ')
    v = cr.translator_version()
    print(v)


def test_translator_query_named_thing():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:NamedThing
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:NamedThing')


def test_translator_query_disease_phenotypic():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:DiseaseOrPhenotypicFeature
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:DiseaseOrPhenotypicFeature')


def test_translator_query_disease():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:Disease
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:Disease')


def test_translator_query_phenotypic():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:DiseaseOrPhenotypicFeature
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:PhenotypicFeature')


def test_translator_query_drug():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:Drug
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:Drug')


def test_translator_query_molecular_entity():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:MolecularEntity
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:MolecularEntity')


def test_translator_query_small_molecule():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:SmallMolecule
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:SmallMolecule')


def test_translator_query_procedure():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:Procedure
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:Procedure')


def test_translator_query_positively_correlated_with():
    """  Check the TRAPI endpoint to make sure it only returns positive correlations when queried with
         biolink:positively_correlated_with
    """
    _test_translator_query_predicates(q1_curie='MONDO:0004981', q2_category='biolink:DiseaseOrPhenotypicFeature',
                                      predicates=['biolink:positively_correlated_with'])


def test_translator_query_negatively_correlated_with():
    """  Check the TRAPI endpoint to make sure it only returns positive correlations when queried with
         biolink:negatively_correlated_with
    """
    _test_translator_query_predicates(q1_curie='MONDO:0004981', q2_category='biolink:DiseaseOrPhenotypicFeature',
                                      predicates=['biolink:negatively_correlated_with'])


def test_translator_query_correlated_with():
    """  Check the TRAPI endpoint to make sure it only returns positive correlations when queried with
         biolink:correlated_with
    """
    _test_translator_query_predicates(q1_curie='MONDO:0004981', q2_category='biolink:DiseaseOrPhenotypicFeature',
                                      predicates=['biolink:correlated_with'])

def test_translator_query_unsupported_category():
    """ Check the TRAPI endpoint against an unsupported category (biolink:Gene). Expect COHD to return a TRAPI message
    with no results.
    """
    print(f'\ntest_cohd_trapi::test_translator_query_unsupported_category: testing TRAPI query with an unsupported '
          f'category on {cr.server}..... ')
    resp, query = translator_query(node_1_curies='DOID:9053', node_2_categories='biolink:Gene')
    print(query)

    # Should have 200 status response code
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be 0 results or null
    results = json['message']['results']
    assert results is None or len(results) == 0, 'Found results when expecting none' + _print_trapi_log(json)


def test_translator_query_bad_category():
    """ Check the TRAPI endpoint against a category that's not in biolink (biolink:Fake). Expect COHD to return a 400.
    """
    print(f'\ntest_cohd_trapi::test_translator_query_bad_category: testing TRAPI query with a non-biolink category on '
          f'{cr.server}..... ')
    resp, query = translator_query(node_1_curies='DOID:9053', node_2_categories='biolink:Fake')
    print(query)

    # Should have 200 status response code
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    # Don't validate TRAPI for this because biolink:Fake will invalidate the TRAPI
    # _validate_trapi_response(json)

    # There should be 0 results or null
    results = json['message']['results']
    print(results)
    assert results is None or len(results) == 0, 'Found results when expecting none' + _print_trapi_log(json)


def test_translator_query_no_predicate():
    """ Check the TRAPI endpoint when not using a predicate. Expect results to be returned. """
    print(f'\ntest_cohd_trapi::test_translator_query_no_predicate: testing TRAPI query without a predicate on '
          f'{cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"]
                    },
                    "obj": {
                        "ids": ["DOID:9053"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj"
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10, _print_trapi_log(json)


def test_translator_query_related_to():
    """ Check the TRAPI endpoint when using a generic predicate (biolink:related_to). Expect results to be returned. """
    print(f'\ntest_cohd_trapi::test_translator_query_related_to: testing TRAPI query with a generic predicate '
          f'(biolink:related_to) on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"]
                    },
                    "obj": {
                        "ids": ["DOID:9053"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:related_to"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10, _print_trapi_log(json)


def test_translator_query_unsupported_predicate():
    """ Check the TRAPI endpoint when using an unsupported predicate (biolink:affects). Expect COHD to 400 status """
    print(f'\ntest_cohd_trapi::test_translator_query_unsupported_predicate: testing TRAPI query with an unsupported '
          f'predicate (biolink:affects) on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"]
                    },
                    "obj": {
                        "ids": ["DOID:9053"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:affects"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 400 status response
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'


def test_translator_query_bad_predicate():
    """ Check the TRAPI endpoint when using an bad predicate (biolink:correlated). Expect COHD to return a 400 """
    print(f'\ntest_cohd_trapi::test_translator_query_bad_predicate: testing TRAPI query a bad predicate '
          f'(biolink:correlated) on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"]
                    },
                    "obj": {
                        "ids": ["DOID:9053"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'


def test_translator_query_q1_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the subject node. Expect COHD to return 3+ results """
    print(f'\ntest_cohd_trapi::test_translator_query_q1_multiple_ids: testing TRAPI query with multiple IDs in subject '
          f'QNode on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["UMLS:C0686169", "HP:0002907", "MONDO:0001375"]
                    },
                    "obj": {
                        "ids": ["DOID:9053"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be at least 3 results
    assert len(json['message']['results']) >= 3, f'Expected 3 or more results.\n{json}' + _print_trapi_log(json)

    # All three queried CURIEs should appear in the results
    ids = ["UMLS:C0686169", "HP:0002907", "MONDO:0001375"]
    result_object_ids = [r['node_bindings']['subj'][0]['id'] for r in json['message']['results']]
    for qid in ids:
        assert qid in result_object_ids, f'Result subject {qid} is not one of the original IDs {ids}' + \
                                         _print_trapi_log(json)


def test_translator_query_q2_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the object node. Expect COHD to return 3+ results """
    print(f'\ntest_cohd_trapi::test_translator_query_q2_multiple_ids: testing TRAPI query with multiple IDs in object '
          f'QNode on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["DOID:9053"]
                    },
                    "obj": {
                        "ids": ["UMLS:C0686169", "HP:0002907", "MONDO:0001375"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be at least 3 results
    assert len(json['message']['results']) >= 3, _print_trapi_log(json)

    # All three queried CURIEs should appear in the results
    ids = ["UMLS:C0686169", "HP:0002907", "MONDO:0001375"]
    result_object_ids = [r['node_bindings']['obj'][0]['id'] for r in json['message']['results']]
    for qid in ids:
        assert qid in result_object_ids, f'Result object {qid} is not one of the original IDs {ids}' + \
                                         _print_trapi_log(json)


# TODO: Temporarily replacing this test to not use CHEMBL.COMPOUND since Node Norm is currently missing mappings to MeSH
# def test_translator_query_q1_q2_multiple_ids():
#     """ Check the TRAPI endpoint when using multiple IDs in the subject and object nodes. Expect COHD to return 12+
#     results """
#     print(f'\ntest_cohd_trapi: testing TRAPI query with multiple IDs in both query nodes on {cr.server}..... ')
#
#     url = f'{cr.server}/query'
#     query = '''
#     {
#         "message": {
#             "query_graph": {
#                 "nodes": {
#                     "subj": {
#                         "ids": ["DOID:9053", "UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
#                     },
#                     "obj": {
#                         "ids": ["CHEMBL.COMPOUND:CHEMBL1242", "PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
#                     }
#                 },
#                 "edges": {
#                     "e0": {
#                         "subject": "subj",
#                         "object": "obj",
#                         "predicates": ["biolink:correlated_with"]
#                     }
#                 }
#             }
#         },
#         "query_options": {
#             "max_results": 50
#         }
#     }
#     '''
#     query = j.loads(query)
#     query['query_options']['query_id'] = str(uuid.uuid4())
#     print(query)
#     resp = requests.post(url, json=query, timeout=300)
#
#     # Expect HTTP 200 status response
#     assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
#                                     f'Received {resp.status_code}: {resp.text}'
#
#     # Use the Reasoner Validator Python package to validate against Reasoner Standard API
#     json = resp.json()
#     _validate_trapi_response(json)
#
#     # There should be at least 12 results
#     assert len(json['message']['results']) >= 12, _print_trapi_log(json)
#
#     # All pairs of the queried IDs should appear in at least one of the results
#     subj_ids = ["DOID:9053", "UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
#     obj_ids = ["CHEMBL.COMPOUND:CHEMBL1242", "PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
#     result_id_pairs = [(r['node_bindings']['subj'][0]['id'], r['node_bindings']['obj'][0]['id'])
#                        for r in json['message']['results']]
#     for pair in product(subj_ids, obj_ids):
#         assert pair in result_id_pairs, f'Query pair {pair} is not found in results pairs {result_id_pairs}.' + \
#                                         _print_trapi_log(json)
#
#     print('...passed')
def test_translator_query_q1_q2_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the subject and object nodes. Expect COHD to return 12+
    results """
    print(f'\ntest_cohd_trapi::test_translator_query_q1_q2_multiple_ids: testing TRAPI query with multiple IDs in both '
          f'query nodes on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["DOID:9053", "UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
                    },
                    "obj": {
                        "ids": ["PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 50
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be at least 12 results
    assert len(json['message']['results']) >= 8, _print_trapi_log(json)

    # All pairs of the queried IDs should appear in at least one of the results
    subj_ids = ["DOID:9053", "UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    obj_ids = ["PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
    result_id_pairs = [(r['node_bindings']['subj'][0]['id'], r['node_bindings']['obj'][0]['id'])
                       for r in json['message']['results']]
    for pair in product(subj_ids, obj_ids):
        assert pair in result_id_pairs, f'Query pair {pair} is not found in results pairs {result_id_pairs}.' + \
                                        _print_trapi_log(json)


def test_translator_query_multiple_categories():
    """ Check the TRAPI endpoint when using multiple categories in the object node. UMLS:C0451709 is "Toxic liver
    disease with acute hepatitis", which has low prevalence in COHD, hence will have few correlations (much less than
    500). Run multiple queries with individual categories to get counts, and then run it with multiple categories to
    make sure we're getting more results back. """
    print(f'\ntest_cohd_trapi::test_translator_query_multiple_categories: testing TRAPI query with multiple categories '
          f'for object node on {cr.server}..... ')

    url = f'{cr.server}/query'
    query_disease = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["UMLS:C0451709"]
                    },
                    "obj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 500
        }
    }
    '''
    query_disease = j.loads(query_disease)
    query_disease['query_options']['query_id'] = str(uuid.uuid4())
    print(query_disease)
    resp_disease = requests.post(url, json=query_disease, timeout=300)

    # Expect HTTP 200 status response
    assert resp_disease.status_code == 200, 'Expected an HTTP 200 status response code' \
                                            f'Received {resp_disease.status_code}: {resp_disease.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_disease = resp_disease.json()
    _validate_trapi_response(json_disease)

    num_results_disease = len(json_disease['message']['results'])

    query_procedure = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["UMLS:C0451709"]
                    },
                    "obj": {
                        "categories": ["biolink:Procedure"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 500
        }
    }
    '''
    query_procedure = j.loads(query_procedure)
    query_procedure['query_options']['query_id'] = str(uuid.uuid4())
    print(query_procedure)
    resp_procedure = requests.post(url, json=query_procedure, timeout=300)

    # Expect HTTP 200 status response
    assert resp_procedure.status_code == 200, 'Expected an HTTP 200 status response code' \
                                              f'Received {resp_procedure.status_code}: {resp_procedure.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_procedure = resp_procedure.json()
    _validate_trapi_response(json_procedure)

    num_results_procedure = len(json_procedure['message']['results'])

    query_combined = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["UMLS:C0451709"]
                    },
                    "obj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature", "biolink:Procedure"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 500
        }
    }
    '''
    query_combined = j.loads(query_combined)
    query_combined['query_options']['query_id'] = str(uuid.uuid4())
    print(query_combined)
    resp_combined = requests.post(url, json=query_combined, timeout=300)

    # Expect HTTP 200 status response
    assert resp_combined.status_code == 200, 'Expected an HTTP 200 status response code' \
                                             f'Received {resp_combined.status_code}: {resp_combined.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_combined = resp_combined.json()
    _validate_trapi_response(json_combined)

    num_results_combined = len(json_combined['message']['results'])

    assert num_results_disease <= num_results_combined and num_results_procedure <= num_results_combined and \
        num_results_combined <= (num_results_disease + num_results_procedure), \
        'Number of results outside of expected range' + _print_trapi_log(json_combined)


def test_translator_query_qnode_subclasses():
    """ Check the TRAPI endpoint to make sure we're also querying for ID subclasses. The TRAPI query will only specify
    a query between MONDO:0005015 (diabetes mellitus) and PUBCHEM.COMPOUND:3476 (glimepiride). Without subclassing,
    we would only expect 1 result. But with subclassing working, there should be more (check for at least 2).
    Note 7/19/2021: In previous versions of this test, used CHEMBL.COMPOUND:CHEMBL1481 for obj ID, but SRI Node Norm
    changed how it performed its mappings, and CHEMBL.COMPOUND:CHEMBL1481 no longer maps to MESH:C057619, which is what
    maps to OMOP standard concept. """
    print(f'\ntest_cohd_trapi::test_translator_query_qnode_subclasses: testing TRAPI query with multiple IDs in both '
          f'query nodes on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["MONDO:0005015"]
                    },
                    "obj": {
                        "ids": ["PUBCHEM.COMPOUND:3476"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 50
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be more than 1 result
    results = json['message']['results']
    if _ontology_kp_issue and len(results) < 2:
        # There was previously an issue observed with the OntologyKP, which may degrade results here.
        # Issue warning, but don't fail the test
        warnings.warn('test_translator_query_qnode_subclasses: Expected more than 1 result but only found '
                      f'{len(results)} results. However, OntologyKP may be having issues right now.')
        return

    assert len(results) > 1, _print_trapi_log(json)

    # We are expecting COHD to provide descendant results for the "subj" QNode (MONDO:0005015)
    # Check that query_id is specified in the node bindings
    original_query_id = 'MONDO:0005015'
    for result in results:
        subj_binding = result['node_bindings']['subj'][0]
        assert ((subj_binding['id'] == original_query_id) or
               (subj_binding['id'] != original_query_id and
                'query_id' in subj_binding and
                subj_binding['query_id'] == original_query_id))


def test_translator_query_qnode_empty_constraint():
    """ Check the TRAPI endpoint to make sure it allows null & empty constraints on QNodes. The null constraints should be
    ignored regardless of whether or not COHD implements constraints. """
    print(f'\ntest_cohd_trapi::test_translator_query_qnode_empty_constraint: testing TRAPI query with null constraints '
          f'on QNodes {cr.server}..... ')

    url = f'{cr.server}/query'

    # Query with empty constraints
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["DOID:9053"],
                        "constraints": []
                    },
                    "obj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                        "constraints": []
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "query_options": {
            "max_results": 10
        }
    }
    '''
    query = j.loads(query)
    query['query_options']['query_id'] = str(uuid.uuid4())
    print(query)
    resp = requests.post(url, json=query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)

    # There should be at least 1 result
    assert len(json['message']['results']) >= 1, _print_trapi_log(json)


def test_translator_workflows():
    """ Check the TRAPI endpoint to make sure COHD only responds when workflow is a single lookup operation. """
    print(f'\ntest_cohd_trapi::test_translator_workflows: testing TRAPI query with workflows on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["DOID:9053"]
                    },
                    "obj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"]
                    }
                },
                "edges": {
                    "e0": {
                        "subject": "subj",
                        "object": "obj",
                        "predicates": ["biolink:correlated_with"]
                    }
                }
            }
        },
        "workflow": [
            {"id": "lookup"}
        ],
        "query_options": {
            "max_results": 1
        }
    }
    '''
    j_query = j.loads(query)
    j_query['query_options']['query_id'] = str(uuid.uuid4())
    print(j_query)
    resp = requests.post(url, json=j_query, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'
    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    _validate_trapi_response(json)
    # There should be 1 result
    assert len(json['message']['results']) == 1, _print_trapi_log(json)

    # Test with bad workflows: unsupported operation (overlay)
    j_query['workflow'] = [{'id': 'overlay'}]
    resp = requests.post(url, json=j_query, timeout=300)
    # Expect HTTP 400 status response
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Test with bad workflows: multiple lookups
    j_query['workflow'] = [{'id': 'lookup'}, {'id': 'lookup'}]
    resp = requests.post(url, json=j_query, timeout=300)
    # Expect HTTP 400 status response
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'


def test_translator_meta_knowledge_graph():
    """ Check the /meta_knowledge_graph endpoint to make sure it returns a valid response. """
    print(f'\ntest_cohd_trapi::test_translator_meta_knowledge_graph: testing TRAPI /meta_knowledge_graph '
          f'on QNodes {cr.server}..... ')

    url = f'{cr.server}/meta_knowledge_graph'
    resp = requests.get(url, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    validate_trapi(json, "MetaKnowledgeGraph")


def test_biolink_to_omop():
    """ Check that the /translator/biolink_to_omop is functioning with good CURIEs """
    print(f'\ntest_cohd_trapi::test_biolink_to_omop: testing /translator/biolink_to_omop on {cr.server}..... ')

    curies = ['HP:0002907', 'MONDO:0001187']
    response = cr.translator_biolink_to_omop(curies)

    # Expect HTTP 200 status response
    assert response.status_code == 200, 'Expected a 200 status response code' \
                                        f'Received {response.status_code}: {response.text}'

    # Expect that each curie has a non-null mapping
    j = response.json()
    for curie in curies:
        assert j.get(curie) is not None, f'Did not find a mapping for curie {curie}'


def test_biolink_to_omop_bad():
    """ Check /translator/biolink_to_omop with bad CURIEs """
    print(f'\ntest_cohd_trapi::test_biolink_to_omop_bad: testing /translator/biolink_to_omop with bad CURIEs on '
          f'{cr.server}..... ')

    curies = ['HP:0002907BAD', 'MONDO:0001187BAD']
    response = cr.translator_biolink_to_omop(curies)

    # Expect HTTP 200 status response
    assert response.status_code == 200, 'Expected a 200 status response code'\
                                        f'Received {response.status_code}: {response.text}'

    # Expect that each curie has a null mapping
    j = response.json()
    for curie in curies:
        assert curie in j, f'Did not find curie {curie} in response'
        assert j[curie] is None, f'Found a non-null mapping for curie {curie}'


def test_omop_to_biolink():
    """ Check that the /translator/omop_to_biolink is functioning with good OMOP IDs """
    print(f'\ntest_cohd_trapi::test_omop_to_biolink: testing /translator/omop_to_biolink on {cr.server}..... ')

    omop_ids = ['78472', '197508']
    response = cr.translator_omop_to_biolink(omop_ids)

    # Expect HTTP 200 status response
    assert response.status_code == 200, 'Expected a 200 status response code.' \
                                        f'Received {response.status_code}: {response.text}'

    # Expect that each OMOP ID has a non-null mapping
    j = response.json()
    for omop_id in omop_ids:
        assert j.get(omop_id) is not None, f'Did not find a mapping for OMOP ID {omop_id}'


def test_omop_to_biolink_bad():
    """ Check /translator/omop_to_biolink with bad OMOP IDs """
    print(f'\ntest_cohd_trapi::test_omop_to_biolink_bad: testing /translator/omop_to_biolink with bad OMOP IDs on '
          f'{cr.server}..... ')

    omop_ids = ['78472197508']
    response = cr.translator_omop_to_biolink(omop_ids)

    # Expect HTTP 200 status response
    assert response.status_code == 200, 'Expected a 200 status response code.' \
                                        f'Received {response.status_code}: {response.text}'

    # Expect that each OMOP ID has a non-null mapping
    j = response.json()
    for omop_id in omop_ids:
        assert omop_id in j, f'Did not find OMOP ID {omop_id} in response'
        assert j[omop_id] is None, f'Found a non-null mapping for OMOP ID {omop_id}'
