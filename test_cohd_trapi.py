"""
This test module tests the COHD API by making requests to cohd.io/api and checking the schema of the response JSONs and
checking the results against known values.

Intended to be run with pytest: pytest -s test_cohd_trapi.py
"""
from collections import namedtuple
from itertools import product
import requests
import json as j
from bmt import Toolkit

from notebooks.cohd_helpers import cohd_requests as cr
from cohd.trapi import reasoner_validator_11x, reasoner_validator_10x

# Static instance of the Biolink Model Toolkit
bm_toolkit = Toolkit('https://raw.githubusercontent.com/biolink/biolink-model/1.8.2/biolink-model.yaml')

""" 
tuple for storing pairs of (key, type) for results schemas
"""
_s = namedtuple('_s', ['key', 'type'])

# Choose which server to test
cr.server = 'https://cohd.io/api'

# Proxy for main TRAPI version
reasoner_validator = reasoner_validator_11x
translator_query = cr.translator_query_110

# No longer supporting TRAPI 1.0. Leaving this code block here so that we can re-use it later on when transitioning
# between TRAPI 1.1 to 1.2
# def test_translator_query_100():
#     """ Check the /translator/query endpoint. Primarily checks that the major objects adhere to the schema
#     """
#     print(f'test_cohd_trapi: testing /1.0.0/query on {cr.server}..... ')
#     resp, query = cr.translator_query_100(node_1_curie='DOID:9053', node_2_type='procedure', method='obsExpRatio',
#                                           dataset_id=3, confidence_interval=0.99, min_cooccurrence=50, threshold=0.5,
#                                           max_results=10, local_oxo=True, timeout=300)
#
#     # Expect HTTP 200 status response
#     assert resp.status_code == 200, 'Expected an HTTP 200 status response code'
#
#     # Use the Reasoner Validator Python package to validate against Reasoner Standard API
#     json = resp.json()
#     reasoner_validator_10x.validate_Response(json)
#
#     # There should be 10 results
#     assert len(json['message']['results']) == 10
#
#     print('...passed')


def _test_translator_query_subclasses(q1_curie, q2_category, max_results=10):
    """ Check the TRAPI endpoint. Query q1_curies against q2_categories. Check that the responses are all subclasses of
    q2_categories.
    """
    print(f'\ntest_cohd_trapi: testing TRAPI query between {q1_curie} and {q2_category} on {cr.server}..... ')
    resp, query = translator_query(node_1_curies=q1_curie, node_2_categories=q2_category, method='obsExpRatio',
                                   dataset_id=3, confidence_interval=0.99, min_cooccurrence=50, threshold=0.5,
                                   max_results=max_results, local_oxo=True, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10

    # Check that all results are subclasses of qnode2
    descendants = bm_toolkit.get_descendants(q2_category, formatted=True, reflexive=True)
    kg_nodes = json['message']['knowledge_graph']['nodes']
    for result in json['message']['results']:
        obj_node_id = result['node_bindings']['n01'][0]['id']

        assert obj_node_id in kg_nodes
        obj_node = kg_nodes[obj_node_id]

        # Check that at least one of the categories is a descendant of the requested node category
        found = False
        for cat in obj_node['categories']:
            if cat in descendants:
                found = True
                break

        assert found, f"{obj_node['categories']} not a descendant of {q2_category}"

    print('...passed')


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


def test_translator_query_chemical():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:ChemicalSubstance
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:ChemicalSubstance')


def test_translator_query_procedure():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:Procedure
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:Procedure')


def test_translator_query_molecular_entity():
    """ Check the TRAPI endpoint. biolink:MolecularEntity is the superclass of biolink:Drug and
    biolink:ChemicalSubstance. COHD should return types that are a subclass
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:MolecularEntity')


def test_translator_query_unsupported_category():
    """ Check the TRAPI endpoint against an unsupported category (biolink:Gene). Expect COHD to return a TRAPI message
    with no results.
    """
    print(f'\ntest_cohd_trapi: testing TRAPI query with an unsupported category on {cr.server}..... ')
    resp, query = translator_query(node_1_curies='DOID:9053', node_2_categories='biolink:Gene')

    # Should have 200 status response code
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 0 results or null
    results = json['message']['results']
    assert results is None or len(results) == 0, 'Found results when expecting none'

    print('...passed')


def test_translator_query_bad_category():
    """ Check the TRAPI endpoint against a category that's not in biolink (biolink:Fake). Expect COHD to return a 400.
    """
    print(f'\ntest_cohd_trapi: testing TRAPI query with a non-biolink category on {cr.server}..... ')
    resp, query = translator_query(node_1_curies='DOID:9053', node_2_categories='biolink:Fake')

    # Should have 200 status response code
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    print('...passed')


def test_translator_query_no_predicate():
    """ Check the TRAPI endpoint when not using a predicate. Expect results to be returned. """
    print(f'\ntest_cohd_trapi: testing TRAPI query without a predicate on {cr.server}..... ')

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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10

    print('...passed')


def test_translator_query_related_to():
    """ Check the TRAPI endpoint when using a generic predicate (biolink:related_to). Expect results to be returned. """
    print(f'\ntest_cohd_trapi: testing TRAPI query with a generic predicate (biolink:related_to) on {cr.server}..... ')

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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10

    print('...passed')


def test_translator_query_unsupported_predicate():
    """ Check the TRAPI endpoint when using an unsupported predicate (biolink:affects). Expect COHD to 400 status """
    print(f'\ntest_cohd_trapi: testing TRAPI query with an unsupported predicate (biolink:affects) on {cr.server}..... ')

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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 400 status response
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    print('...passed')


def test_translator_query_bad_predicate():
    """ Check the TRAPI endpoint when using an bad predicate (biolink:correlated). Expect COHD to return a 400 """
    print(f'\ntest_cohd_trapi: testing TRAPI query a bad predicate (biolink:correlated) on {cr.server}..... ')

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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    print('...passed')


def test_translator_query_q1_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the subject node. Expect COHD to return 3+ results """
    print(f'\ntest_cohd_trapi: testing TRAPI query with multiple IDs in subject QNode on {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be at least 3 results
    assert len(json['message']['results']) >= 3

    # All three queried CURIEs should appear in the results
    ids = ["UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    result_object_ids = [r['node_bindings']['subj'][0]['id'] for r in json['message']['results']]
    for qid in ids:
        assert qid in result_object_ids, f'Result subject {qid} is not one of the original IDs {ids}'

    print('...passed')


def test_translator_query_q2_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the object node. Expect COHD to return 3+ results """
    print(f'\ntest_cohd_trapi: testing TRAPI query with multiple IDs in object QNode on {cr.server}..... ')

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
                        "ids": ["UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be at least 3 results
    assert len(json['message']['results']) >= 3

    # All three queried CURIEs should appear in the results
    ids = ["UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    result_object_ids = [r['node_bindings']['obj'][0]['id'] for r in json['message']['results']]
    for qid in ids:
        assert qid in result_object_ids, f'Result object {qid} is not one of the original IDs {ids}'

    print('...passed')


def test_translator_query_q1_q2_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the subject and object nodes. Expect COHD to return 12+
    results """
    print(f'\ntest_cohd_trapi: testing TRAPI query with multiple IDs in both query nodes on {cr.server}..... ')

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
                        "ids": ["CHEMBL.COMPOUND:CHEMBL1242", "PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be at least 12 results
    assert len(json['message']['results']) >= 12

    # All pairs of the queried IDs should appear in at least one of the results
    subj_ids = ["DOID:9053", "UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    obj_ids = ["CHEMBL.COMPOUND:CHEMBL1242", "PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
    result_id_pairs = [(r['node_bindings']['subj'][0]['id'], r['node_bindings']['obj'][0]['id'])
                       for r in json['message']['results']]
    for pair in product(subj_ids, obj_ids):
        assert pair in result_id_pairs, f'Query pair {pair} is not found in results pairs {result_id_pairs}.'

    print('...passed')


def test_translator_query_multiple_categories():
    """ Check the TRAPI endpoint when using multiple categories in the object node. UMLS:C0451709 is "Toxic liver
    disease with acute hepatitis", which has low prevalence in COHD, hence will have few correlations (much less than
    500). Run multiple queries with individual categories to get counts, and then run it with multiple categories to
    make sure we're getting more results back. """
    print(f'\ntest_cohd_trapi: testing TRAPI query with multiple categories for object node on {cr.server}..... ')

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
    resp_disease = requests.post(url, json=j.loads(query_disease), timeout=300)

    # Expect HTTP 200 status response
    assert resp_disease.status_code == 200, 'Expected an HTTP 200 status response code' \
                                            f'Received {resp_disease.status_code}: {resp_disease.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_disease = resp_disease.json()
    reasoner_validator.validate_Response(json_disease)

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
    resp_procedure = requests.post(url, json=j.loads(query_procedure), timeout=300)

    # Expect HTTP 200 status response
    assert resp_procedure.status_code == 200, 'Expected an HTTP 200 status response code' \
                                              f'Received {resp_procedure.status_code}: {resp_procedure.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_procedure = resp_procedure.json()
    reasoner_validator.validate_Response(json_procedure)

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
    resp_combined = requests.post(url, json=j.loads(query_combined), timeout=300)

    # Expect HTTP 200 status response
    assert resp_combined.status_code == 200, 'Expected an HTTP 200 status response code' \
                                             f'Received {resp_combined.status_code}: {resp_combined.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_combined = resp_combined.json()
    reasoner_validator.validate_Response(json_combined)

    num_results_combined = len(json_combined['message']['results'])

    assert num_results_disease <= num_results_combined and num_results_procedure <= num_results_combined and \
        num_results_combined <= (num_results_disease + num_results_procedure), \
        'Number of results outside of expected range'

    print('...passed')


def test_biolink_to_omop():
    """ Check that the /translator/biolink_to_omop is functioning with good CURIEs """
    print(f'\ntest_cohd_trapi: testing /translator/biolink_to_omop on {cr.server}..... ')

    curies = ['HP:0002907', 'MONDO:0001187']
    response = cr.translator_biolink_to_omop(curies)

    # Expect HTTP 200 status response
    assert response.status_code == 200, 'Expected a 200 status response code' \
                                        f'Received {response.status_code}: {response.text}'

    # Expect that each curie has a non-null mapping
    j = response.json()
    for curie in curies:
        assert j.get(curie) is not None, f'Did not find a mapping for curie {curie}'

    print('...passed')
    

def test_biolink_to_omop_bad():
    """ Check /translator/biolink_to_omop with bad CURIEs """
    print(f'\ntest_cohd_trapi: testing /translator/biolink_to_omop with bad CURIEs on {cr.server}..... ')

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

    print('...passed')


def test_omop_to_biolink():
    """ Check that the /translator/omop_to_biolink is functioning with good OMOP IDs """
    print(f'\ntest_cohd_trapi: testing /translator/omop_to_biolink on {cr.server}..... ')

    omop_ids = ['78472', '197508']
    response = cr.translator_omop_to_biolink(omop_ids)

    # Expect HTTP 200 status response
    assert response.status_code == 200, 'Expected a 200 status response code.' \
                                        f'Received {response.status_code}: {response.text}'

    # Expect that each OMOP ID has a non-null mapping
    j = response.json()
    for omop_id in omop_ids:
        assert j.get(omop_id) is not None, f'Did not find a mapping for OMOP ID {omop_id}'

    print('...passed')


def test_omop_to_biolink_bad():
    """ Check /translator/omop_to_biolink with bad OMOP IDs """
    print(f'\ntest_cohd_trapi: testing /translator/omop_to_biolink with bad OMOP IDs on {cr.server}..... ')

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

    print('...passed')


def test_translator_query_qnode_subclasses():
    """ Check the TRAPI endpoint to make sure we're also querying for ID subclasses. The TRAPI query will only specify
    a query between MONDO:0005015 (diabetes mellitus) and CHEMBL.COMPOUND:CHEMBL1481 (glimepiride). Without subclassing,
    we would only expect 1 result. But with subclassing working, there should be more (check for at least 2). """
    print(f'\ntest_cohd_trapi: testing TRAPI query with multiple IDs in both query nodes on {cr.server}..... ')

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
                        "ids": ["CHEMBL.COMPOUND:CHEMBL1481"]
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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be more than 1 result
    assert len(json['message']['results']) > 1

    print('...passed')


def test_translator_query_qnode_null_constraint():
    """ Check the TRAPI endpoint to make sure it allows null constraints on QNodes. The null constraints should be
    ignored regardless of whether or not COHD implements constraints. """
    print(f'\ntest_cohd_trapi: testing TRAPI query with null constraints on QNodes {cr.server}..... ')

    url = f'{cr.server}/query'
    query = '''
    {
        "message": {
            "query_graph": {
                "nodes": {
                    "subj": {
                        "ids": ["DOID:9053"],
                        "constraints": null
                    },
                    "obj": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                        "constraints": null
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
    resp = requests.post(url, json=j.loads(query), timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code' \
                                    f'Received {resp.status_code}: {resp.text}'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be at least 1 result
    assert len(json['message']['results']) >= 1

    print('...passed')
