"""
This test module tests the COHD API by making requests to cohd.io/api and checking the schema of the response JSONs and
checking the results against known values.

Intended to be run with pytest: pytest -s test_cohd_trapi.py
"""
from collections import namedtuple
import requests
import json as j
from bmt import Toolkit

from notebooks.cohd_helpers import cohd_requests as cr
from cohd.trapi import reasoner_validator_11x, reasoner_validator_10x

# Static instance of the Biolink Model Toolkit
bm_toolkit = Toolkit()

""" 
tuple for storing pairs of (key, type) for results schemas
"""
_s = namedtuple('_s', ['key', 'type'])

# Choose which server to test
cr.server = 'https://cohd.io/api'

# Proxy for main TRAPI version
reasoner_validator = reasoner_validator_11x
translator_query = cr.translator_query_110


def test_translator_query_100():
    """ Check the /translator/query endpoint. Primarily checks that the major objects adhere to the schema
    """
    print(f'test_cohd_io: testing /1.0.0/query on {cr.server}..... ')
    resp, query = cr.translator_query_100(node_1_curie='DOID:9053', node_2_type='procedure', method='obsExpRatio',
                                          dataset_id=3, confidence_interval=0.99, min_cooccurrence=50, threshold=0.5,
                                          max_results=10, local_oxo=True, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator_10x.validate_Response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10

    print('...passed')


def _test_translator_query_subclasses(q1_curie, q2_category, max_results=10):
    """ Check the TRAPI endpoint. Query q1_curies against q2_categories. Check that the responses are all subclasses of
    q2_categories.
    """
    print(f'test_cohd_io: testing TRAPI query between {q1_curie} and {q2_category}) on {cr.server}..... ')
    resp, query = translator_query(node_1_curies=q1_curie, node_2_categories=q2_category, method='obsExpRatio',
                                   dataset_id=3, confidence_interval=0.99, min_cooccurrence=50, threshold=0.5,
                                   max_results=max_results, local_oxo=True, timeout=300)

    # Expect HTTP 200 status response
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

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


def test_translator_query_11x_disease_phenotypic():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:DiseaseOrPhenotypicFeature
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:DiseaseOrPhenotypicFeature')


def test_translator_query_11x_drug():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:Drug
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:Drug')


def test_translator_query_11x_chemical():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:ChemicalSubstance
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:ChemicalSubstance')


def test_translator_query_11x_procedure():
    """ Check the TRAPI endpoint to make sure it returns results for biolink:Procedure
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:Procedure')


def test_translator_query_11x_molecular_entity():
    """ Check the TRAPI endpoint. biolink:MolecularEntity is the superclass of biolink:Drug and
    biolink:ChemicalSubstance. COHD should return types that are a subclass
    """
    _test_translator_query_subclasses(q1_curie='DOID:9053', q2_category='biolink:MolecularEntity')


def test_translator_query_unsupported_category():
    """ Check the TRAPI endpoint against an unsupported category (biolink:Gene). Expect COHD to return a TRAPI message
    with no results.
    """
    print(f'test_cohd_io: testing TRAPI query with an unsupported category on {cr.server}..... ')
    resp, query = translator_query(node_1_curies='DOID:9053', node_2_categories='biolink:Gene')

    # Should have 200 status response code
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

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
    print(f'test_cohd_io: testing TRAPI query with a non-biolink category on {cr.server}..... ')
    resp, query = translator_query(node_1_curies='DOID:9053', node_2_categories='biolink:Fake')

    # Should have 200 status response code
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code'

    print('...passed')


def test_translator_query_no_predicate():
    """ Check the TRAPI endpoint when not using a predicate. Expect results to be returned. """
    print(f'test_cohd_io: testing TRAPI query without a predicate on {cr.server}..... ')

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
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10

    print('...passed')


def test_translator_query_related_to():
    """ Check the TRAPI endpoint when using a generic predicate (biolink:related_to). Expect results to be returned. """
    print(f'test_cohd_io: testing TRAPI query with a generic predicate (biolink:related_to) on {cr.server}..... ')

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
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 10 results
    assert len(json['message']['results']) == 10

    print('...passed')


def test_translator_query_unsupported_predicate():
    """ Check the TRAPI endpoint when using an unsupported predicate (biolink:affects). Expect COHD to 400 status """
    print(f'test_cohd_io: testing TRAPI query with an unsupported predicate (biolink:affects) on {cr.server}..... ')

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
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code'

    print('...passed')


def test_translator_query_bad_predicate():
    """ Check the TRAPI endpoint when using an bad predicate (biolink:correlated). Expect COHD to return a 400 """
    print(f'test_cohd_io: testing TRAPI query a bad predicate (biolink:correlated) on {cr.server}..... ')

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
    assert resp.status_code == 400, 'Expected an HTTP 400 status response code'

    print('...passed')


def test_translator_query_q1_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the subject node. Expect COHD to return 3 results """
    print(f'test_cohd_io: testing TRAPI query with muldiple IDs in subject QNode on {cr.server}..... ')

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
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 3 results
    assert len(json['message']['results']) == 3

    # All three results should be from the original queried CURIE list
    ids = ["UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    for result in json['message']['results']:
        id = result['node_bindings']['subj'][0]['id']
        assert id in ids, f'Result subject {id} is not one of the original IDs {ids}'

    print('...passed')


def test_translator_query_q2_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the object node. Expect COHD to return 3 results """
    print(f'test_cohd_io: testing TRAPI query with multiple IDs in object QNode on {cr.server}..... ')

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
    assert resp.status_code == 200, 'Expected an HTTP 200 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 3 results
    assert len(json['message']['results']) == 3

    # All three results should be from the original queried CURIE list
    ids = ["UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    for result in json['message']['results']:
        id = result['node_bindings']['obj'][0]['id']
        assert id in ids, f'Result object {id} is not one of the original IDs {ids}'

    print('...passed')


def test_translator_query_q1_q2_multiple_ids():
    """ Check the TRAPI endpoint when using multiple IDs in the subject and object nodes. Expect COHD to return 12
    results """
    print(f'test_cohd_io: testing TRAPI query with multiple IDs in both query nodes on {cr.server}..... ')

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
    assert resp.status_code == 200, 'Expected an HTTP 400 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json = resp.json()
    reasoner_validator.validate_Response(json)

    # There should be 12 results
    assert len(json['message']['results']) == 12

    # All three results should be from the original queried CURIE list
    subj_ids = ["DOID:9053", "UMLS:C2939141", "HP:0002907", "MONDO:0001375"]
    obj_ids = ["CHEMBL.COMPOUND:CHEMBL1242", "PUBCHEM.COMPOUND:129211", "UNII:K9P6MC7092"]
    for result in json['message']['results']:
        subj_id = result['node_bindings']['subj'][0]['id']
        assert subj_id in subj_ids, f'Result subject {subj_id} is not one of the original IDs {subj_ids}'
        obj_id = result['node_bindings']['obj'][0]['id']
        assert obj_id in obj_ids, f'Result object {obj_id} is not one of the original IDs {obj_ids}'

    print('...passed')


def test_translator_query_multiple_categories():
    """ Check the TRAPI endpoint when using multiple categories in the object node. UMLS:C0451709 is "Toxic liver
    disease with acute hepatitis", which has low prevalence in COHD, hence will have few correlations (much less than
    500). Run multiple queries with individual categories to get counts, and then run it with multiple categories to
    make sure we're getting more results back. """
    print(f'test_cohd_io: testing TRAPI query with multiple categories for object node on {cr.server}..... ')

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
    assert resp_disease.status_code == 200, 'Expected an HTTP 200 status response code'

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
    assert resp_procedure.status_code == 200, 'Expected an HTTP 200 status response code'

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
    assert resp_combined.status_code == 200, 'Expected an HTTP 200 status response code'

    # Use the Reasoner Validator Python package to validate against Reasoner Standard API
    json_combined = resp_combined.json()
    reasoner_validator.validate_Response(json_combined)

    num_results_combined = len(json_combined['message']['results'])

    assert num_results_disease <= num_results_combined and num_results_procedure <= num_results_combined and \
        num_results_combined <= (num_results_disease + num_results_procedure), \
        'Number of results outside of expected range'

    print('...passed')
