"""
Implementation of the NCATS Biodmedical Data Translator TRAPI Spec
https://github.com/NCATS-Tangerine/NCATS-ReasonerStdAPI/tree/master/API
"""

from flask import jsonify
from semantic_version import Version

from . import cohd_trapi_093
from . import cohd_trapi_100


def translator_predicates():
    """ Implementation of /translator/predicates for Translator Reasoner API

    Returns
    -------
    json response object
    """
    return jsonify({
        'biolink:Disease': {
            'biolink:Disease': ['biolink:correlated_with'],
            'biolink:Drug': ['biolink:correlated_with'],
            'biolink:Procedure': ['biolink:correlated_with'],
            'biolink:PopulationOfIndividualOrganisms': ['biolink:correlated_with']
        },
        'biolink:Drug': {
            'biolink:Disease': ['biolink:correlated_with'],
            'biolink:Drug': ['biolink:correlated_with'],
            'biolink:Procedure': ['biolink:correlated_with'],
            'biolink:PopulationOfIndividualOrganisms': ['biolink:correlated_with']
        },
        'biolink:Procedure': {
            'biolink:Disease': ['biolink:correlated_with'],
            'biolink:Drug': ['biolink:correlated_with'],
            'biolink:Procedure': ['biolink:correlated_with'],
            'biolink:PopulationOfIndividualOrganisms': ['biolink:correlated_with']
        },
        'biolink:PopulationOfIndividualOrganisms': {
            'biolink:Disease': ['biolink:correlated_with'],
            'biolink:Drug': ['biolink:correlated_with'],
            'biolink:Procedure': ['biolink:correlated_with'],
            'biolink:PopulationOfIndividualOrganisms': ['biolink:correlated_with']
        },
    })


def translator_query(request, version=None):
    """ Implementation of query endpoint for TRAPI

    Calls the requested version of the TRAPI message

    Parameters
    ----------
    request - flask request object
    version - string: TRAPI version

    Returns
    -------
    Response message with JSON data in Translator Reasoner API Standard or error status response for unsupported
    requested version
    """
    if version is None:
        version = '1.0.0'

    try:
        version = Version(version)
    except ValueError:
        return f'TRAPI version {version} not supported. Please use semantic version specifier, e.g., 1.0.0', 400

    if Version('1.0.0-beta') <= version < Version('1.1.0'):
        trapi = cohd_trapi_100.CohdTrapi100(request)
        return trapi.operate()
    elif version == Version('0.9.3'):
        trapi = cohd_trapi_093.CohdTrapi093(request)
        return trapi.operate()
    else:
        return f'TRAPI version {version} not supported', 501
