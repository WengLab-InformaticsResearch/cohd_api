"""
Implementation of the NCATS Biodmedical Data Translator TRAPI Spec
https://github.com/NCATS-Tangerine/NCATS-ReasonerStdAPI/tree/master/API
"""

from flask import jsonify

from . import cohd_trapi_093


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
    if version is None or version == '0.9.3':
        trapi = cohd_trapi_093.CohdTrapi093(request)
        return trapi.operate()
    elif version == '1.0.0' or version == '1.0':
        return 'TRAPI 1.0.0 implementation coming soon', 501
    else:
        return f'TRAPI version {version} not supported', 501