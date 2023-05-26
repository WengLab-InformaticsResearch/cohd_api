import jsonschema
import copy
from functools import lru_cache
import requests
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
from reasoner_validator import TRAPIResponseValidator
from reasoner_validator.trapi import TRAPISchemaValidator, openapi_to_jsonschema


# Reasoner-Validator can only validate on released versions, which is problematic when we need to validate on a TRAPI
# version that is not officially released yet. Add a utility function to allow specifying a schema url for a specific
# TRAPI version

# TODO: Untested after reasoner-validator 3.0 updates, but seems like a straightforward change
@lru_cache()
def _load_schema_url(trapi_schema_url: str):
    """Load schema from GitHub."""
    response = requests.get(trapi_schema_url, timeout=10)
    spec = yaml.load(response.text, Loader=Loader)
    components = spec["components"]["schemas"]
    for component, schema in components.items():
        openapi_to_jsonschema(schema)
    schemas = dict()
    for component in components:
        # build json schema against which we validate
        subcomponents = copy.deepcopy(components)
        schema = subcomponents.pop(component)
        schema["components"] = {"schemas": subcomponents}
        schemas[component] = schema
    return schemas


def validate_trapi_schema_url(instance, component, trapi_schema_url):
    """Validate instance against schema.

    Parameters
    ----------
    instance
        instance to validate
    component : str
        component to validate against
    trapi_schema_url : str
        URL of TRAPI schema yaml

    Raises
    ------
    `ValidationError <https://python-jsonschema.readthedocs.io/en/latest/errors/#jsonschema.exceptions.ValidationError>`_
        If the instance is invalid.

    Examples
    --------
    >>> validate({"message": {}}, "Query", "1.0.3")
    """
    schema = _load_schema_url(trapi_schema_url)[component]
    jsonschema.validate(instance, schema)


def validate_trapi_12x(instance, component):
    """Validate instance against schema.

    Parameters
    ----------
    instance
        instance to validate
    component : str
        component to validate against

    Raises
    ------
    `ValidationError <https://python-jsonschema.readthedocs.io/en/latest/errors/#jsonschema.exceptions.ValidationError>`_
        If the instance is invalid.

    Examples
    --------
    >>> validate({"message": {}}, "Query")
    """
    url = 'https://raw.githubusercontent.com/NCATSTranslator/ReasonerAPI/8dd458d27ae9df2cd1d17e563f989314ea51fed8/TranslatorReasonerAPI.yaml'
    return validate_trapi_schema_url(instance, component, url)


def validate_trapi_13x(instance, component):
    """Validate instance against TRAPI 1.3 schema.

    Parameters
    ----------
    instance
        instance to validate
    component : str
        component to validate against

    Raises
    ------
    `ValidationError <https://python-jsonschema.readthedocs.io/en/latest/errors/#jsonschema.exceptions.ValidationError>`_
        If the instance is invalid.

    Examples
    --------
    >>> validate({"message": {}}, "Query")
    """
    # Pre-TRAPI Release Validation
    # url = 'https://raw.githubusercontent.com/NCATSTranslator/ReasonerAPI/1.3/TranslatorReasonerAPI.yaml'
    # return validate_trapi_schema_url(instance, component, url)

    # Validate against official TRAPI 1.3 release
    validator = TRAPISchemaValidator(trapi_version='1.3.0')
    return validator.validate(instance, component)


def validate_trapi_14x(instance, component):
    """Validate instance against TRAPI 1.4 schema.

    Parameters
    ----------
    instance
        instance to validate
    component : str
        component to validate against

    Raises
    ------
    `ValidationError <https://python-jsonschema.readthedocs.io/en/latest/errors/#jsonschema.exceptions.ValidationError>`_
        If the instance is invalid.

    Examples
    --------
    >>> validate({"message": {}}, "Query")
    """
    # Validate against official TRAPI 1.4 release
    validator = TRAPISchemaValidator(trapi_version='1.4.0-beta4')
    return validator.validate(instance, component)


def validate_trapi_response(trapi_version, bl_version, response):
    """ Uses the reasoner_validator's more advanced TRAPIResponseValidator to perform thorough validation

    Parameters
    ----------
    trapi_version: str - TRAPI version, e.g., '1.3.0'
    bl_version: str - biolink version, e.g., '3.0.3'
    response: TRAPI Response object (pass the whole response, but only the message is validated)

    Returns
    -------
    Response validation messages
    """
    # Ignore the following codes
    codes_ignore = {
        'warnings': [
            'warning.response.knowledge_graph.empty',  # For TRAPI error responses, COHD uses null KG, which is allowed
            'warning.response.results.empty',  # For TRAPI error responses, COHD uses null results, which is allowed
        ],
        'errors': [
        ]
    }

    # Validation
    validator = TRAPIResponseValidator(
        trapi_version=trapi_version,
        biolink_version=bl_version,
        strict_validation=None
    )
    validator.check_compliance_of_trapi_response(response)
    vms = validator.get_messages()

    # Ignore certain codes
    for level, codes in codes_ignore.items():
        v_messages = vms[level]
        for code in codes:
            # remove code from dictionary, no error if code not in dict
            v_messages.pop(code, None)

    return vms
