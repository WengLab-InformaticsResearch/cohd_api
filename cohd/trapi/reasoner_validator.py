import jsonschema
import copy
from functools import lru_cache
import requests
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
from reasoner_validator.util import openapi_to_jsonschema


# Reasoner-Validator can only validate on released versions, which is problematic when we need to validate on a TRAPI
# version that is not officially released yet. Add a utility function to allow specifying a schema url for a specific
# TRAPI version

@lru_cache()
def _load_schema_url(trapi_schema_url: str):
    """Load schema from GitHub."""
    response = requests.get(trapi_schema_url)
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


def validate_trapi(instance, component, trapi_schema_url):
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
    return validate_trapi(instance, component, url)


def validate_trapi_13x(instance, component):
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
    url = 'https://raw.githubusercontent.com/NCATSTranslator/ReasonerAPI/1.3/TranslatorReasonerAPI.yaml'
    return validate_trapi(instance, component, url)


