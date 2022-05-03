"""
Implementation of the NCATS Biodmedical Data Translator TRAPI Spec
https://github.com/NCATS-Tangerine/NCATS-ReasonerStdAPI/tree/master/API
"""

from flask import jsonify
from semantic_version import Version

from . import cohd_trapi_120
from .biolink_mapper import BiolinkConceptMapper, SriNodeNormalizer, map_omop_domain_to_blm_class
from .query_cohd_mysql import omop_concept_definitions

# Get the static instance of the Biolink Model Toolkit from cohd_trapi
from .cohd_trapi import CohdTrapi
from .translator import bm_toolkit


def translator_meta_knowledge_graph():
    """ Implementation of /meta_knowledge_graph for Translator Reasoner API to provide supported nodes and edges

    Returns
    -------
    json response object
    """
    # Supported categories in most recent TRAPI implementation
    categories = cohd_trapi_120.CohdTrapi120.supported_categories

    # Add the supported nodes using all id_prefixes for each category since we use SRI Node Normalizer
    nodes = dict()
    common_node_attributes = [
        {
            'attribute_type_id': 'EDAM:data_0954',  # Database cross-mapping
            'attribute_source': CohdTrapi._INFORES_ID,
            'original_attribute_names': ['Database cross-mapping'],
            'constraint_use': False
        }
    ]
    for cat in categories:
        # Most nodes can be added using just the id_prefixes returned by the Biolink Model Toolkit
        prefixes = bm_toolkit.get_element(cat).id_prefixes
        if prefixes is not None and len(prefixes) >= 1:
            nodes[cat] = {
                'id_prefixes': prefixes,
                'attributes': common_node_attributes
            }
        else:
            # Some categories do not have any id_prefixes defined in biolink
            if cat == 'biolink:DiseaseOrPhenotypicFeature':
                # Use the union of biolink:Disease and biolink:PhenotypicFeature
                prefixes_dis = bm_toolkit.get_element('biolink:Disease').id_prefixes
                prefixes_phe = bm_toolkit.get_element('biolink:PhenotypicFeature').id_prefixes
                nodes['biolink:DiseaseOrPhenotypicFeature'] = {
                    'id_prefixes': list(set(prefixes_dis).union(prefixes_phe))
                }
            elif cat == 'biolink:Procedure':
                # ICD and SNOMED used for biolink:Disease, so for now, use these vocabularies for procedure, also
                nodes['biolink:Procedure'] = {
                    'id_prefixes': ['ICD10PCS', 'SNOMEDCT']
                }

    # Add the supported edges
    common_edge_attributes = [
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:original_knowledge_source",
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:supporting_data_source",
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:p_value",
            "original_attribute_names": ["p-value", "p-value adjusted"],
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:has_evidence",
            "original_attribute_names": ["ln_ratio",
                                         "relative_frequency_subject",
                                         "relative_frequency_object"],
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:has_confidence_level",
            "original_attribute_names": ["ln_ratio_confidence_interval",
                                         "relative_freq_subject_confidence_interval",
                                         "relative_freq_object_confidence_interval"],
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:has_count",
            "original_attribute_names": ["concept_pair_count",
                                         "concept_count_subject",
                                         "concept_count_object"],
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "EDAM:operation_3438",
            "original_attribute_names": ["expected_count"],
            "constraint_use": False
        },
        {
            "attribute_source": "infores:cohd",
            "attribute_type_id": "biolink:provided_by",
            "original_attribute_names": ["dataset_id"],
            "constraint_use": False
        }
    ]
    edges = list()
    for subject in categories:
        for object in categories:
            # Temporarily support both correlated_with and has_real_world_evidence_of_association_with
            edges.append({
                'subject': subject,
                'object': object,
                'predicate': 'biolink:correlated_with',
                'attributes': common_edge_attributes
            })
            edges.append({
                'subject': subject,
                'object': object,
                'predicate': 'biolink:has_real_world_evidence_of_association_with',
                'attributes': common_edge_attributes
            })

    return jsonify({
        'nodes': nodes,
        'edges': edges
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
        version = '1.2.0'

    try:
        version = Version(version)
    except ValueError:
        return f'TRAPI version {version} not supported. Please use semantic version specifier, e.g., 1.2.0', 400

    if Version('1.2.0-alpha') <= version < Version('1.3.0-alpha'):
        trapi = cohd_trapi_120.CohdTrapi120(request)
        return trapi.operate()
    else:
        return f'TRAPI version {version} not supported', 501


def biolink_to_omop(request):
    """ Map from biolink CURIEs to OMOP concepts

    Parameters
    ----------
    request: Flask request

    Returns
    -------
    json response like:
    {
        "MONDO:0001187": {
        "distance": 2,
        "omop_concept_id": 197508,
        "omop_concept_name": "Malignant tumor of urinary bladder"
    }
    """
    j = request.get_json()
    if j is not None and 'curies' in j and j['curies'] is not None and type(j['curies']) == list:
        curies = j['curies']
    else:
        return 'Bad request', 400

    mappings, _ = BiolinkConceptMapper.map_to_omop(curies)

    # Convert the Mappings object into a dict for return
    mappings_j = dict()
    for curie, mapping in mappings.items():
        if mapping is None:
            mappings_j[curie] = None
        else:
            omop_id = int(mapping.omop_id.split(':')[1])
            mappings_j[curie] = {
                'distance': mapping.distance,
                'omop_concept_id': omop_id,
                'omop_concept_name': mapping.omop_label,
                'mapping_history': mapping.provenance
            }

    return jsonify(mappings_j)


def omop_to_biolink(request):
    """ Map from OMOP IDs to Biolink CURIEs

    Parameters
    ----------
    request: Flask request

    Returns
    -------
    JSON response with OMOP IDs as keys and SRI Node Normalizer response as values
    """
    j = request.get_json()
    if j is not None and 'omop_ids' in j and j['omop_ids'] is not None and type(j['omop_ids']) == list:
        omop_id_input = j['omop_ids']
    else:
        return 'Bad request', 400

    # Input may be list of ints, strings, or curie format. Convert to ints
    omop_ids = list()
    for omop_id in omop_id_input:
        if type(omop_id) == int:
            omop_ids.append(omop_id)
        elif type(omop_id) == str:
            # Strip OMOP prefix
            if omop_id.lower()[:5] == 'omop:':
                omop_id = omop_id[5:]
            if omop_id.isdigit():
                omop_ids.append(int(omop_id))

    # Map to Biolink
    concept_definitions = omop_concept_definitions(omop_ids)
    mappings = dict()
    for omop_id in omop_ids:
        if omop_id in concept_definitions:
            domain_id = concept_definitions[omop_id]['domain_id']
            concept_class_id = concept_definitions[omop_id]['concept_class_id']
            mapping, _ = BiolinkConceptMapper.map_from_omop(omop_id)
            mappings[omop_id] = mapping
        else:
            mappings[omop_id] = None

    curies = [x.biolink_id for x in mappings.values() if x is not None]
    if not curies:
        # No mappings found for input OMOP IDs
        normalized_mappings = {omop_id: None for omop_id in omop_ids}
    else:
        # Normalize with SRI Node Normalizer
        normalized_mappings = dict()
        normalized_nodes = SriNodeNormalizer.get_normalized_nodes_raw(curies)
        if normalized_nodes is None:
            return 'Unexpected response or no response received from SRI Node Normalizer', 503

        for omop_id in omop_ids:
            normalized_mapping = None
            if mappings[omop_id] is not None and normalized_nodes[mappings[omop_id].biolink_id] is not None:
                m = mappings[omop_id]
                normalized_mapping = normalized_nodes[m.biolink_id]
                normalized_mapping['mapping_history'] = m.provenance
            normalized_mappings[omop_id] = normalized_mapping

    return jsonify(normalized_mappings)
