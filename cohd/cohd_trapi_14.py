from datetime import datetime
from numbers import Number
import logging
from typing import Union, List, Iterable

from flask import jsonify
import werkzeug
from jsonschema import ValidationError

from . import query_cohd_mysql
from .cohd_utilities import omop_concept_curie
from .cohd_trapi import *
from .biolink_mapper import *
from .trapi.reasoner_validator_ext import validate_trapi_13x as validate_trapi
from .translator import bm_toolkit
from .translator.ontology_kp import OntologyKP


class CohdTrapi140(CohdTrapi):
    """
    Pseudo-reasoner conforming to NCATS Biomedical Data Translator Reasoner API Spec 1.0
    """

    # Biolink categories that COHD TRAPI supports (only the lowest level listed, not including ancestors)
    supported_categories = ['biolink:Disease', 'biolink:Drug', 'biolink:PhenotypicFeature', 'biolink:Procedure',
                            'biolink:SmallMolecule']

    # Biolink predicates that COHD TRAPI supports (only the lowest level listed, not including ancestors)
    supported_edge_types = ['biolink:positively_correlated_with', 'biolink:negatively_correlated_with',
                            'biolink:has_real_world_evidence_of_association_with']

    # Biolink predicates that request positive associations only
    edge_types_positive = ['biolink:positively_correlated_with']
    default_positive_predicate = edge_types_positive[0]

    # Biolink predicates that request positive associations only
    edge_types_negative = ['biolink:negatively_correlated_with']
    default_negative_predicate = edge_types_negative[0]

    tool_version = f'{CohdTrapi._SERVICE_NAME} 6.3.0'
    schema_version = '1.4.0'
    biolink_version = '3.1.2'

    def __init__(self, request):
        super().__init__(request)

        self._start_time = datetime.now()
        self._time_limit = CohdTrapi.default_time_limit

        self._valid_query = False
        self._invalid_query_response = None
        self._json_data = None
        self._response = None
        self._query_graph = None
        self._concept_1_qnode_key = None
        self._concept_2_qnode_key = None
        self._concept_1_ancestor_dict = None
        self._concept_2_ancestor_dict = None
        # Boolean indicating if concept_1 (from API context) is the subject node (True) or object node (False)
        self._concept_1_is_subject_qnode = True
        self._query_options = None
        self._method = None
        self._concept_1_omop_ids = None
        self._concept_2_omop_ids = None
        self._dataset_id = None
        self._domain_class_pairs = None
        self._threshold = None
        self._criteria = []
        self._min_cooccurrence = None
        self._confidence_interval = None
        self._request = request
        self._max_results_per_input = CohdTrapi.default_max_results_per_input
        self._max_results = CohdTrapi.default_max_results
        self._local_oxo = CohdTrapi.default_local_oxo
        self._kg_nodes = {}
        self._knowledge_graph = {
            'nodes': {},
            'edges': {}
        }
        # Track in the KG which CURIEs are being used by which OMOP IDs (may be more than 1 OMOP ID)
        self._kg_curie_omop_use = defaultdict(list)
        # Track mappings from OMOP to Biolink used for this KG
        self._kg_omop_curie_map = dict()
        self._cohd_results = []
        self._results = []
        self._logs = []
        self._log_level = CohdTrapi.default_log_level

        # Determine how the query should be performed
        self._interpret_query()

    def log(self, message: str, code: TrapiStatusCode = None, level=logging.DEBUG):
        # Add to TRAPI log if above desired log level
        if level >= self._log_level:
            self._logs.append({
                'timestamp': datetime.now().isoformat(),
                'level': logging.getLevelName(level),
                'code': None if code is None else code.value,
                'message': message
            })

        # Also pass the message to the root logger
        logging.log(level=level, msg=message)

    def _check_query_input(self):
        """ Check that the input JSON data has the expected fields

        Parameters
        ----------
        json_data - JSON data from request

        Returns
        -------
        If successful: (True, None)
        If failed: (False, Response)

        """
        # Check the body contains the proper json request object
        if self._json_data is None:
            self._valid_query = False
            self._invalid_query_response = ('Missing JSON request body', 400)
            return self._valid_query, self._invalid_query_response

        # Use TRAPI Reasoner Validator to validate the query
        try:
            validate_trapi(self._json_data, "Query")
            self.log('Query passed reasoner validator')
        except ValidationError as err:
            self._valid_query = False
            self._invalid_query_response = (str(err), 400)
            return self._valid_query, self._invalid_query_response

        # Check for the query_graph (nullable, but required for COHD)
        query_message = self._json_data.get('message')
        query_graph = query_message.get('query_graph')
        if query_graph is None or not query_graph:
            self._valid_query = False
            self._invalid_query_response = ('Unsupported query: query_graph missing from query_message or empty', 400)
            return self._valid_query, self._invalid_query_response

        # Check the structure of the query graph. Should have 2 nodes and 1 edge (one-hop query)
        nodes = query_graph.get('nodes')
        edges = query_graph.get('edges')
        if nodes is None or len(nodes) != 2 or edges is None or len(edges) != 1:
            self._valid_query = False
            self._invalid_query_response = ('Unsupported query. Only one-hop queries supported.', 400)
            return self._valid_query, self._invalid_query_response

        # Check the workflow. Should be at most a single lookup operation
        workflow = self._json_data.get('workflow')
        if workflow and type(workflow) is list:
            if len(workflow) > 1 or workflow[0]['id'] != 'lookup':
                self._valid_query = False
                self._invalid_query_response = ('Unsupported workflow. Only a single "lookup" operation is supported',
                                                400)
                return self._valid_query, self._invalid_query_response

        # Everything looks good so far
        self._valid_query = True
        self._invalid_query_response = None
        return True, None

    def _find_query_node(self, query_node_id):
        """ Finds the desired query node by node_id from the dict of query nodes in query_graph

        Parameters
        ----------
        query_nodes - List of nodes in query_graph
        query_node_id - node_id of desired node

        Returns
        -------
        query node object, or None if not found
        """
        assert self._query_graph is not None and query_node_id,\
            'cohd_translator.py::COHDTranslatorReasoner::_find_query_nodes() - empty query_graph or query_node_id'

        return self._query_graph['nodes'].get(query_node_id)

    @staticmethod
    def _process_qnode_category(categories: Union[str, Iterable]) -> List[str]:
        """ Process a qnode's categories in a query graph for COHD.
        1) converts a singular string to a list
        2) make sure format is correct (biolink prefix and snake case)
        3) suggest categories that COHD handles better

        Parameters
        ----------
        categories: str or list of str of categories

        Returns
        -------
        list of categories
        """
        # Fix any non-conforming category strings
        categories = [fix_blm_category(cat) for cat in categories]

        # Suggest changes to certain categories
        for i, category in enumerate(categories):
            preferred_category = suggest_blm_category(category)
            if preferred_category is not None and preferred_category not in categories:
                categories[i] = preferred_category

        return categories

    def _interpret_query(self):
        """ Interprets the JSON request data for how the query should be performed.

        Parameters
        ----------
        query_graph - The query graph (see Reasoner API Standard)

        Returns
        -------
        True if input is valid, otherwise (False, message)
        """
        # Log that TRAPI 1.3 was called because there's no clear indication otherwise
        logging.debug('Query issued against TRAPI 1.3')

        try:
            self._json_data = self._request.get_json()
            logging.info(f'Client: {self._request.remote_addr}\n{json.dumps(self._json_data)}')
        except werkzeug.exceptions.BadRequest:
            self._valid_query = False
            self._invalid_query_response = ('Request body is not valid JSON', 400)
            return self._valid_query, self._invalid_query_response

        if self._json_data is None:
            self._valid_query = False
            self._invalid_query_response = ('Missing JSON payload', 400)
            return self._valid_query, self._invalid_query_response

        # Get the log level
        log_level = self._json_data.get('log_level')
        if log_level is not None:
            log_level_enum = {
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR
            }
            self._log_level = log_level_enum.get(log_level, CohdTrapi.default_log_level)

        # Check that the query input has the correct structure
        input_check = self._check_query_input()
        if not input_check[0]:
            return input_check

        # Get options that don't fit into query_graph structure from query_options
        self._query_options = self._json_data.get('query_options')
        if self._query_options is None:
            # No query options provided. Get default options for all query options (below)
            self._query_options = dict()

        # Get the query method and check that it matches a supported type
        self._method = self._query_options.get('method')
        if self._method is None or not self._method or not isinstance(self._method, str):
            self._method = CohdTrapi.default_method
            self._query_options['method'] = CohdTrapi.default_method
        else:
            if self._method not in CohdTrapi.supported_query_methods:
                self._valid_query = False
                self._invalid_query_response = ('Query method "{method}" not supported. Options are: {methods}'.format(
                    method=self._method, methods=','.join(CohdTrapi.supported_query_methods)), 400)
                return self._valid_query, self._invalid_query_response

        # Get the query_option for dataset ID
        self._dataset_id = self._query_options.get('dataset_id')
        self._dataset_auto = False
        self._id_categories = set()
        self._qnode_categories = set()
        if self._dataset_id is None or not self._dataset_id or not isinstance(self._dataset_id, Number):
            self._dataset_id = CohdTrapi.default_dataset_id
            self._query_options['dataset_id'] = CohdTrapi.default_dataset_id
            self._dataset_auto = True

        # Get the query_option for minimum co-occurrence
        self._min_cooccurrence = self._query_options.get('min_cooccurrence')
        if self._min_cooccurrence is None or not isinstance(self._min_cooccurrence, Number):
            self._min_cooccurrence = CohdTrapi.default_min_cooccurrence
            self._query_options['min_cooccurrence'] = CohdTrapi.default_min_cooccurrence

        # Get the query_option for confidence_interval. Only used for obsExpRatio. If not specified, use default.
        self._confidence_interval = self._query_options.get('confidence_interval')
        if self._confidence_interval is None or not isinstance(self._confidence_interval, Number) or \
                self._confidence_interval < 0 or self._confidence_interval >= 1:
            self._confidence_interval = CohdTrapi.default_confidence_interval
            self._query_options['confidence_interval'] = CohdTrapi.default_confidence_interval

        # Get the query_option for local_oxo
        self._local_oxo = self._query_options.get('local_oxo')
        if self._local_oxo is None or not isinstance(self._local_oxo, bool):
            self._local_oxo = CohdTrapi.default_local_oxo

        # Get the query_option for maximum mapping distance
        self._mapping_distance = self._query_options.get('mapping_distance')
        if self._mapping_distance is None or not isinstance(self._mapping_distance, Number):
            self._mapping_distance = CohdTrapi.default_mapping_distance

        # Get query_option for including only Biolink nodes
        self._biolink_only = self._query_options.get('biolink_only')
        if self._biolink_only is None or not isinstance(self._biolink_only, bool):
            self._biolink_only = CohdTrapi.default_biolink_only

        # Get query_option for the desired maximum number of results
        max_results = self._query_options.get('max_results')
        if max_results:
            # Don't allow user to specify more than default
            self._max_results = min(max_results, CohdTrapi.limit_max_results)

        # Get query information from query_graph
        self._query_graph = self._json_data['message']['query_graph']

        # Check that the query_graph is supported by the COHD reasoner (1-hop query)
        edges = self._query_graph['edges']
        if len(edges) != 1:
            self._valid_query = False
            self._invalid_query_response = (f'{CohdTrapi._SERVICE_NAME} reasoner only supports 1-hop queries', 400)
            return self._valid_query, self._invalid_query_response

        # Check if the edge type is supported by COHD Reasoner and how it should be processed
        self._query_edge_key = list(edges.keys())[0]  # Get first and only edge
        self._query_edge = edges[self._query_edge_key]
        self._query_edge_predicates = self._query_edge.get('predicates')
        if self._query_edge_predicates is not None:
            edge_supported = False
            positive_edge = False
            negative_edge = False
            for edge_predicate in self._query_edge_predicates:
                # Check if this is a valid biolink predicate
                if not bm_toolkit.is_predicate(edge_predicate):
                    self._valid_query = False
                    self._invalid_query_response = (f'{edge_predicate} was not recognized as a biolink predicate', 400)
                    return self._valid_query, self._invalid_query_response

                # Check if any of the predicates are an ancestor of the supported edge predicates
                predicate_descendants = bm_toolkit.get_descendants(edge_predicate, reflexive=True, formatted=True)
                for pd in predicate_descendants:
                    if pd in CohdTrapi130.supported_edge_types:
                        edge_supported = True
                        break

                # Check directionality of predicate
                if edge_predicate in CohdTrapi130.edge_types_positive:
                    positive_edge = True
                elif edge_predicate in CohdTrapi130.edge_types_negative:
                    negative_edge = True
                else:
                    positive_edge = True
                    negative_edge = True

            if edge_supported:
                # Determine which predicate to use - temporary legacy support of has RWE predicate
                if len(self._query_edge_predicates) == 1 and \
                        self._query_edge_predicates[0] == 'biolink:has_real_world_evidence_of_association_with':
                    self._kg_edge_predicate = 'biolink:has_real_world_evidence_of_association_with'
                    self._association_direction = 0  # query both positive and negative associations
                else:
                    # Will use pos/negatively correlated with predicates as determined by data
                    self._kg_edge_predicate = None

                    # Determine which association directions to query for
                    if positive_edge and not negative_edge:
                        self._association_direction = 1
                    elif negative_edge and not positive_edge:
                        self._association_direction = -1
                    else:
                        self._association_direction = 0
            else:
                self._valid_query = False
                self._invalid_query_response = (f'None of the predicates in {self._query_edge_predicates} '
                                                f'are supported by {CohdTrapi._SERVICE_NAME}.', 400)
                return self._valid_query, self._invalid_query_response
        else:
            # TRAPI does not require predicates. If no predicates specified, find all relations
            self._kg_edge_predicate = CohdTrapi.default_predicate
            self._association_direction = 0

        # Get the QNodes
        # Note: qnode_key refers to the key identifier for the qnode in the QueryGraph's nodes property, e.g., "n00"
        subject_qnode_key = self._query_edge['subject']
        subject_qnode = self._find_query_node(subject_qnode_key)
        if subject_qnode is None:
            self._valid_query = False
            self._invalid_query_response = (f'QNode id "{subject_qnode_key}" not found in query graph', 400)
            return self._valid_query, self._invalid_query_response
        object_qnode_key = self._query_edge['object']
        object_qnode = self._find_query_node(object_qnode_key)
        if object_qnode is None:
            self._valid_query = False
            self._invalid_query_response = (f'QNode id "{object_qnode_key}" not found in query graph', 400)
            return self._valid_query, self._invalid_query_response

        # In COHD queries, concept_id_1 must be specified by ID. Figure out which QNode to use for concept_1
        node_ids = set()
        concept_1_qnode = None
        concept_2_qnode = None
        if 'ids' in subject_qnode:
            self._concept_1_is_subject_qnode = True
            self._concept_1_qnode_key = subject_qnode_key
            concept_1_qnode = subject_qnode
            self._concept_2_qnode_key = object_qnode_key
            concept_2_qnode = object_qnode

            # Check the length of the IDs list is below the batch size limit
            ids = subject_qnode['ids']
            if len(ids) > CohdTrapi.batch_size_limit:
                # Warn the client and truncate the ids list
                description = f"More IDs ({len(ids)}) in QNode '{subject_qnode_key}' than batch_size_limit allows "\
                              f"({CohdTrapi.batch_size_limit}). IDs list will be truncated."
                self.log(description, code=None, level=logging.WARNING)
                ids = ids[:CohdTrapi.batch_size_limit]
                subject_qnode['ids'] = ids
            node_ids = node_ids.union(ids)
        if 'ids' in object_qnode:
            if 'ids' not in subject_qnode:
                # Swap the subj/obj mapping to concept1/2 if only the obj node has IDs
                self._concept_1_is_subject_qnode = False
                self._concept_1_qnode_key = object_qnode_key
                concept_1_qnode = object_qnode
                self._concept_2_qnode_key = subject_qnode_key
                concept_2_qnode = subject_qnode

            # Check the length of the IDs list is below the batch size limit
            ids = object_qnode['ids']
            if len(ids) > CohdTrapi.batch_size_limit:
                # Warn the client and truncate the ids list
                description = f"More IDs ({len(ids)}) in QNode '{object_qnode_key}' than batch_size_limit allows " \
                              f"({CohdTrapi.batch_size_limit}). IDs list will be truncated."
                self.log(description, code=None, level=logging.WARNING)
                ids = ids[:CohdTrapi.batch_size_limit]
                object_qnode['ids'] = ids
            node_ids = node_ids.union(ids)
        node_ids = list(node_ids)

        # COHD queries require at least 1 node with a specified ID
        if len(node_ids) == 0:
            self._valid_query = False
            self._invalid_query_response = (f'{CohdTrapi._SERVICE_NAME} TRAPI requires at least one node to have an ID',
                                            400)
            return self._valid_query, self._invalid_query_response

        # Get qnode categories and check the formatting
        self._concept_1_qnode_categories = concept_1_qnode.get('categories', None)
        if self._concept_1_qnode_categories is not None:
            self._concept_1_qnode_categories = CohdTrapi130._process_qnode_category(self._concept_1_qnode_categories)
            self._qnode_categories = self._qnode_categories.union(self._concept_1_qnode_categories)

            # Check if any of the categories supported by COHD are included in the categories list (or one of their
            # descendants)
            found_supported_cat = False
            for supported_cat in CohdTrapi130.supported_categories:
                for queried_cat in self._concept_1_qnode_categories:
                    # Check if this is a valid biolink category
                    if not bm_toolkit.is_category(queried_cat):
                        self._valid_query = False
                        self._invalid_query_response = (f'{queried_cat} was not recognized as a biolink category',
                                                        400)
                        return self._valid_query, self._invalid_query_response

                    # Check if the COHD supported categories are descendants of the queried categories
                    if supported_cat in bm_toolkit.get_descendants(queried_cat, reflexive=True, formatted=True):
                        found_supported_cat = True
                        break

            if not found_supported_cat:
                # None of the categories for this QNode were mapped to OMOP
                self._valid_query = False
                description = f"None of QNode {self._concept_1_qnode_key}'s categories " \
                              f"({self._concept_1_qnode_categories}) are supported by {CohdTrapi._SERVICE_NAME}"
                response = self._trapi_mini_response(TrapiStatusCode.UNSUPPORTED_QNODE_CATEGORY, description)
                self._invalid_query_response = response, 200
                return self._valid_query, self._invalid_query_response

        self._concept_2_qnode_categories = concept_2_qnode.get('categories', None)
        if self._concept_2_qnode_categories is not None:
            self._concept_2_qnode_categories = CohdTrapi130._process_qnode_category(self._concept_2_qnode_categories)
            self._qnode_categories = self._qnode_categories.union(self._concept_2_qnode_categories)
            concept_2_qnode['categories'] = self._concept_2_qnode_categories

            # Check if any of the categories supported by COHD are included in the categories list (or one of their
            # descendants)
            self._domain_class_pairs = set()
            for supported_cat in CohdTrapi130.supported_categories:
                for queried_cat in self._concept_2_qnode_categories:
                    # Check if this is a valid biolink category
                    if not bm_toolkit.is_category(queried_cat):
                        self._valid_query = False
                        self._invalid_query_response = (f'{queried_cat} was not recognized as a biolink category',
                                                        400)
                        return self._valid_query, self._invalid_query_response

                    # Check if the COHD supported categories are descendants of the queried categories
                    if supported_cat in bm_toolkit.get_descendants(queried_cat, reflexive=True, formatted=True):
                        dc_pair = map_blm_class_to_omop_domain(supported_cat)
                        self._domain_class_pairs = self._domain_class_pairs.union(dc_pair)

            # Remove any domain-class pairs where the concept_class_id is specified if the broader domain is also there
            dcps_to_remove = set()
            for dcp in self._domain_class_pairs:
                if dcp.concept_class_id is not None and DomainClass(dcp.domain_id, None) in self._domain_class_pairs:
                    dcps_to_remove.add(dcp)
            self._domain_class_pairs -= dcps_to_remove

            if self._domain_class_pairs is None or len(self._domain_class_pairs) == 0:
                # None of the categories for this QNode were mapped to OMOP
                self._valid_query = False
                description = f"None of QNode {self._concept_2_qnode_key}'s categories " \
                              f"({self._concept_2_qnode_categories}) are supported by {CohdTrapi._SERVICE_NAME}"
                response = self._trapi_mini_response(TrapiStatusCode.UNSUPPORTED_QNODE_CATEGORY, description)
                self._invalid_query_response = response, 200
                return self._valid_query, self._invalid_query_response

        # If client provided non-empty QNode constraints, respond with error code
        if concept_1_qnode.get('constraints') or concept_2_qnode.get('constraints'):
            self._valid_query = False
            description = f'{CohdTrapi._SERVICE_NAME} does not support QNode constraints'
            self.log(description, TrapiStatusCode.UNSUPPORTED_CONSTRAINT, logging.ERROR)
            response = self._trapi_mini_response(TrapiStatusCode.UNSUPPORTED_CONSTRAINT, description)
            self._invalid_query_response = response, 200
            return self._valid_query, self._invalid_query_response
        if self._query_edge.get("attribute_constraints"):
            self._valid_query = False
            description = f'{CohdTrapi._SERVICE_NAME} does not support QEdge attribute constraints'
            self.log(description, TrapiStatusCode.UNSUPPORTED_ATTR_CONSTRAINT, logging.ERROR)
            response = self._trapi_mini_response(TrapiStatusCode.UNSUPPORTED_ATTR_CONSTRAINT, description)
            self._invalid_query_response = response, 200
            return self._valid_query, self._invalid_query_response
        if self._query_edge.get("qualifier_constraints"):
            self._valid_query = False
            description = f'{CohdTrapi._SERVICE_NAME} does not support QEdge qualifier constraints'
            self.log(description, TrapiStatusCode.UNSUPPORTED_QUAL_CONSTRAINT, logging.ERROR)
            response = self._trapi_mini_response(TrapiStatusCode.UNSUPPORTED_QUAL_CONSTRAINT, description)
            self._invalid_query_response = response, 200
            return self._valid_query, self._invalid_query_response

        # Check to see if cohd doesn't recognize any properties
        qnode_properties = {'ids','categories', 'is_set', 'constraints'}
        qedge_properties = {'knowledge_type', 'predicates', 'subject', 'object', 'attribute_constraints',
                            'qualifier_constraints'}
        sep = ', '
        unrec_properties = set(concept_1_qnode.keys()) - qnode_properties
        if unrec_properties:
            description = f'{CohdTrapi._SERVICE_NAME} does not recognize the following properties: ' \
                          f'{sep.join(unrec_properties)}. {CohdTrapi._SERVICE_NAME} will ignore these properties.'
            self.log(description, level=logging.WARNING)

        unrec_properties = set(concept_2_qnode.keys()) - qnode_properties
        if unrec_properties:
            description = f'{CohdTrapi._SERVICE_NAME} does not recognize the following properties: ' \
                          f'{sep.join(unrec_properties)}. {CohdTrapi._SERVICE_NAME} will ignore these properties.'
            self.log(description, level=logging.WARNING)

        unrec_properties = set(self._query_edge.keys()) - qedge_properties
        if unrec_properties:
            description = f'{CohdTrapi._SERVICE_NAME} does not recognize the following properties: ' \
                          f'{sep.join(unrec_properties)}. {CohdTrapi._SERVICE_NAME} will ignore these properties.'
            self.log(description, level=logging.WARNING)

        # Get concept_id_1. QNode IDs is a list.
        self._concept_1_omop_ids = list()
        found = False
        ids = list(set(concept_1_qnode['ids']))  # remove duplicate CURIEs

        # Get subclasses for all CURIEs using ontology KP
        descendant_ids = list()
        ancestor_dict = dict()

        descendant_results = OntologyKP.get_descendants(ids, self._concept_1_qnode_categories)
        if descendant_results is not None:
            # Add new descendant CURIEs to the end of IDs list
            descendants, ancestor_dict = descendant_results
            descendant_ids = list(set(descendants.keys()) - set(ids))
            if len(descendant_ids) > 0:
                if (len(ids) + len(descendant_ids)) > CohdTrapi.batch_size_limit:
                    # Only add up to the batch_size_limit
                    n_to_add = CohdTrapi.batch_size_limit - len(ids)
                    descendant_ids_ignored = descendant_ids[n_to_add:]
                    descendant_ids = descendant_ids[:n_to_add]
                    description = f"More descendants from Ontology KP for QNode '{self._concept_1_qnode_key}'"\
                                  f"than batch_size_limit allows. Ignored: {descendant_ids_ignored}."
                    self.log(description, level=logging.WARNING)

                ids.extend(descendant_ids)
                ids_deduped = SriNodeNormalizer.remove_equivalents(ids)
                if ids_deduped is not None:
                    ids = ids_deduped
                else:
                    self.log(f'Issue encountered with SRI Node Norm when removing equivalents', level=logging.WARNING)
                self.log(f"Adding descendants from Ontology KP to QNode '{self._concept_1_qnode_key}': {descendant_ids}.",
                         level=logging.INFO)
            else:
                self.log(f"No descendants found from Ontology KP for QNode '{self._concept_1_qnode_key}'.",
                         level=logging.INFO)
        else:
            # Add a warning that we didn't get descendants from Ontology KP
            self.log(f"Issue with retrieving descendants from Ontology KP for QNode '{self._concept_1_qnode_key}'",
                     level=logging.WARNING)

        # Update the ancestor dictionary for concept 1
        self._concept_1_ancestor_dict = ancestor_dict

        # Find BLM - OMOP mappings for all identified query nodes
        node_mappings, normalized_nodes = BiolinkConceptMapper.map_to_omop(ids)
        if normalized_nodes is None:
            # Issue getting normalized nodes. Log a warning, but attempt to continue
            self.log('Encountered an issue when querying Node Norm', level=logging.WARNING)
        else:
            # Keep track of all categories
            for nn in normalized_nodes.values():
                if nn is not None:
                    self._id_categories = self._id_categories.union(nn.categories)

        # Map as many IDs to OMOP as possible
        unmapped_curies = list()
        # Fetch all OMOP concept definitions at once to save time
        concept_1_omop_ids = [int(mapping.omop_id.split(':')[1]) for curie, mapping in node_mappings.items() if mapping is not None]
        concept_1_omop_defs = query_cohd_mysql.omop_concept_definitions(concept_1_omop_ids)
        for curie in ids:
            if node_mappings[curie] is not None:
                # Found an OMOP mapping. Use this CURIE
                concept_1_mapping = node_mappings[curie]
                concept_1_omop_id = int(concept_1_mapping.omop_id.split(':')[1])
                self._concept_1_omop_ids.append(concept_1_omop_id)
                self._kg_omop_curie_map[concept_1_omop_id] = curie
                found = True

                # If category wasn't specified in QNode, try to get it from SRI Node Normalizer results
                qnode_categories = self._concept_1_qnode_categories
                if self._concept_1_qnode_categories is None and normalized_nodes is not None \
                        and normalized_nodes.get(curie) is not None:
                    qnode_categories = normalized_nodes[curie].categories

                # Create a KG node now with the curie and mapping specified
                concept_name = ''
                domain = ''
                concept_class = ''
                if concept_1_omop_defs and concept_1_omop_id in concept_1_omop_defs:
                    concept_def = concept_1_omop_defs[concept_1_omop_id]
                    concept_name = concept_def.get('concept_name', concept_name)
                    domain = concept_def.get('domain_id', domain)
                    concept_class = concept_def.get('concept_class_id', concept_class)
                inode = self._get_kg_node(concept_1_omop_id, concept_name=concept_name, domain=domain,
                                          concept_class=concept_class, query_node_curie=curie,
                                          query_node_categories=qnode_categories, mapping=concept_1_mapping)
                self._add_internal_node_to_kg(inode)

                # Debug logging
                message = f"Mapped node '{self._concept_1_qnode_key}' ID {curie} to OMOP:{concept_1_omop_id}"
                self.log(message, level=logging.DEBUG)
            else:
                # No OMOP mapping found. Just add the node to the KG.
                unmapped_curies.append(curie)
                if normalized_nodes is not None and (nn:= normalized_nodes.get(curie)) is not None:
                    # Use node norm info when available
                    self._add_kg_node(curie, CohdTrapi130._make_kg_node(name=nn.normalized_identifier.label,
                                                                        categories=nn.categories))
                else:
                    # No node norm info available, make an empty KG node
                    self._add_kg_node(curie, CohdTrapi130._make_kg_node())

            # For descendant nodes, add subclass_of edge
            if curie in descendant_ids and curie in ancestor_dict:
                self._add_kg_edge_subclass_of(curie, ancestor_dict[curie])

        # Log mapped and unmapped CURIEs
        reverse_map = {v:f'OMOP:{k}' for k,v in self._kg_omop_curie_map.items() if k in self._concept_1_omop_ids}
        message = f"Mapped node '{self._concept_1_qnode_key}' IDs to OMOP: {reverse_map}"
        self.log(message, level=logging.INFO)
        if found and len(unmapped_curies) > 0:
            # Couldn't map some CURIEs in qnode to OMOP
            message = f"Could not map node '{self._concept_1_qnode_key}' IDs {unmapped_curies} to OMOP concepts"
            self.log(message, TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, logging.WARNING)

        if not found:
            # Couldn't map any CURIEs in qnode to OMOP
            self._valid_query = False
            description = f"Could not map node '{self._concept_1_qnode_key}' to OMOP concept"
            self.log(description, code=TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, level=logging.WARNING)
            response = self._trapi_mini_response(TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, description)
            self._invalid_query_response = response, 200
            return self._valid_query, self._invalid_query_response

        # Get the desired association concept or category
        ids = concept_2_qnode.get('ids')
        if ids is not None and ids:
            ids = list(set(ids))  # remove duplicate CURIEs

            # If CURIE of the 2nd node is specified, then query the association between concept_1 and concept_2
            self._domain_class_pairs = None

            # Get subclasses for all CURIEs using ontology KP
            descendant_ids = list()
            ancestor_dict = dict()
            descendant_results = OntologyKP.get_descendants(ids, self._concept_2_qnode_categories)
            if descendant_results is not None:
                # Add new descendant CURIEs to the end of IDs list
                descendants, ancestor_dict = descendant_results
                descendant_ids = list(set(descendants.keys()) - set(ids))
                if len(descendant_ids) > 0:
                    if (len(ids) + len(descendant_ids)) > CohdTrapi.batch_size_limit:
                        # Only add up to the batch_size_limit
                        n_to_add = CohdTrapi.batch_size_limit - len(ids)
                        descendant_ids_ignored = descendant_ids[n_to_add:]
                        descendant_ids = descendant_ids[:n_to_add]
                        description = f"More descendants from Ontology KP for QNode '{self._concept_2_qnode_key}'" \
                                      f"than batch_size_limit allows. Ignored: {descendant_ids_ignored}."
                        self.log(description, level=logging.WARNING)

                    ids.extend(descendant_ids)
                    ids_deduped = SriNodeNormalizer.remove_equivalents(ids)
                    if ids_deduped is not None:
                        ids = ids_deduped
                    else:
                        self.log(f'Issue encountered with SRI Node Norm when removing equivalents',
                                 level=logging.WARNING)
                    self.log(f"Adding descendants from Ontology KP to QNode '{self._concept_2_qnode_key}': {descendant_ids}.",
                             level=logging.INFO)
                else:
                    self.log(f"No descendants found from Ontology KP for QNode '{self._concept_2_qnode_key}'.",
                             level=logging.INFO)
            else:
                # Add a warning that we didn't get descendants from Ontology KP
                self.log(f"Issue with retrieving descendants from Ontology KP for QNode '{self._concept_2_qnode_key}'",
                         level=logging.WARNING)

            # Update the ancestor dictionary for concept 2
            self._concept_2_ancestor_dict = ancestor_dict

            # Find BLM - OMOP mappings for all identified query nodes
            node_mappings, normalized_nodes = BiolinkConceptMapper.map_to_omop(ids)
            if normalized_nodes is None:
                # Issue getting normalized nodes. Log a warning, but attempt to continue
                self.log('Encountered an issue when querying Node Norm', level=logging.WARNING)
            else:
                # Keep track of all categories
                for nn in normalized_nodes.values():
                    if nn is not None:
                        self._id_categories = self._id_categories.union(nn.categories)

            # Map as many of the QNode IDs to OMOP as we can
            self._concept_2_omop_ids = list()
            found = False
            unmapped_curies = list()
            # Fetch all OMOP concept definitions at once to save time
            concept_2_omop_ids = [int(mapping.omop_id.split(':')[1]) for curie, mapping in node_mappings.items() if
                                  mapping is not None]
            concept_2_omop_defs = query_cohd_mysql.omop_concept_definitions(concept_2_omop_ids)
            for curie in ids:
                if node_mappings.get(curie) is not None:
                    # Found an OMOP mapping. Use this CURIE
                    concept_2_mapping = node_mappings[curie]
                    concept_2_omop_id = int(concept_2_mapping.omop_id.split(':')[1])
                    self._concept_2_omop_ids.append(concept_2_omop_id)
                    self._kg_omop_curie_map[concept_2_omop_id] = curie
                    found = True

                    # If category wasn't specified in QNode, try to get it from SRI Node Normalizer results
                    qnode_categories = self._concept_2_qnode_categories
                    if self._concept_2_qnode_categories is None and normalized_nodes is not None \
                            and normalized_nodes.get(curie) is not None:
                        qnode_categories = normalized_nodes[curie].categories

                    # Create a KG node now with the curie and mapping specified
                    concept_name = ''
                    domain = ''
                    concept_class = ''
                    if concept_2_omop_defs and concept_2_omop_id in concept_2_omop_defs:
                        concept_def = concept_2_omop_defs[concept_2_omop_id]
                        concept_name = concept_def.get('concept_name', concept_name)
                        domain = concept_def.get('domain_id', domain)
                        concept_class = concept_def.get('concept_class_id', concept_class)
                    inode = self._get_kg_node(concept_2_omop_id, concept_name=concept_name, domain=domain,
                                              concept_class=concept_class, query_node_curie=curie,
                                              query_node_categories=qnode_categories, mapping=concept_2_mapping)
                    self._add_internal_node_to_kg(inode)
                else:
                    # No OMOP mapping found. Just add the node to the KG.
                    unmapped_curies.append(curie)
                    if normalized_nodes is not None:
                        nn = normalized_nodes.get(curie)
                        if nn is not None:
                            # Use node norm info when available
                            self._add_kg_node(curie, CohdTrapi130._make_kg_node(name=nn.normalized_identifier.label,
                                                                                categories=nn.categories))
                    else:
                        # No node norm info available, make an empty KG node
                        self._add_kg_node(curie, CohdTrapi130._make_kg_node())

                # For descendant nodes, add subclass_of edge
                if curie in descendant_ids and curie in ancestor_dict:
                    self._add_kg_edge_subclass_of(curie, ancestor_dict[curie])

            # Log mapped and unmapped CURIEs
            reverse_map = {v:f'OMOP:{k}' for k,v in self._kg_omop_curie_map.items() if k in self._concept_2_omop_ids}
            message = f"Mapped node '{self._concept_2_qnode_key}' IDs to OMOP: {reverse_map}"
            self.log(message, level=logging.INFO)
            if found and len(unmapped_curies) > 0:
                # Couldn't map some CURIEs in qnode to OMOP
                message = f"Could not map node '{self._concept_2_qnode_key}' IDs {unmapped_curies} to OMOP concepts"
                self.log(message, TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, logging.WARNING)

            if not found:
                # Couldn't map any CURIEs in qnode to OMOP
                self._valid_query = False
                description = f"Could not map node '{self._concept_2_qnode_key}' to OMOP concept"
                self.log(description, code=TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, level=logging.WARNING)
                response = self._trapi_mini_response(TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, description)
                self._invalid_query_response = response, 200
                return self._valid_query, self._invalid_query_response
        elif self._concept_2_qnode_categories is not None and self._concept_2_qnode_categories:
            # If CURIE is not specified and target node's category is specified, then query the association
            # between concept_1 and all concepts in the domain
            self._concept_2_omop_ids = None

            # If biolink:NamedThing is in the list of categories, query for associations against all concepts
            if 'biolink:NamedThing' in self._concept_2_qnode_categories:
                self._domain_class_pairs = None
                self.log(f'Querying associations to all OMOP domains', level=logging.INFO)
            else:
                for dc_pair in self._domain_class_pairs:
                    if dc_pair.concept_class_id is not None:
                        self.log(f'Querying associations to all OMOP domain {dc_pair.domain_id}:'
                                 f'{dc_pair.concept_class_id}', level=logging.INFO)
                    else:
                        self.log(f'Querying associations to all OMOP domain {dc_pair.domain_id}', level=logging.INFO)
        else:
            # No CURIE or type specified, query for associations against all concepts
            self._concept_2_omop_ids = None
            self._domain_class_pairs = None
            self.log(f'Querying associations to all OMOP domains', level=logging.INFO)

        # Criteria for returning results
        self._criteria = []

        # Add a criterion for minimum co-occurrence
        if self._min_cooccurrence > 0:
            self._criteria.append(ResultCriteria(function=criteria_min_cooccurrence,
                                                 kargs={'cooccurrence': self._min_cooccurrence}))

        # Get query_option for threshold. Don't use filter if not specified (i.e., no default option for threshold)
        self._threshold = self._query_options.get('threshold')
        if self._threshold is not None and isinstance(self._threshold, Number):
            self._criteria.append(ResultCriteria(function=criteria_threshold,
                                                 kargs={'threshold': self._threshold}))

        # If the method is obsExpRatio, add a criteria for confidence interval
        if self._method.lower() == 'obsexpratio' and self._confidence_interval > 0:
            self._criteria.append(ResultCriteria(function=criteria_confidence,
                                                 kargs={'confidence': self._confidence_interval}))

        if self._dataset_auto:
            # Automatically select the dataset based on which data types being queried
            # Use the non-hierarchical 5-year dataset when drugs are not involved
            self._dataset_id = 1

            # Check if any QNode IDs are chemicals
            chemical_descendants = bm_toolkit.get_descendants('biolink:ChemicalEntity', reflexive=True, formatted=True)
            for id_category in self._id_categories:
                if id_category in chemical_descendants:
                    # Use the hierarchical 5-year dataset when IDs that are chemicals are queried
                    self._dataset_id = 3
                    break

            # Check if any of the QNode categories include chemicals
            for qnode_category in self._qnode_categories:
                cat_descendants = set(bm_toolkit.get_descendants(qnode_category, reflexive=True, formatted=True))
                if len(cat_descendants.intersection(chemical_descendants)) > 0:
                    # Use the hierarchical 5-year dataset whenever categories may include chemicals
                    self._dataset_id = 3
                    break

        if self._valid_query:
            return True
        else:
            return self._valid_query, self._invalid_query_response

    def operate(self):
        """ Performs the COHD query and reasoning.

        Returns
        -------
        Response message with JSON data in Translator Reasoner API Standard
        """
        # Check if the query is valid
        if self._valid_query:
            self._cohd_results = []
            self._initialize_trapi_response()

            for i, concept_1_omop_id in enumerate(self._concept_1_omop_ids):
                # Limit the amount of time the TRAPI query runs for
                ellapsed_time = (datetime.now() - self._start_time).total_seconds()
                if ellapsed_time > self._time_limit:
                    skipped_curies = [self._kg_omop_curie_map[x] for x in self._concept_1_omop_ids[i:]]
                    description = f'Maximum time limit {self._time_limit} sec reached before all input IDs processed. '\
                                  f'Skipped IDs: {skipped_curies}'
                    self.log(description, level=logging.WARNING)
                    break

                new_cohd_results = list()
                if self._concept_2_omop_ids is None:
                    # Node 2's IDs were not specified
                    if self._domain_class_pairs:
                        # Node 2's category was specified. Query associations between Node 1 and the requested
                        # categories (domains)
                        for domain_id, concept_class_id in self._domain_class_pairs:
                            json_results = query_cohd_mysql.query_trapi(concept_id_1=concept_1_omop_id,
                                                                        concept_id_2=None,
                                                                        dataset_id=self._dataset_id,
                                                                        domain_id=domain_id,
                                                                        concept_class_id=concept_class_id,
                                                                        ln_ratio_sign=self._association_direction,
                                                                        confidence=self._confidence_interval)
                            if json_results:
                                new_cohd_results.extend(json_results['results'])
                    else:
                        # No category (domain) was specified for Node 2. Query the associations between Node 1 and all
                        # domains
                        json_results = query_cohd_mysql.query_trapi(concept_id_1=concept_1_omop_id, concept_id_2=None,
                                                                    dataset_id=self._dataset_id, domain_id=None,
                                                                    ln_ratio_sign=self._association_direction,
                                                                    confidence=self._confidence_interval)
                        if json_results:
                            new_cohd_results.extend(json_results['results'])

                else:
                    # Concept 2's IDs were specified. Query Concept 1 against all IDs for Concept 2
                    for concept_2_id in self._concept_2_omop_ids:
                        json_results = query_cohd_mysql.query_trapi(concept_id_1=concept_1_omop_id,
                                                                    concept_id_2=concept_2_id,
                                                                    dataset_id=self._dataset_id, domain_id=None,
                                                                    confidence=self._confidence_interval)
                        if json_results:
                            new_cohd_results.extend(json_results['results'])

                # Results within each query call should be sorted, but still need to be sorted across query calls
                new_cohd_results = sort_cohd_results(new_cohd_results)

                # Convert results from COHD format to Translator Reasoner standard
                results_limit_reached = self._add_results_to_trapi(new_cohd_results)

                # Log warnings and stop when results limits reached
                if results_limit_reached:
                    curie = self._kg_omop_curie_map[concept_1_omop_id]
                    self.log(f'Results limit ({self._max_results_per_input}) reached for {curie}. '
                             'There may be additional associations.', level=logging.WARNING)
                    if len(self._results) >= self._max_results:
                        if i < len(self._concept_1_omop_ids) - 1:
                            skipped_ids = [self._kg_omop_curie_map[x] for x in self._concept_1_omop_ids[i+1:]]
                            self.log(f'Total results limit ({self._max_results}) reached. Skipped {skipped_ids}',
                                    level=logging.WARNING)
                        break

            return self._finalize_trapi_response()
        else:
            # Invalid query. Return the invalid query response
            return self._invalid_query_response

    def _add_cohd_result(self, cohd_result, criteria):
        """ Adds a COHD result. The COHD result is always added to the knowledge graph. If the COHD result passes all
        criteria, it is also added to the results.

        Parameters
        ----------
        cohd_result
        criteria: List - [ResultCriteria]
        """
        assert cohd_result is not None and 'concept_id_1' in cohd_result and 'concept_id_2' in cohd_result, \
            'Translator::KnowledgeGraph::add_edge() - Bad cohd_result'

        # Check if result passes all filters before adding
        if criteria is not None:
            if not all([c.check(cohd_result) for c in criteria]):
                return

        # Get node for concept 1
        concept_1_id = cohd_result['concept_id_1']
        node_1 = self._get_kg_node(concept_1_id, query_node_categories=self._concept_1_qnode_categories)
        

        if not node_1.get('query_category_compliant', False) or \
                (self._biolink_only and not node_1.get('biolink_compliant', False)):
            # Only include results when node_1 maps to biolink and matches the queried category
            return

        # Get node for concept 2
        concept_2_id = cohd_result['concept_id_2']
        concept_2_name = cohd_result.get('concept_2_name')
        concept_2_domain = cohd_result.get('concept_2_domain')
        concept_2_class_id = cohd_result.get('concept_2_class_id')
        node_2 = self._get_kg_node(concept_2_id, concept_2_name, concept_2_domain, concept_2_class_id,
                                   query_node_categories=self._concept_2_qnode_categories)

        if not node_2.get('query_category_compliant', False) or \
                (self._biolink_only and not node_2.get('biolink_compliant', False)):
            # Only include results when node_2 maps to biolink and matches the queried category
            return

        # Only allow one OMOP ID to use a CURIE. Will allow the first result using a given CURIE to go through. Since
        # results are in descending order, will give priority to the OMOP ID with the strongest association
        concept_1_curie = node_1['primary_curie']
        concept_2_curie = node_2['primary_curie']
        if ((self._kg_curie_omop_use[concept_1_curie] and
            concept_1_id not in self._kg_curie_omop_use[concept_1_curie]) or
            (self._kg_curie_omop_use[concept_2_curie] and
            concept_2_id not in self._kg_curie_omop_use[concept_2_curie])):
            return

        # Add nodes and edge to knowledge graph
        if self._query_edge['subject'] == self._concept_1_qnode_key:
            subject_node = node_1
            object_node = node_2
        elif self._query_edge['subject'] == self._concept_2_qnode_key:
            subject_node = node_2
            object_node = node_1
        else:
            raise LookupError("Error mapping query nodes to edges")
        kg_node_1, kg_node_2, kg_edge, kg_edge_id = self._add_kg_edge(subject_node, object_node, cohd_result)

        # Add to results
        score = score_cohd_result(cohd_result)
        self._add_result(node_1['primary_curie'], node_2['primary_curie'], kg_edge_id, score)

    def _add_result(self, kg_node_1_id, kg_node_2_id, kg_edge_id, score):
        """ Adds a knowledge graph edge to the results list

        Parameters
        ----------
        kg_node_1_id: Subject node ID
        kg_node_2_id: Object node ID
        kg_edge_id: edge ID
        score: result score

        Returns
        -------
        result
        """
        result = {
            'node_bindings': {
                self._concept_1_qnode_key: [{
                    'id': kg_node_1_id
                }],
                self._concept_2_qnode_key: [{
                    'id': kg_node_2_id
                }]
            },
            'edge_bindings': {
                self._query_edge_key: [{
                    'id': kg_edge_id
                }]
            },
            'score': score
        }

        # If QNodes have IDs and the bound KGNode ID is different (e.g., descendant CURIE), then specify the query_id
        qnode1 = self._find_query_node(self._concept_1_qnode_key)
        if qnode1.get('ids') is not None and kg_node_1_id not in qnode1['ids']:
            result['node_bindings'][self._concept_1_qnode_key][0]['query_id'] = self._concept_1_ancestor_dict.get(kg_node_1_id)

        qnode2 = self._find_query_node(self._concept_2_qnode_key)
        if qnode2.get('ids') is not None and kg_node_2_id not in qnode2['ids']:
            result['node_bindings'][self._concept_2_qnode_key][0]['query_id'] = self._concept_2_ancestor_dict.get(kg_node_2_id)

        self._results.append(result)
        return result

    def _sort_results(self):
        """ Sort the TRAPI results in descending order of score
        """
        if not self._results:
            return

        scores = [result['score'] for result in self._results]
        self._results = [self._results[i] for i in list(reversed(argsort(scores)))]

    def _get_kg_node(self, concept_id, concept_name=None, domain=None, concept_class=None, query_node_curie=None,
                     query_node_categories=None, mapping: OmopBiolinkMapping=None):
        """ Gets the node from internal "graph" representing the OMOP concept. Creates the node if not yet created.
        Node is not added to the knowledge graph or results.

        Parameters
        ----------
        concept_id: OMOP concept ID
        concept_name: OMOP concept name
        domain: OMOP concept domain
        concept_class: OMOP concept class ID
        query_node_curie: CURIE used in the QNode corresponding to this KG Node
        query_node_categories: list of categories for this QNode
        mapping: mapping between OMOP and Biolink

        Returns
        -------
        Node for internal use
        """
        node = self._kg_nodes.get(concept_id)

        if node is None:
            # Create the node
            if concept_name is None or domain is None or concept_class is None:
                # Concept information not specified, lookup concept definition
                concept_name = concept_name if concept_name is not None else ''
                domain = domain if domain is not None else ''
                concept_def = query_cohd_mysql.omop_concept_definition(concept_id)

                if concept_def is not None:
                    if not concept_name:
                        concept_name = concept_def['concept_name']
                    if not domain:
                        domain = concept_def['domain_id']
                    if not concept_class:
                        concept_class = concept_def['concept_class_id']

            # If we don't find better mappings (below) for this concept, default to OMOP CURIE and label
            omop_curie = omop_concept_curie(concept_id)
            primary_curie = omop_curie
            primary_label = concept_name
            # Whether or not this node has been mapped to biolink
            mapped_to_blm = False
            # Whether or not this node's category fits the queried category (queries for biolink:Disease use OMOP query
            # for Condition which may return diseases and phenotypic features)
            query_category_compliant = False

            if query_node_curie is not None and query_node_curie:
                # The CURIE was specified for this node in the query_graph, use that CURIE to identify this node
                mapped_to_blm = True
                query_category_compliant = True
                primary_curie = query_node_curie

                # Find the label from the mappings
                if mapping is not None:
                    primary_label = mapping.biolink_label

                if query_node_categories:
                    # The query specified both the ID and the category. Use the specified category
                    blm_categories = query_node_categories
                else:
                    blm_categories = [map_omop_domain_to_blm_class(domain, concept_class)]
            else:
                # Map to Biolink Model
                blm_category = map_omop_domain_to_blm_class(domain, concept_class)
                blm_categories = [blm_category]
                mapping, normalized_categories = BiolinkConceptMapper.map_from_omop(concept_id)
                if mapping is not None:
                    primary_curie = mapping.biolink_id
                    primary_label = mapping.biolink_label
                    mapped_to_blm = True

                if normalized_categories is not None:
                    blm_categories = normalized_categories

                # Check if at least 1 of the blm_categories is a descendant of the queried category
                if not query_node_categories:
                    # No categories specified, then all are allowed
                    query_category_compliant = True
                else:
                    for query_category in query_node_categories:
                        desc = bm_toolkit.get_descendants(query_category, reflexive=True, formatted=True)
                        for blm_category in blm_categories:
                            if blm_category in desc:
                                query_category_compliant = True
                                break

            # Create representations for the knowledge graph node and query node, but don't add them to the graphs yet
            internal_id = '{id:06d}'.format(id=len(self._kg_nodes))
            node = {
                'omop_id': concept_id,
                'name': concept_name,
                'domain': domain,
                'internal_id': internal_id,
                'primary_curie': primary_curie,
                'in_kgraph': False,
                'biolink_compliant': mapped_to_blm,
                'query_category_compliant': query_category_compliant,
                'kg_node': {
                    'name': primary_label,
                    'categories': blm_categories
                }
            }

            # Add the OMOP-Biolink mapping
            if mapping is not None:
                node['kg_node']['attributes'] = [{
                    'attribute_type_id': 'EDAM:data_0954',  # Database cross-mapping
                    'original_attribute_name': 'Database cross-mapping',
                    'value': mapping.provenance,
                    'value_type_id': 'EDAM:data_0954',  # Database cross-mapping
                    'attribute_source': CohdTrapi._INFORES_ID,
                    'attributes': [
                        {
                            'attribute_type_id': 'EDAM:data_1087',  # Ontology concept ID
                            'original_attribute_name': 'concept_id',
                            'value': omop_curie,
                            'value_type_id': 'EDAM:data_1087',  # Ontology concept ID
                            'attribute_source': 'infores:omop-ohdsi',
                            'value_url': f'https://athena.ohdsi.org/search-terms/terms/{concept_id}'
                        },
                        {
                            'attribute_type_id': 'EDAM:data_2339',  # Ontology concept name
                            'original_attribute_name': 'concept_name',
                            'value': concept_name,
                            'value_type_id': 'EDAM:data_2339',  # Ontology concept name
                            'attribute_source': 'infores:omop-ohdsi',
                        },
                        {
                            'attribute_type_id': 'EDAM:data_0967',  # Ontology concept data
                            'original_attribute_name': 'domain',
                            'value': domain,
                            'value_type_id': 'EDAM:data_0967',  # Ontology concept data
                            'attribute_source': 'infores:omop-ohdsi',
                        }
                    ]
                }]

            self._kg_nodes[concept_id] = node

        return node

    @staticmethod
    def _make_kg_node(name: Optional[str] = None, categories: Optional[List[str]] = None, attributes: Optional[List[Any]] = None):
        """ Makes the KG node

        Parameters
        ----------
        name: node name
        categories: node categories
        attributes: node attributes """
        if categories is None:
            categories = list()

        return {
            'name': name,
            'categories': categories,
            'attributes': attributes
        }

    def _add_kg_node(self, id, node):
        """ Adds the node to the knowledge graph

        Parameters
        ----------
        node: Node
        """
        self._knowledge_graph['nodes'][id] = node

    def _add_internal_node_to_kg(self, node):
        """ Adds the internal node to the knowledge graph

        Parameters
        ----------
        node: Node

        Returns
        -------
        node
        """
        kg_node = node['kg_node']
        if not node['in_kgraph']:
            curie = node['primary_curie']
            self._knowledge_graph['nodes'][curie] = kg_node
            node['in_kgraph'] = True
            self._kg_curie_omop_use[curie].append(node['omop_id'])

        return kg_node

    def _get_new_kg_edge_id(self) -> str:
        """ Mint a new KG edge identifier
        """
        return 'ke{id:06d}'.format(id=len(self._knowledge_graph['edges']))

    def _add_kg_edge(self, node_1, node_2, cohd_result):
        """ Adds the edge to the knowledge graph

        Parameters
        ----------
        node_1: Subject node
        node_2: Object node
        cohd_result: COHD result - data gets added to edge

        Returns
        -------
        kg_node_1, kg_node_2, kg_edge
        """
        # Add nodes to knowledge graph
        kg_node_1 = self._add_internal_node_to_kg(node_1)
        kg_node_2 = self._add_internal_node_to_kg(node_2)

        # Mint a new identifier
        ke_id = self._get_new_kg_edge_id()

        # Add properties from COHD results to the edge attributes
        attributes = [
            # Information Resource - Source Retrieval Provenance
            # Guidance: https://docs.google.com/document/d/177sOmjTueIK4XKJ0GjxsARg909CaU71tReIehAp5DDo/edit#
            {
                'attribute_type_id': 'biolink:primary_knowledge_source',
                'value': CohdTrapi._INFORES_ID,
                'value_type_id': 'biolink:InformationResource',
                'attribute_source': CohdTrapi._INFORES_ID,
                'value_url': 'http://cohd.io/api/query',
                'description': 'The COHD KP defines associations between biomedical concepts based on statistical '
                               'analysis of clinical/EHR data.'
            },
            {
                'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                'original_attribute_name': 'dataset_id',
                'value': f"COHD:dataset_{cohd_result['dataset_id']}",
                'value_type_id': 'EDAM:data_1048',  # Database ID
                'attribute_source': CohdTrapi._INFORES_ID,
                'description': f'Dataset ID within {CohdTrapi._SERVICE_NAME}'
            },
            # Basic counts
            {
                "attribute_source": CohdTrapi._INFORES_ID,
                "attribute_type_id": "biolink:has_supporting_study_result",
                "description": "A study result describing the initial count of concepts",
                "value": "N/A",
                "value_type_id": "biolink:ConceptCountAnalysisResult",
                "attributes": [
                    {
                        'attribute_type_id': 'biolink:concept_pair_count',
                        'original_attribute_name': 'concept_pair_count',
                        'value': cohd_result['concept_pair_count'],
                        'value_type_id': 'EDAM:data_0006',  # Data
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Observed concept count between the pair of subject and object nodes'
                    },
                    {
                        'attribute_type_id': 'biolink:concept_count_subject',
                        'original_attribute_name': 'concept_count_subject',
                        'value': cohd_result[
                            'concept_1_count' if self._concept_1_is_subject_qnode else 'concept_2_count'],
                        'value_type_id': 'EDAM:data_0006',  # Data
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Observed concept count of the subject node'
                    },
                    {
                        'attribute_type_id': 'biolink:concept_count_object',
                        'original_attribute_name': 'concept_count_object',
                        'value': cohd_result[
                            'concept_2_count' if self._concept_1_is_subject_qnode else 'concept_1_count'],
                        'value_type_id': 'EDAM:data_0006',  # Data
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Observed concept count of the object node'
                    },
                    {
                        'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                        'original_attribute_name': 'dataset_id',
                        'value': f"COHD:dataset_{cohd_result['dataset_id']}",
                        'value_type_id': 'EDAM:data_1048',  # Database ID
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Dataset ID within {CohdTrapi._SERVICE_NAME}'
                    }
                ]
            },
            # Chi-square analysis
            {
                "attribute_source": CohdTrapi._INFORES_ID,
                "attribute_type_id": "biolink:has_supporting_study_result",
                "description": "A study result describing a chi-squared analysis on a single pair of concepts",
                "value": "N/A",
                "value_type_id": "biolink:ChiSquaredAnalysisResult",
                "attributes": [
                    {
                        'attribute_type_id': 'biolink:unadjusted_p_value',
                        'original_attribute_name': 'p-value',
                        'value': cohd_result['chi_square_p-value'],
                        'value_type_id': 'EDAM:data_1669',  # P-value
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'value_url': 'http://edamontology.org/data_1669',
                        'description': 'Chi-square p-value, unadjusted. http://cohd.io/about.html'
                    },
                    {
                        'attribute_type_id': 'biolink:bonferonni_adjusted_p_value',
                        'original_attribute_name': 'p-value adjusted',
                        'value': cohd_result['chi_square_p-value_adjusted'],
                        'value_type_id': 'EDAM:data_1669',  # P-value
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'value_url': 'http://edamontology.org/data_1669',
                        'description': 'Chi-square p-value, Bonferonni adjusted by number of pairs of concepts. '
                                       'http://cohd.io/about.html'
                    },
                    {
                        'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                        'original_attribute_name': 'dataset_id',
                        'value': f"COHD:dataset_{cohd_result['dataset_id']}",
                        'value_type_id': 'EDAM:data_1048',  # Database ID
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Dataset ID within {CohdTrapi._SERVICE_NAME}'
                    }
                ]
            },
            # Observed-expected frequency ratio analysis
            {
                "attribute_source": CohdTrapi._INFORES_ID,
                "attribute_type_id": "biolink:has_supporting_study_result",
                "description": "A study result describing an observed-expected frequency anaylsis on a single pair of concepts",
                "value": "N/A",
                "value_type_id": "biolink:ObservedExpectedFrequencyAnalysisResult",
                "attributes": [
                    {
                        'attribute_type_id': 'biolink:expected_count',
                        'original_attribute_name': 'expected_count',
                        'value': cohd_result['expected_count'],
                        'value_type_id': 'EDAM:operation_3438',
                        # Calculation (not sure if it's correct to use an operation)
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Calculated expected count of concept pair. For ln_ratio. http://cohd.io/about.html'
                    },
                    {
                        'attribute_type_id': 'biolink:ln_ratio',
                        'original_attribute_name': 'ln_ratio',
                        'value': cohd_result['ln_ratio'],
                        'value_type_id': 'EDAM:data_1772',  # Score
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Observed-expected frequency ratio. http://cohd.io/about.html'
                    },
                    {
                        'attribute_type_id': 'biolink:ln_ratio_confidence_interval',
                        'original_attribute_name': 'ln_ratio_confidence_interval',
                        'value': cohd_result['ln_ratio_ci'],
                        'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Observed-expected frequency ratio {self._confidence_interval}% confidence interval'
                    },
                    {
                        'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                        'original_attribute_name': 'dataset_id',
                        'value': f"COHD:dataset_{cohd_result['dataset_id']}",
                        'value_type_id': 'EDAM:data_1048',  # Database ID
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Dataset ID within {CohdTrapi._SERVICE_NAME}'
                    }
                ]
            },
            # Relative frequency analysis
            {
                "attribute_source": CohdTrapi._INFORES_ID,
                "attribute_type_id": "biolink:has_supporting_study_result",
                "description": "A study result describing a relative frequency anaylsis on a single pair of concepts",
                "value": "N/A",
                "value_type_id": "biolink:RelativeFrequencyAnalysisResult",
                "attributes": [
                    {
                        'attribute_type_id': 'biolink:relative_frequency_subject',
                        'original_attribute_name': 'relative_frequency_subject',
                        'value': cohd_result['relative_frequency_1' if self._concept_1_is_subject_qnode else
                                             'relative_frequency_2'],
                        'value_type_id': 'EDAM:data_1772',  # Score
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Relative frequency, relative to the subject node. http://cohd.io/about.html'
                    },
                    {
                        'attribute_type_id': 'biolink:relative_frequency_subject_confidence_interval',
                        'original_attribute_name': 'relative_freq_subject_confidence_interval',
                        'value': cohd_result['relative_frequency_1_ci' if self._concept_1_is_subject_qnode else
                                             'relative_frequency_2_ci'],
                        'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Relative frequency (subject) {self._confidence_interval}% confidence interval'
                    },
                    {
                        'attribute_type_id': 'biolink:relative_frequency_object',
                        'original_attribute_name': 'relative_frequency_object',
                        'value': cohd_result['relative_frequency_2' if self._concept_1_is_subject_qnode else
                                             'relative_frequency_1'],
                        'value_type_id': 'EDAM:data_1772',  # Score
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': 'Relative frequency, relative to the object node. http://cohd.io/about.html'
                    },
                    {
                        'attribute_type_id': 'biolink:relative_frequency_object_confidence_interval',
                        'original_attribute_name': 'relative_freq_object_confidence_interval',
                        'value': cohd_result['relative_frequency_2_ci' if self._concept_1_is_subject_qnode else
                                             'relative_frequency_1_ci'],
                        'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Relative frequency (object) {self._confidence_interval}% confidence interval'
                    },
                    {
                        'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                        'original_attribute_name': 'dataset_id',
                        'value': f"COHD:dataset_{cohd_result['dataset_id']}",
                        'value_type_id': 'EDAM:data_1048',  # Database ID
                        'attribute_source': CohdTrapi._INFORES_ID,
                        'description': f'Dataset ID within {CohdTrapi._SERVICE_NAME}'
                    }
                ]
            }
        ]
        # From calculation of chi_square
        chi_study_results_attributes = attributes[3]['attributes']
        for key in ['n', 'n_c1', 'n_c1_c2', 'n_c1_~c2', 'n_c2', 'n_~c1_c2', 'n_~c1_~c2']:
            if key in cohd_result:
                chi_study_results_attributes.append({
                    'attribute_type_id': f'biolink:has_count_{key}',
                    'original_attribute_name': key,
                    'value': cohd_result[key],
                    'value_type_id': 'EDAM:data_0006',  # Data
                    'attribute_source': CohdTrapi._INFORES_ID
                })

        # Determine which predicate to use
        predicate = CohdTrapi.default_predicate
        if self._kg_edge_predicate is not None:
            predicate = self._kg_edge_predicate
        else:
            ln_ratio = cohd_result['ln_ratio']
            if ln_ratio > 0:
                predicate = self.default_positive_predicate
            elif ln_ratio < 0:
                predicate = self.default_negative_predicate
            else:
                predicate = self.default_predicate

        # Set the knowledge graph edge properties
        kg_edge = {
            'predicate': predicate,
            'subject': node_1['primary_curie'],
            'object': node_2['primary_curie'],
            'attributes': attributes
        }

        # Add the new edge
        self._knowledge_graph['edges'][ke_id] = kg_edge

        return kg_node_1, kg_node_2, kg_edge, ke_id

    def _add_kg_edge_subclass_of(self, descendant_node_id, ancestor_node_id):
        """ Adds the biolink:subclass_of edge to the knowledge graph

        Parameters
        ----------
        node_1: Subject node
        node_2: Object node
        cohd_result: COHD result - data gets added to edge

        Returns
        -------
        kg_node_1, kg_node_2, kg_edge
        """
        # Check that this pair is not already in the KG
        for edge in self._knowledge_graph['edges'].values():
            if edge['predicate'] == 'biolink:subclass_of' and \
                edge['subject'] == descendant_node_id and \
                edge['object'] == ancestor_node_id:
                return

        # Add a new subclass_of edge
        ke_id = self._get_new_kg_edge_id()
        self._knowledge_graph['edges'][ke_id] = {
            'predicate': 'biolink:subclass_of',
            'subject': descendant_node_id,
            'object': ancestor_node_id,
            'attributes': [{
                'attribute_type_id': 'biolink:primary_knowledge_source',
                'value': OntologyKP.INFORES_ID,
                'value_type_id': 'biolink:InformationResource',
                'attribute_source': CohdTrapi._INFORES_ID,
                'value_url': OntologyKP.base_url
            },{
                'attribute_type_id': 'biolink:aggregator_knowledge_source',
                'value': CohdTrapi._INFORES_ID,
                'value_type_id': 'biolink:InformationResource',
                'attribute_source': CohdTrapi._INFORES_ID
            }]
        }

    def _initialize_trapi_response(self):
        """ Starts the TRAPI response message
        """
        self._response = {
            # From TRAPI Extended
            'reasoner_id': CohdTrapi._INFORES_ID,
            'tool_version': CohdTrapi130.tool_version,
            'schema_version': CohdTrapi130.schema_version,
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'query_options': self._query_options,
        }

    def _add_results_to_trapi(self, new_cohd_results):
        """ Creates the response message with JSON data in Reasoner Std API format

        Returns
        -------
        boolean: True if results limit reached, otherwise False
        """
        if self._cohd_results is not None:
            self._cohd_results.extend(new_cohd_results)
            n_prior_results = len(self._results)
            for i, result in enumerate(new_cohd_results):
                # Don't add more than the maximum number of results per input ID
                if len(self._results) - n_prior_results >= self._max_results_per_input:
                    return True
                # Don't add more than the maximum total number of results
                if len(self._results) >= self._max_results:
                    return True

                self._add_cohd_result(result, self._criteria)
        return False

    def _finalize_trapi_response(self, status: TrapiStatusCode = TrapiStatusCode.SUCCESS):
        """ Finalizes the TRAPI response

        Returns
        -------
        JSON TRAPI response
        """
        if len(self._results) == 0:
            status = TrapiStatusCode.NO_RESULTS
        self._response['status'] = status.value

        self._response['description'] = f'{CohdTrapi._SERVICE_NAME} returned {len(self._results)} results.'

        self._sort_results()
        self._response['message'] = {
            'results': self._results,
            'query_graph': self._query_graph,
            'knowledge_graph': self._knowledge_graph
        }

        if self._logs is not None and self._logs:
            self._response['logs'] = self._logs

        return jsonify(self._response)

    def _trapi_mini_response(self,
                             status: TrapiStatusCode,
                             description: str):
        """ Creates a minimal TRAPI response without creating the knowledge graph or results.
        This is useful for situations where some issue occurred but the TRAPI convention expects an HTTP
        Status Code 200 and TRAPI Response object.

        Returns
        -------
        Response message with JSON data in Reasoner Std API format
        """
        response = {
            'status': status.value,
            'description': description,
            'message': {
                'results': None,
                'query_graph': self._query_graph,
                'knowledge_graph': None
            },
            # From TRAPI Extended
            'reasoner_id': CohdTrapi._INFORES_ID,
            'tool_version': CohdTrapi130.tool_version,
            'schema_version': CohdTrapi130.schema_version,
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'query_options': self._query_options,
        }
        if self._logs is not None and self._logs:
            response['logs'] = self._logs
        return jsonify(response)
