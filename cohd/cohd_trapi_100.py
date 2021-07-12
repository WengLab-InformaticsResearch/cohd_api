from datetime import datetime
from numbers import Number
import logging
from typing import Union, List, Iterable

from flask import jsonify

from . import query_cohd_mysql
from .cohd_utilities import omop_concept_curie
from .cohd_trapi import *


class CohdTrapi100(CohdTrapi):
    """
    Pseudo-reasoner conforming to NCATS Biomedical Data Translator Reasoner API Spec 1.0
    """

    def __init__(self, request):
        super().__init__(request)

        self._valid_query = False
        self._invalid_query_response = None
        self._json_data = None
        self._query_graph = None
        self._concept_1_qnode_key = None
        self._concept_2_qnode_key = None
        self._query_options = None
        self._method = None
        self._concept_1_omop_id = None
        self._concept_2_omop_id = None
        self._dataset_id = None
        self._domain_class_pairs = None
        self._threshold = None
        self._criteria = []
        self._min_cooccurrence = None
        self._confidence_interval = None
        self._concept_mapper = BiolinkConceptMapper()
        self._request = request
        self._max_results = CohdTrapi.default_max_results
        self._local_oxo = CohdTrapi.default_local_oxo
        self._kg_nodes = {}
        self._knowledge_graph = {
            'nodes': {},
            'edges': {}
        }
        self._results = []

        # Determine how the query should be performed
        self._interpret_query()

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
        self._json_data = self._request.get_json()
        if self._json_data is None:
            self._valid_query = False
            self._invalid_query_response = ('Missing JSON request body', 400)
            return self._valid_query, self._invalid_query_response

        # Check for the query message
        query_message = self._json_data.get('message')
        if query_message is None or not query_message:
            return False, ('message missing from JSON data or empty', 400)

        query_graph = query_message.get('query_graph')
        if query_graph is None or not query_graph:
            self._valid_query = False
            self._invalid_query_response = ('query_graph missing from query_message or empty', 400)
            return self._valid_query, self._invalid_query_response

        # Check the structure of the query graph. Should have 2 nodes and 1 edge
        nodes = query_graph.get('nodes')
        edges = query_graph.get('edges')
        if nodes is None or len(nodes) != 2 or edges is None or len(edges) != 1:
            self._valid_query = False
            self._invalid_query_response = ('Unsupported query', 400)
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

        assert query_node_id in self._query_graph['nodes'], \
            ('cohd_translator.py::COHDTranslatorReasoner::_find_query_nodes() - '
             f'{query_node_id} not in {",".join(self._query_graph["nodes"].keys())}')

        return self._query_graph['nodes'][query_node_id]

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
        # QNode category may be string or list. If not a list, convert to a list to simplify following code
        if type(categories) is str:
            categories = [categories]

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
        # Log that TRAPI 1.0 was called because there's no clear indication otherwise
        logging.info('Query issued against TRAPI 1.0')

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

        # Get the query_option for dataset ID
        self._dataset_id = self._query_options.get('dataset_id')
        if self._dataset_id is None or not self._dataset_id or not isinstance(self._dataset_id, Number):
            self._dataset_id = CohdTrapi.default_dataset_id
            self._query_options['dataset_id'] = CohdTrapi.default_dataset_id

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

        # Get query_option for ontology_targets
        ontology_map = self._query_options.get('ontology_targets')
        if ontology_map and isinstance(ontology_map, dict):
            self._concept_mapper = BiolinkConceptMapper(ontology_map, distance=self._mapping_distance,
                                                        local_oxo=self._local_oxo)
        else:
            # Use default ontology map
            self._concept_mapper = BiolinkConceptMapper(distance=self._mapping_distance, local_oxo=self._local_oxo)

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
            self._invalid_query_response = ('COHD reasoner only supports 1-hop queries', 400)
            return self._valid_query, self._invalid_query_response

        # Check if the edge type is supported by COHD Reasoner
        self._query_edge_key = list(edges.keys())[0]  # Get first and only edge
        self._query_edge = edges[self._query_edge_key]
        if 'predicate' in self._query_edge:
            edge_predicates = self._query_edge['predicate']
            if not isinstance(edge_predicates, list):
                # In TRAPI, QEdge type can be string or list. If it's not currently a list, convert to a list to
                # simplify following processing
                edge_predicates = [edge_predicates]
                self._query_edge['predicate'] = edge_predicates
            edge_supported = False
            for edge_predicate in edge_predicates:
                if edge_predicate in CohdTrapi.supported_edge_types:
                    edge_supported = True
                    self._query_edge_predicates = edge_predicates
                    break
            if not edge_supported:
                self._valid_query = False
                self._invalid_query_response = ('QEdge.predicate not supported by COHD Reasoner API', 400)
                return self._valid_query, self._invalid_query_response
        else:
            # TRAPI does not require predicate. If no predicate specified, suggest the use of 'biolink:correlated_with'
            self._query_edge_predicates = [CohdTrapi.default_predicate]
            self._query_edge['predicate'] = self._query_edge_predicates

        # Get the QNodes
        # Note: qnode_key refers to the key identifier for the qnode in the QueryGraph's nodes property, e.g., "n00"
        subject_qnode_key = self._query_edge['subject']
        subject_qnode = self._find_query_node(subject_qnode_key)
        object_qnode_key = self._query_edge['object']
        object_qnode = self._find_query_node(object_qnode_key)

        # In COHD queries, concept_id_1 must be specified by ID. Figure out which QNode to use for concept_1
        node_ids = set()
        concept_1_qnode = None
        concept_2_qnode = None
        if 'id' in subject_qnode:
            self._concept_1_qnode_key = subject_qnode_key
            concept_1_qnode = subject_qnode
            self._concept_2_qnode_key = object_qnode_key
            concept_2_qnode = object_qnode
            if type(subject_qnode['id']) is list:
                node_ids = node_ids.union(subject_qnode['id'])
            else:
                node_ids.add(subject_qnode['id'])
        if 'id' in object_qnode:
            self._concept_1_qnode_key = object_qnode_key
            concept_1_qnode = object_qnode
            self._concept_2_qnode_key = subject_qnode_key
            concept_2_qnode = subject_qnode
            if type(object_qnode['id']) is list:
                node_ids = node_ids.union(object_qnode['id'])
            else:
                node_ids.add(object_qnode['id'])
        node_ids = list(node_ids)

        # COHD queries require at least 1 node with a specified ID
        if len(node_ids) == 0:
            self._valid_query = False
            self._invalid_query_response = ('COHD TRAPI requires at least one node to have an ID', 400)
            return self._valid_query, self._invalid_query_response

        # Find BLM - OMOP mappings for all identified query nodes
        node_mappings = self._concept_mapper.map_to_omop(node_ids)

        # Get concept_id_1. QNode IDs can be a list. For now, just use the first ID that can map to OMOP
        found = False
        ids = concept_1_qnode['id']
        if type(ids) is str:
            ids = [ids]
        for curie in ids:
            if node_mappings[curie] is not None:
                # Found an OMOP mapping. Use this CURIE
                self._concept_1_qnode_curie = curie
                self._concept_1_mapping = node_mappings[curie]
                self._concept_1_omop_id = int(self._concept_1_mapping.output_id.split(':')[1])
                # Keep track of this mapping in the query_graph for the response
                concept_1_qnode['mapped_omop_concept'] = self._concept_1_mapping.history
                found = True
                break
        if not found:
            self._valid_query = False
            description = f'Could not map node {self._concept_1_qnode_key} to OMOP concept'
            response = self._trapi_mini_response(TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, description)
            self._invalid_query_response = response, 200
            return self._valid_query, self._invalid_query_response

        # Get qnode categories and check the formatting
        self._concept_1_qnode_categories = concept_1_qnode.get('category', None)
        if self._concept_1_qnode_categories is not None:
            self._concept_1_qnode_categories = CohdTrapi100._process_qnode_category(self._concept_1_qnode_categories)
            concept_1_qnode['category'] = self._concept_1_qnode_categories
        self._concept_2_qnode_categories = concept_2_qnode.get('category', None)
        if self._concept_2_qnode_categories is not None:
            self._concept_2_qnode_categories = CohdTrapi100._process_qnode_category(self._concept_2_qnode_categories)
            concept_2_qnode['category'] = self._concept_2_qnode_categories

        # Get the desired association concept or category
        ids = concept_2_qnode.get('id')
        if ids is not None and ids:
            # IDs were specified for the second QNode also
            # QNode IDs can be a list. For now, just use the first ID that can map to OMOP
            if type(ids) is str:
                ids = [ids]
            found = False
            for curie in ids:
                if node_mappings[curie] is not None:
                    # Found an OMOP mapping. Use this CURIE
                    self._concept_2_qnode_curie = curie
                    self._concept_2_mapping = node_mappings[curie]
                    self._concept_2_omop_id = int(self._concept_2_mapping.output_id.split(':')[1])
                    # Keep track of this mapping in the query_graph for the response
                    concept_2_qnode['mapped_omop_concept'] = self._concept_2_mapping.history
                    found = True

                    # If CURIE of the 2nd node is specified, then query the association between concept_1 and concept_2
                    self._domain_class_pairs = None
                    break
            if not found:
                self._valid_query = False
                description = f'Could not map node {self._concept_2_qnode_key} to OMOP concept'
                response = self._trapi_mini_response(TrapiStatusCode.COULD_NOT_MAP_CURIE_TO_LOCAL_KG, description)
                self._invalid_query_response = response, 200
                return self._valid_query, self._invalid_query_response
        elif self._concept_2_qnode_categories is not None and self._concept_2_qnode_categories:
            # If CURIE is not specified and target node's category is specified, then query the association
            # between concept_1 and all concepts in the domain
            self._concept_2_qnode_curie = None
            self._concept_2_omop_id = None
            # For now, only use the first category in the list that maps to a handled OMOP domain/class pair
            for category in self._concept_2_qnode_categories:
                self._domain_class_pairs = map_blm_class_to_omop_domain(category)
                if self._domain_class_pairs is not None:
                    break

            if self._domain_class_pairs is None:
                # None of the categories for this QNode were mapped to OMOP
                self._valid_query = False
                description = f"QNode {self._concept_2_qnode_key}'s category not supported by COHD"
                response = self._trapi_mini_response(TrapiStatusCode.UNSUPPORTED_QNODE_CATEGORY, description)
                self._invalid_query_response = response, 200
                return self._valid_query, self._invalid_query_response

            # Keep track of this mapping in the query graph for the response
            concept_2_qnode['mapped_omop_domains'] = [x.domain_id for x in self._domain_class_pairs]
            concept_2_qnode['mapped_omop_classes'] = [x.concept_class_id for x in self._domain_class_pairs]
        else:
            # No CURIE or type specified, query for associations against all concepts
            self._concept_2_qnode_curie = None
            self._concept_2_omop_id = None
            self._domain_class_pairs = None

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
            if self._concept_2_omop_id is None and self._domain_class_pairs:
                # Node 2 not specified, but node 2's type was specified. Query associations between Node 1 and the
                # requested types (domains)
                results = []
                for domain_id, concept_class_id in self._domain_class_pairs:
                    new_results = query_cohd_mysql.query_association(method=self._method,
                                                                     concept_id_1=self._concept_1_omop_id,
                                                                     concept_id_2=self._concept_2_omop_id,
                                                                     dataset_id=self._dataset_id,
                                                                     domain_id=domain_id,
                                                                     concept_class_id=concept_class_id,
                                                                     confidence=self._confidence_interval)
                    if new_results:
                        results.extend(new_results['results'])
            else:
                # Either Node 2 was specified by a CURIE or no type (domain) was specified for type 2. Query the
                # associations between Node 1 and Node 2 or between Node 1 and all domains
                json_results = query_cohd_mysql.query_association(method=self._method,
                                                                  concept_id_1=self._concept_1_omop_id,
                                                                  concept_id_2=self._concept_2_omop_id,
                                                                  dataset_id=self._dataset_id,
                                                                  domain_id=None,
                                                                  confidence=self._confidence_interval)
                results = json_results['results']

            # Convert results from COHD format to Translator Reasoner standard
            return self._serialize_trapi_response(results)
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
        node_1 = self._get_kg_node(concept_1_id, query_node_curie=self._concept_1_qnode_curie,
                                   query_node_categories=self._concept_1_qnode_categories)

        if self._biolink_only and not node_1.get('biolink_compliant', False):
            # Only include results when node_1 maps to biolink
            return

        # Get node for concept 2
        concept_2_id = cohd_result['concept_id_2']
        concept_2_name = cohd_result.get('concept_2_name')
        concept_2_domain = cohd_result.get('concept_2_domain')
        node_2 = self._get_kg_node(concept_2_id, concept_2_name, concept_2_domain,
                                   query_node_curie=self._concept_2_qnode_curie,
                                   query_node_categories=self._concept_2_qnode_categories)

        if self._biolink_only and not node_2.get('biolink_compliant', False):
            # Only include results when node_2 maps to biolink
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
        self._add_result(node_1['primary_curie'], node_2['primary_curie'], kg_edge_id)

    def _add_result(self, kg_node_1_id, kg_node_2_id, kg_edge_id):
        """ Adds a knowledge graph edge to the results list

        Parameters
        ----------
        kg_node_1_id: Subject node ID
        kg_node_2_id: Object node ID
        kg_edge_id: edge ID

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
            }
        }
        self._results.append(result)
        return result

    def _get_kg_node(self, concept_id, concept_name=None, domain=None, concept_class=None, query_node_curie=None,
                     query_node_categories=None):
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

            # Map to Biolink Model or other target ontologies
            blm_category = map_omop_domain_to_blm_class(domain, concept_class, query_node_categories)
            mapped_to_blm = False
            mapping = None
            if self._concept_mapper:
                mapping = self._concept_mapper.map_from_omop(concept_id, blm_category)

            if query_node_curie is not None and query_node_curie:
                # The CURIE was specified for this node in the query_graph, use that CURIE to identify this node
                mapped_to_blm = True
                primary_curie = query_node_curie
                # Find the label from the mappings
                if mapping is not None:
                    primary_label = mapping.output_label
            elif mapping is not None:
                # Choose one of the mappings to be the main identifier for the node. Prioritize distance first, and then
                # choose by the order of prefixes listed in the Concept Mapper. If no biolink prefix found, use OMOP
                primary_curie = mapping.output_id
                primary_label = mapping.output_label
                mapped_to_blm = True

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
                'kg_node': {
                    'name': primary_label,
                    'category': [blm_category],
                    'attributes': [
                        {
                            'name': 'omop_concept_id',
                            'value': omop_curie,
                            'type': 'EDAM:data_1087',  # Ontology concept ID
                            'source': 'OMOP',
                        },
                        {
                            'name': 'omop_concept_name',
                            'value': concept_name,
                            'type': 'EDAM:data_2339',  # Ontology concept name
                            'source': 'OMOP',
                        },
                        {
                            'name': 'omop_domain',
                            'value': domain,
                            'type': 'EDAM:data_0967',  # Ontology concept data
                            'source': 'OMOP',
                        }
                    ]
                }
            }
            self._kg_nodes[concept_id] = node

        return node

    def _add_kg_node(self, node):
        """ Adds the node to the knowledge graph

        Parameters
        ----------
        node: Node

        Returns
        -------
        node
        """
        kg_node = node['kg_node']
        if not node['in_kgraph']:
            self._knowledge_graph['nodes'][node['primary_curie']] = kg_node
            node['in_kgraph'] = True

        return kg_node

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
        kg_node_1 = self._add_kg_node(node_1)
        kg_node_2 = self._add_kg_node(node_2)

        # Mint a new identifier
        ke_id = 'ke{id:06d}'.format(id=len(self._knowledge_graph['edges']))

        # Add properties from COHD results to the edge attributes
        attributes = list()
        if 'p-value' in cohd_result:
            attributes.append({
                'name': 'p-value',
                'value': cohd_result['p-value'],
                'type': 'EDAM:data_1669',  # P-value
                'url': 'http://edamontology.org/data_1669',
                'source': 'COHD'
            })
        if 'confidence_interval' in cohd_result:
            attributes.append({
                'name': 'confidence_interval',
                'value': cohd_result['confidence_interval'],
                'type': 'EDAM:data_0951',  # Statistical estimate score
                'source': 'COHD'
            })
        if 'dataset_id' in cohd_result:
            attributes.append({
                'name': 'dataset_id',
                'value': cohd_result['dataset_id'],
                'type': 'EDAM:data_1048',  # Database ID
                'source': 'COHD'
            })
        if 'expected_count' in cohd_result:
            attributes.append({
                'name': 'expected_count',
                'value': cohd_result['expected_count'],
                'type': 'EDAM:operation_3438',  # Calculation (not sure if it's correct to use an operation)
                'source': 'COHD'
            })
        # Some properties are handled together as a group based on their type
        for key in ['ln_ratio', 'relative_frequency']:
            if key in cohd_result:
                attributes.append({
                    'name': key,
                    'value': cohd_result[key],
                    'type': 'EDAM:data_1772',  # Score
                    'source': 'COHD'
                })
        for key in ['observed_count',  # From ln_ratio
                    'concept_pair_count', 'concept_2_count',  # From relative_frequency
                    'n', 'n_c1', 'n_c1_c2', 'n_c1_~c2', 'n_c2', 'n_~c1_c2', 'n_~c1_~c2'  # From chi_square
                    ]:
            if key in cohd_result:
                attributes.append({
                    'name': key,
                    'value': cohd_result[key],
                    'type': 'EDAM:data_0006',  # Data
                    'source': 'COHD'
                })

        # Set the knowledge graph edge properties
        kg_edge = {
            'predicate': self._get_kg_predicate(),
            'subject': node_1['primary_curie'],
            'object': node_2['primary_curie'],
            'attributes': attributes
        }

        # Add the new edge
        self._knowledge_graph['edges'][ke_id] = kg_edge

        return kg_node_1, kg_node_2, kg_edge, ke_id

    def _serialize_trapi_response(self,
                                  cohd_results: List[Dict[str, Any]],
                                  status: TrapiStatusCode = TrapiStatusCode.SUCCESS,
                                  logs: Optional[List[str]] = None):
        """ Creates the response message with JSON data in Reasoner Std API format

        Returns
        -------
        Response message with JSON data in Reasoner Std API format
        """
        if cohd_results is not None:
            for result in cohd_results:
                self._add_cohd_result(result, self._criteria)

                # Don't add more than the maximum number of results
                if len(self._results) >= self._max_results:
                    break

        if len(self._results) == 0:
            status = TrapiStatusCode.NO_RESULTS

        response = {
            'status': status.value,
            'description': f'COHD returned {len(self._results)} results.',
            'message': {
                'results': self._results,
                'query_graph': self._query_graph,
                'knowledge_graph': self._knowledge_graph
            },
            # From TRAPI Extended
            'reasoner_id': 'COHD',
            'tool_version': 'COHD 3.0.0',
            'schema_version': '1.0.0',
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'query_options': self._query_options,
        }
        if logs is not None and logs:
            response['logs'] = logs
        return jsonify(response)

    def _trapi_mini_response(self,
                             status: TrapiStatusCode,
                             description: str,
                             logs: Optional[List[str]] = None):
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
            'reasoner_id': 'COHD',
            'tool_version': 'COHD 3.0.0',
            'schema_version': '1.0.0',
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'query_options': self._query_options,
        }
        if logs is not None and logs:
            response['logs'] = logs
        return jsonify(response)
