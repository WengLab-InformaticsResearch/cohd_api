from datetime import datetime
from numbers import Number

from flask import jsonify

from . import query_cohd_mysql
from .cohd_utilities import omop_concept_curie
from .cohd_trapi import *


class CohdTrapi100(CohdTrapi):
    """
    Pseudo-reasoner conforming to NCATS Biodmedical Data Translator Reasoner API Spec 1.0
    """

    def __init__(self, request):
        super().__init__(request)

        self._valid_query = False
        self._invalid_query_response = None
        self._json_data = None
        self._query_graph = None
        self._query_options = None
        self._method = None
        self._concept_id_1 = None
        self._concept_id_2 = None
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

        # # Check query_options
        # query_options = self._json_data.get(u'query_options')
        # if query_options is None or not query_options:
        #     self._valid_query = False
        #     self._invalid_query_response = (u'query_options missing from JSON data', 400)
        #     return self._valid_query, self._invalid_query_response

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

    def _interpret_query(self):
        """ Interprets the JSON request data for how the query should be performed.

        Parameters
        ----------
        query_graph - The query graph (see Reasoner API Standard)

        Returns
        -------
        True if input is valid, otherwise (False, message)
        """
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
            self._invalid_query_response = ('COHD reasoner only supports 1-hop queries', 422)
            return self._valid_query, self._invalid_query_response

        # Check if the edge type is supported by COHD Reasoner
        edge = list(edges.values())[0]  # Get first and only edge
        if 'predicate' in edge:
            edge_predicates = edge['predicate']
            if not isinstance(edge_predicates, list):
                # In TRAPI, QEdge type can be string or list. If it's not currently a list, convert to a list to
                # simplify following processing
                edge_predicates = [edge_predicates]
            edge_supported = False
            for edge_predicate in edge_predicates:
                if edge_predicate in CohdTrapi.supported_edge_types:
                    edge_supported = True
                    break
            if not edge_supported:
                self._valid_query = False
                self._invalid_query_response = ('QEdge.predicate not supported by COHD Reasoner API', 422)
                return self._valid_query, self._invalid_query_response
        else:
            # TRAPI does not require predicate. If no predicate specified, assume using biolink:correlated_with
            pass

        # Find BLM - OMOP mappings for all identified query nodes
        node_ids = [x['id'] for x in self._query_graph['nodes'].values() if 'id' in x and x['id'] is not None]
        node_mappings = self._concept_mapper.map_to_omop(node_ids)

        # Get concept_id_1
        subject_node = self._find_query_node(edge['subject'])
        curie_1 = subject_node['id']  # source node must contain a CURIE
        self._source_concept_mapping = node_mappings[curie_1]
        if self._source_concept_mapping:
            self._concept_id_1 = self._source_concept_mapping['omop_concept_id']

            # Keep track of this mapping in the query_graph for the response
            subject_node['mapped_omop_concept'] = self._source_concept_mapping
        else:
            self._valid_query = False
            self._invalid_query_response = ('Could not map source node to OMOP concept', 422)
            return self._valid_query, self._invalid_query_response

        # Check the formatting of node 1's category (even though it's not used)
        if 'category' in subject_node:
            subject_node['category'] = fix_blm_category(subject_node['category'])
            # OMOP conditions are better represented as DiseaseOrPhenotypicFeature than as Disease. Change the query
            if subject_node['category'] == 'biolink:Disease':
                subject_node['category'] = 'biolink:DiseaseOrPhenotypicFeature'

        # Get the desired association concept or category
        object_node = self._find_query_node(edge['object'])
        curie_2 = object_node.get('id')
        node_category_2 = object_node.get('category')
        # OMOP conditions are better represented as DiseaseOrPhenotypicFeature than as Disease. Change the query
        if node_category_2 == 'biolink:Disease':
            node_category_2 = 'biolink:DiseaseOrPhenotypicFeature'

        if curie_2 is not None and curie_2:
            # If CURIE of target node is specified, then query the association between concept_1 and concept_2
            self._domain_class_pairs = None
            self._target_concept_mapping = node_mappings[curie_2]

            if self._target_concept_mapping:
                self._concept_id_2 = self._target_concept_mapping['omop_concept_id']

                # Keep track of this mapping in the query graph for the response
                object_node['mapped_omop_concept'] = self._target_concept_mapping
            else:
                self._valid_query = False
                self._invalid_query_response = ('Could not map target node to OMOP concept', 422)
                return self._valid_query, self._invalid_query_response
        elif node_category_2 is not None and node_category_2:
            # Attempt to correct the node type if necessary
            node_category_2 = fix_blm_category(node_category_2)
            object_node['category'] = node_category_2

            # If CURIE is not specified and target node's category is specified, then query the association
            # between concept_1 and all concepts in the domain
            self._concept_id_2 = None
            self._domain_class_pairs = map_blm_class_to_omop_domain(node_category_2)

            # Keep track of this mapping in the query graph for the response
            object_node['mapped_omop_domains'] = [x.domain_id for x in self._domain_class_pairs]
            object_node['mapped_omop_classes'] = [x.concept_class_id for x in self._domain_class_pairs]
        else:
            # No CURIE or type specified, query for associations against all concepts
            self._concept_id_2 = None
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
                                                 kargs={'alpha': self._confidence_interval}))

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
            if self._concept_id_2 is None and self._domain_class_pairs:
                # Node 2 not specified, but node 2's type was specified. Query associations between Node 1 and the
                # requested types (domains)
                results = []
                for domain_id, concept_class_id in self._domain_class_pairs:
                    new_results = query_cohd_mysql.query_association(method=self._method,
                                                                     concept_id_1=self._concept_id_1,
                                                                     concept_id_2=self._concept_id_2,
                                                                     dataset_id=self._dataset_id,
                                                                     domain_id=domain_id,
                                                                     concept_class_id=concept_class_id,
                                                                     confidence=self._confidence_interval)
                    if new_results:
                        results.extend(new_results['results'])
            else:
                # Either Node 2 was specified by a CURIE or no type (domain) was specified for type 2. Query the
                # associations between Node 1 and Node 2 or between Node 1 and all domains
                json_results = query_cohd_mysql.query_association(method=self._method, concept_id_1=self._concept_id_1,
                                                                  concept_id_2=self._concept_id_2,
                                                                  dataset_id=self._dataset_id,
                                                                  domain_id=None,
                                                                  confidence=self._confidence_interval)
                results = json_results['results']

            # Convert results from COHD format to Translator Reasoner standard
            trm = _TranslatorResponseMessage(self._query_graph, self._query_options, self._criteria,
                                             results, self._concept_mapper, self._max_results, self._biolink_only)
            return trm.serialize()
        else:
            # Invalid query. Return the invalid query response
            return self._invalid_query_response


class _TranslatorResponseMessage:
    """
    Creates a response message conforming to the NCATS Translator Reasoner API Standard
    """
    edges = None
    nodes = None
    results = None
    query_graph = None
    query_options = None
    knowledge_graph = None
    query_edge_id = None
    query_subject_node_id = None
    query_object_node_id = None

    def __init__(self, query_graph, query_options, criteria=None, cohd_results=None, concept_mapper=None,
                 max_results=500, biolink_only=True):
        """ Constructor

        Parameters
        ----------
        query_graph: query_graph from query message
        query_options: query_options from query
        criteria: List of required criteria for cohd_results to be added to list of results
        cohd_results: COHD results
        concept_mapper: ConceptMapper object
        max_results: maximum number of results to add
        biolink_only: True to only allow nodes that map to biolink
        """
        self.edges = {}
        self.nodes = {}
        self.results = []
        self.knowledge_graph = {
            'nodes': {},
            'edges': {}
        }
        self.query_options = query_options
        self.max_results = max_results
        self.biolink_only = biolink_only

        # Mappings from OMOP to other vocabularies / ontologies
        self.concept_mapper = concept_mapper

        # Save info from query graph
        self.query_graph = query_graph
        for query_edge_id, query_edge in query_graph['edges'].items():
            self.query_edge_id = query_edge_id
            self.query_edge_predicate = query_edge['predicate']
            self.query_subject_node_id = query_edge['subject']
            self.query_object_node_id = query_edge['object']
            break  # one and only one edge expected

        # Get the input CURIEs from the query graph
        self.query_subject_node_curie = self.query_graph['nodes'][self.query_subject_node_id].get('id', None)
        self.query_subject_node_categories = self.query_graph['nodes'][self.query_subject_node_id].get('category', None)
        self.query_object_node_curie = self.query_graph['nodes'][self.query_object_node_id].get('id', None)
        self.query_object_node_categories = self.query_graph['nodes'][self.query_object_node_id].get('category', None)

        if cohd_results is not None:
            for result in cohd_results:
                self.add_cohd_result(result, criteria)

                # Don't add more than the maximum number of results
                if len(self.results) >= self.max_results:
                    break

    def add_cohd_result(self, cohd_result, criteria):
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
        node_1 = self.get_node(concept_1_id, query_node_curie=self.query_subject_node_curie,
                               query_node_categories=self.query_subject_node_categories)

        if self.biolink_only and not node_1.get('biolink_compliant', False):
            # Only include results when node_1 maps to biolink
            return

        # Get node for concept 2
        concept_2_id = cohd_result['concept_id_2']
        concept_2_name = cohd_result.get('concept_2_name')
        concept_2_domain = cohd_result.get('concept_2_domain')
        node_2 = self.get_node(concept_2_id, concept_2_name, concept_2_domain, self.query_object_node_curie,
                               query_node_categories=self.query_object_node_categories)

        if self.biolink_only and not node_2.get('biolink_compliant', False):
            # Only include results when node_2 maps to biolink
            return

        # Add nodes and edge to knowledge graph
        kg_node_1, kg_node_2, kg_edge, kg_edge_id = self.add_kg_edge(node_1, node_2, cohd_result)

        # Add to results
        self.add_result(node_1['primary_curie'], node_2['primary_curie'], kg_edge_id)

    def add_result(self, kg_node_1_id, kg_node_2_id, kg_edge_id):
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
                self.query_subject_node_id: [{
                    'id': kg_node_1_id
                }],
                self.query_object_node_id: [{
                    'id': kg_node_2_id
                }]
            },
            'edge_bindings': {
                self.query_edge_id: [{
                    'id': kg_edge_id
                }]
            }
        }
        self.results.append(result)
        return result

    def get_node(self, concept_id, concept_name=None, domain=None, concept_class=None, query_node_curie=None,
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
        node = self.nodes.get(concept_id)

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
            if self.concept_mapper:
                mapping = self.concept_mapper.map_from_omop(concept_id, blm_category)

            if query_node_curie is not None and query_node_curie:
                # The CURIE was specified for this node in the query_graph, use that CURIE to identify this node
                mapped_to_blm = True
                primary_curie = query_node_curie
                # Find the label from the mappings
                if mapping is not None:
                    primary_label = mapping['target_label']
            elif mapping is not None:
                # Choose one of the mappings to be the main identifier for the node. Prioritize distance first, and then
                # choose by the order of prefixes listed in the Concept Mapper. If no biolink prefix found, use OMOP
                primary_curie = mapping['target_curie']
                primary_label = mapping['target_label']
                mapped_to_blm = True

            # Create representations for the knowledge graph node and query node, but don't add them to the graphs yet
            internal_id = '{id:06d}'.format(id=len(self.nodes))
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
            self.nodes[concept_id] = node

        return node

    def add_kg_node(self, node):
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
            self.knowledge_graph['nodes'][node['primary_curie']] = kg_node
            node['in_kgraph'] = True

        return kg_node

    def add_kg_edge(self, node_1, node_2, cohd_result):
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
        kg_node_1 = self.add_kg_node(node_1)
        kg_node_2 = self.add_kg_node(node_2)

        # Mint a new identifier
        ke_id = 'ke{id:06d}'.format(id=len(self.knowledge_graph['edges']))

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
            'predicate': self.query_edge_predicate,
            'subject': node_1['primary_curie'],
            'object': node_2['primary_curie'],
            'attributes': attributes
        }

        # Add the new edge
        self.knowledge_graph['edges'][ke_id] = kg_edge

        return kg_node_1, kg_node_2, kg_edge, ke_id

    def serialize(self):
        """ Creates the response message with JSON data in Reasoner Std API format

        Returns
        -------
        Response message with JSON data in Reasoner Std API format
        """
        return jsonify({
            # 'status': '', # TODO: String: One of a standardized set of short codes, e.g. Success,
            #                       QueryNotTraversable, KPsNotAvailable
            # 'description': '', # TODO: String: A brief human-readable description of the outcome
            # 'logs': [],  # TODO: Array of LogEntry objects
            'message': {
                'results': self.results,
                'query_graph': self.query_graph,
                'knowledge_graph': self.knowledge_graph
            },
            # From TRAPI Extended
            'reasoner_id': 'COHD',
            'tool_version': 'COHD 3.0.0',
            'schema_version': '1.0.0',
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'query_options': self.query_options,
        })
