"""
Implementation of the NCATS Biodmedical Data Translator Reasoner API Spec
https://github.com/NCATS-Tangerine/NCATS-ReasonerStdAPI/tree/master/API

Current version 0.9.1
"""

import query_cohd_mysql
from datetime import datetime
from flask import jsonify
from numbers import Number
from cohd_utilities import ln_ratio_ci, ci_significance, omop_concept_curie


class COHDTranslatorReasoner:
    def __init__(self, request):
        assert request is not None, 'cohd_translator.py::COHDTranslatorReasoner::__init__() - Bad request'

        self._valid_query = False
        self._invalid_query_response = None
        self._json_data = None
        self._query_graph = None
        self._query_options = None
        self._method = None
        self._concept_id_1 = None
        self._concept_id_2 = None
        self._dataset_id = None
        self._domain_id = None
        self._threshold = None
        self._filters = []
        self._min_cooccurrence = None
        self._confidence_interval = None
        self._request = request

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
            self._invalid_query_response = (u'Missing JSON request body', 400)
            return self._valid_query, self._invalid_query_response

        # Check for query_message
        query_message = self._json_data.get(u'query_message')
        if query_message is None or not query_message:
            return False, (u'query_message missing from JSON data or empty', 400)

        query_graph = query_message.get(u'query_graph')
        if query_graph is None or not query_graph:
            self._valid_query = False
            self._invalid_query_response = (u'query_graph missing from query_message or empty', 400)
            return self._valid_query, self._invalid_query_response

        # Check the structure of the query graph. Should have 2 nodes and 1 edge
        nodes = query_graph.get(u'nodes')
        edges = query_graph.get(u'edges')
        if nodes is None or len(nodes) != 2 or edges is None or len(edges) != 1:
            self._valid_query = False
            self._invalid_query_response = (u'Unsupported query', 400)
            return self._valid_query, self._invalid_query_response

        # Check query_options
        query_options = self._json_data.get(u'query_options')
        if query_options is None or not query_options:
            self._valid_query = False
            self._invalid_query_response = (u'query_options missing from JSON data', 400)
            return self._valid_query, self._invalid_query_response

        # Everything looks good so far
        self._valid_query = True
        self._invalid_query_response = None
        return True, None

    def _find_query_node(self, query_node_id):
        """ Finds the desired query node by node_id from the list of quey nodes in query_graph

        Parameters
        ----------
        query_nodes - List of nodes in query_graph
        query_node_id - node_id of desired node

        Returns
        -------
        query node object, or None if not found
        """
        assert self._query_graph is not None and query_node_id, \
            'cohd_translator.py::COHDTranslatorReasoner::_find_query_nodes()'

        for query_node in self._query_graph[u'nodes']:
            if query_node[u'node_id'] == query_node_id:
                return query_node

        return None

    def _interpret_query(self):
        """ Interprets the JSON request data for how the query should be performed.

        Parameters
        ----------
        query_graph - The query graph (see Reasoner API Standard)

        Returns
        -------
        True if input is valid, otherwise (False, message)
        """
        # Defaults when options are not specified in request body
        default_method = u'obsExpRatio'
        default_min_cooccurrence = 50
        default_confidence_interval = 0.99

        # Check that the query input has the correct structure
        input_check = self._check_query_input()
        if not input_check[0]:
            return input_check

        # Get the query method and check that it matches a supported type
        self._query_options = self._json_data[u'query_options']
        self._method = self._query_options.get(u'method')
        if self._method is None or not self._method:
            self._method = default_method
            self._query_options[u'method'] = default_method
        if self._method.lower() not in [u'relativefrequency', u'obsexpratio', u'chisquare']:
            self._valid_query = False
            self._invalid_query_response = ('Query method "{method}" not supported'.format(method=self._method), 400)

        # Get options that don't fit into query_graph structure from query_options
        self._dataset_id = self._query_options.get(u'dataset_id')

        # Get query information from query_graph
        self._query_graph = self._json_data[u'query_message'][u'query_graph']
        edge = self._query_graph[u'edges'][0]

        # Get concept_id_1
        source_node = self._find_query_node(edge[u'source_id'])
        curie_1 = source_node[u'curie']  # source node must contain a CURIE
        self._concept_id_1 = normalize_curie_to_omop_concept(curie_1)

        # Get the desired association concept or type
        target_node = self._find_query_node(edge[u'target_id'])
        curie_2 = target_node.get(u'curie')
        node_type_2 = target_node.get(u'type')
        if curie_2 is not None and curie_2:
            # If CURIE of target node is specified, then query the association between concept_1 and concept_2
            self._concept_id_2 = normalize_curie_to_omop_concept(curie_2)
            self._domain_id = None
        elif node_type_2 is not None and node_type_2:
            # If CURIE is not specified and target node's type is specified, then query the association between
            # concept_1 and all concepts in the domain
            self._concept_id_2 = None
            self._domain_id = normalize_type_to_omop_domain(node_type_2)
        else:
            # No CURIE or type specified, query for associations against all concepts
            self._concept_id_2 = None
            self._domain_id = None

        # Get query_option for min_cooccurrence. If not specified, use default of 50
        self._filters = []
        self._min_cooccurrence = self._query_options.get(u'min_cooccurrence')
        if self._min_cooccurrence is None or not isinstance(self._min_cooccurrence, Number):
            self._min_cooccurrence = default_min_cooccurrence
            self._query_options[u'min_cooccurrence'] = default_min_cooccurrence
        self._filters.append((COHDTranslatorReasoner.filter_min_cooccurrence, self._min_cooccurrence))

        # Get query_option for threshold. Don't use filter if not specified
        self._threshold = self._query_options.get(u'threshold')
        if self._threshold is not None and isinstance(self._threshold, Number):
            self._filters.append((COHDTranslatorReasoner.filter_threshold, self._threshold))

        # Get query_option for confidence_interval. Only for obsExpRatio. If not specified, use default of 0.99.
        self._confidence_interval = self._query_options.get(u'confidence_interval')
        if self._confidence_interval is None or not isinstance(self._confidence_interval, Number):
            self._confidence_interval = default_confidence_interval
            self._query_options[u'confidence_interval'] = default_confidence_interval
        self._filters.append((COHDTranslatorReasoner.filter_confidence, self._confidence_interval))

        if self._valid_query:
            return True
        else:
            return self._valid_query, self._invalid_query_response

    @staticmethod
    def filter_min_cooccurrence(cohd_result, threshold):
        if u'n_c1_c2' in cohd_result:
            # chi-square
            return cohd_result[u'n_c1_c2'] >= threshold
        elif u'observed_count' in cohd_result:
            # obsExpRatio
            return cohd_result[u'observed_count'] >= threshold
        elif u'concept_pair_count' in cohd_result:
            # relative frequency
            return cohd_result[u'concept_pair_count'] >= threshold
        else:
            return False

    @staticmethod
    def filter_threshold(cohd_result, threshold):
        if u'p-value' in cohd_result:
            # chi-square
            return cohd_result[u'p-value'] < threshold
        elif u'ln_ratio' in cohd_result:
            # obsExpRatio
            return abs(cohd_result[u'ln_ratio']) >= threshold
        elif u'relative_frequency' in cohd_result:
            # relative frequency
            return cohd_result[u'relative_frequency'] >= threshold
        else:
            return False

    @staticmethod
    def filter_confidence(cohd_result, alpha):
        if u'ln_ratio' in cohd_result:
            # obsExpFreq
            ci = ln_ratio_ci(cohd_result[u'observed_count'], cohd_result[u'ln_ratio'], alpha)
            return ci_significance(ci)
        else:
            # Other  methods don't have good way of dealing with confidence interval, so allow the to apss
            return True

    def reason(self):
        # Check if the query is valid
        if self._valid_query:
            # Get the association results
            json_results = query_cohd_mysql.query_association(method=self._method, concept_id_1=self._concept_id_1,
                                                              concept_id_2=self._concept_id_2,
                                                              dataset_id=self._dataset_id, domain_id=self._domain_id)

            # Convert results from COHD format to Translator Reasoner standard
            trm = TranslatorResponseMessage(self._query_graph, self._query_options, self._filters, json_results[u'results'])
            return trm.serialize()
        else:
            # Invalid query. Return the invalid query response
            return self._invalid_query_response


class TranslatorResponseMessage:
    edges = None
    nodes = None
    results = None
    query_graph = None
    query_options = None
    knowledge_graph = None
    query_edge_id = None
    query_source_node_id = None
    query_target_node_id = None

    def __init__(self, query_graph, query_options, filters=None, cohd_results=None):
        self.edges = {}
        self.nodes = {}
        self.results = []
        self.knowledge_graph = {
            u'nodes': [],
            u'edges': []
        }
        self.query_options = query_options

        # Save info from query graph
        self.query_graph = query_graph
        self.query_edge_id = query_graph[u'edges'][0][u'edge_id']
        self.query_source_node_id = query_graph[u'edges'][0][u'source_id']
        self.query_target_node_id = query_graph[u'edges'][0][u'target_id']

        if cohd_results is not None:
            for result in cohd_results:
                self.add_cohd_result(result, filters)

    def add_cohd_result(self, cohd_result, filters):
        assert cohd_result is not None and 'concept_id_1' in cohd_result and 'concept_id_2' in cohd_result, \
            'Translator::KnoweldgeGraph::add_edge() - Bad cohd_result'

        # Add node for concept 1
        concept_1_id = cohd_result[u'concept_id_1']
        node_1 = self.get_node(concept_1_id)

        # Add node for concept 2
        concept_2_id = cohd_result[u'concept_id_2']
        concept_2_name = cohd_result.get(u'concept_2_name')
        concept_2_domain = cohd_result.get(u'concept_2_domain')
        node_2 = self.get_node(concept_2_id, concept_2_name, concept_2_domain)

        # Add to knowledge graph
        kg_node_1, kg_node_2, kg_edge = self.add_kg_edge(node_1, node_2, cohd_result)

        # Add to results
        if filters is not None:
            # Check if result passes all filters before adding to results
            if all([f[0](cohd_result, f[1]) for f in filters]):
                self.add_result(kg_node_1, kg_node_2, kg_edge)
        else:
            self.add_result(kg_node_1, kg_node_2, kg_edge)

    def add_result(self, kg_node_1, kg_node_2, kg_edge):
        self.results.append({
            u'node_bindings': {
                self.query_source_node_id: kg_node_1[u'id'],
                self.query_target_node_id: kg_node_2[u'id'],
            },
            u'edge_bindigns': {
                self.query_edge_id: kg_edge[u'id']
            }
        })

    def get_node(self, concept_id, concept_name=None, domain=None):
        node = self.nodes.get(concept_id)

        if node is None:
            # Create the node
            if concept_name is None or type is None:
                # Concept information not specified, lookup concept definition
                concept_name = u''
                domain = u''
                concept_def = query_cohd_mysql.omop_concept_definition(concept_id)

                if concept_def is not None and not concept_name:
                    concept_name = concept_def[u'concept_name']
                if concept_def is not None and not domain:
                    domain = concept_def[u'domain_id']

            # Create representations for the knowledge graph node and query node, but don't add them to the graphs yet
            # TODO: Update to BioLink Model
            internal_id = u'{id:06d}'.format(id=len(self.nodes))
            node = {
                u'omop_id': concept_id,
                u'name': concept_name,
                u'domain': domain,
                u'internal_id': internal_id,
                u'kg_node': {
                    u'id': omop_concept_curie(concept_id),
                    u'name': concept_name,
                    u'type': domain,
                },
                u'in_kgraph': False
            }
            self.nodes[concept_id] = node

        return node

    def add_kg_node(self, node):
        kg_node = node[u'kg_node']
        if not node[u'in_kgraph']:
            self.knowledge_graph[u'nodes'].append(kg_node)
            node[u'in_kgraph'] = True

        return kg_node

    def add_kg_edge(self, node_1, node_2, cohd_result):
        # Add nodes to knowledge graph
        kg_node_1 = self.add_kg_node(node_1)
        kg_node_2 = self.add_kg_node(node_2)

        # Create the new edge as a copy of the COHD result so that the edge automatically has all the data
        kg_edge = cohd_result.copy()

        # Mint a new identifier
        ke_id = 'ke{id:06d}'.format(id=len(self.knowledge_graph[u'edges']))

        # Set the knowledge graph edge properties
        kg_edge.update({
            u'id': ke_id,
            u'type': 'association',
            u'source_id': omop_concept_curie(node_1[u'omop_id']),
            u'target_id': omop_concept_curie(node_2[u'omop_id'])
        })

        # Add the new edge
        self.knowledge_graph[u'edges'].append(kg_edge)

        return kg_node_1, kg_node_2, kg_edge

    def serialize(self):
        return jsonify({
            u'type': u'translator_reasoner_message',
            u'reasoner_id': u'COHD',
            u'tool_version': u'COHD 2.1.0',
            u'schema_version': u'0.9.1',
            u'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            u'n_results': len(self.results),
            u'message_code': u'OK',
            u'code_description': u'{n} result(s) found'.format(n=len(self.results)),
            u'query_options': self.query_options,
            u'results': self.results,
            u'query_graph': self.query_graph,
            u'knowledge_graph': self.knowledge_graph
        })


def normalize_curie_to_omop_concept(curie):
    """ Normalizes a CURIE to OMOP concept ID

    Parameters
    ----------
    curie - CURIE, e.g, "OMOP:313217" or "OMIM:603903"

    Returns
    -------
    If normalized successfully: OMOP Concept ID, e.g., "313217"; Otherwise: None
    """
    # For now, only supporting OMOP CURIEs
    if curie[0:5] == u'OMOP:':
        return curie[5:]

    return None


def normalize_type_to_omop_domain(concept_type):
    """ Normalizes the node type to OMOP domain_id

    Parameters
    ----------
    concept_type - node type, e.g., 'Drug'

    Returns
    -------
    If normalized successfully: OMOP Domain ID, e.g., 'Drug'; Otherwise: None
    """
    # Not yet implemented
    return concept_type


