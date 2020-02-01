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
from omop_xref import ConceptMapper


class COHDTranslatorReasoner:
    """
    Pseudo-reasoner conforming to NCATS Biodmedical Data Translator Reasoner API Spec
    """

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
        self._domain_ids = None
        self._threshold = None
        self._criteria = []
        self._min_cooccurrence = None
        self._confidence_interval = None
        self._concept_mapper = ConceptMapper()
        self._request = request
        self._max_results = 500
        self._local_oxo = True

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

        # Check for the query message
        query_message = self._json_data.get(u'message')
        if query_message is None or not query_message:
            return False, (u'message missing from JSON data or empty', 400)

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
            if query_node[u'id'] == query_node_id:
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
            self._invalid_query_response = (u'Query method "{method}" not supported'.format(method=self._method), 400)

        # Get options that don't fit into query_graph structure from query_options
        self._dataset_id = self._query_options.get(u'dataset_id')

        # Get query information from query_graph
        self._query_graph = self._json_data[u'message'][u'query_graph']
        edge = self._query_graph[u'edges'][0]

        # Get concept_id_1
        source_node = self._find_query_node(edge[u'source_id'])
        curie_1 = source_node[u'curie']  # source node must contain a CURIE
        self._source_concept_mapping = self._concept_mapper.map_to_omop(curie_1)
        if self._source_concept_mapping:
            self._concept_id_1 = self._source_concept_mapping[u'omop_concept_id']

            # Keep track of this mapping in the query_graph for the response
            source_node[u'mapped_omop_concept'] = self._source_concept_mapping
        else:
            self._valid_query = False
            self._invalid_query_response = (u'Could not map source node to OMOP concept', 422)

        # Get the desired association concept or type
        target_node = self._find_query_node(edge[u'target_id'])
        curie_2 = target_node.get(u'curie')
        node_type_2 = target_node.get(u'type')
        if curie_2 is not None and curie_2:
            # If CURIE of target node is specified, then query the association between concept_1 and concept_2
            self._domain_ids = None
            self._target_concept_mapping = self._concept_mapper.map_to_omop(curie_2)

            if self._target_concept_mapping:
                self._concept_id_2 = self._target_concept_mapping[u'omop_concept_id']

                # Keep track of this mapping in the query graph for the response
                target_node[u'mapped_omop_concept'] = self._target_concept_mapping
            else:
                self._valid_query = False
                self._invalid_query_response = (u'Could not map target node to OMOP concept', 422)
        elif node_type_2 is not None and node_type_2:
            # If CURIE is not specified and target node's type is specified, then query the association between
            # concept_1 and all concepts in the domain
            self._concept_id_2 = None
            self._domain_ids = map_blm_class_to_omop_domain(node_type_2)

            # Keep track of this mapping in the query graph for the response
            target_node[u'mapped_omop_domains'] = self._domain_ids
        else:
            # No CURIE or type specified, query for associations against all concepts
            self._concept_id_2 = None
            self._domain_ids = None

        # Get the desired maximum number of results
        max_results = self._json_data.get(u'max_results')
        if max_results:
            self._max_results = min(max_results, self._max_results)  # Don't allow user to specify more than default

        # Get query_option for min_cooccurrence. If not specified, use default of 50
        self._criteria = []
        self._min_cooccurrence = self._query_options.get(u'min_cooccurrence')
        if self._min_cooccurrence is None or not isinstance(self._min_cooccurrence, Number):
            self._min_cooccurrence = default_min_cooccurrence
            self._query_options[u'min_cooccurrence'] = default_min_cooccurrence
        self._criteria.append(ResultCriteria(function=criteria_min_cooccurrence,
                                             kargs={'cooccurrence': self._min_cooccurrence}))

        # Get query_option for threshold. Don't use filter if not specified
        self._threshold = self._query_options.get(u'threshold')
        if self._threshold is not None and isinstance(self._threshold, Number):
            self._criteria.append(ResultCriteria(function=criteria_threshold,
                                                 kargs={'threshold': self._threshold}))

        # Get query_option for confidence_interval. Only for obsExpRatio. If not specified, use default of 0.99.
        self._confidence_interval = self._query_options.get(u'confidence_interval')
        if self._confidence_interval is None or not isinstance(self._confidence_interval, Number):
            self._confidence_interval = default_confidence_interval
            self._query_options[u'confidence_interval'] = default_confidence_interval
        self._criteria.append(ResultCriteria(function=criteria_confidence,
                                             kargs={'alpha': self._confidence_interval}))

        # Get option for local_oxo
        local_oxo = self._query_options.get(u'local_oxo')
        if local_oxo is not None:
            self._local_oxo = local_oxo

        # Get query_option for ontology_targets
        ontology_map = self._query_options.get(u'ontology_targets')
        if ontology_map:
            self._concept_mapper = ConceptMapper(ontology_map, distance=2, local_oxo=self._local_oxo)

        if self._valid_query:
            return True
        else:
            return self._valid_query, self._invalid_query_response

    def reason(self):
        """ Performs the COHD query and reasoning.

        Returns
        -------
        Response message with JSON data in Translator Reasoner API Standard
        """
        # Check if the query is valid
        if self._valid_query:
            if self._concept_id_2 is None and self._domain_ids:
                # Process each of the domains
                results = []
                for domain_id in self._domain_ids:
                    new_results = query_cohd_mysql.query_association(method=self._method,
                                                                     concept_id_1=self._concept_id_1,
                                                                     concept_id_2=self._concept_id_2,
                                                                     dataset_id=self._dataset_id,
                                                                     domain_id=domain_id)
                    if new_results:
                        results.extend(new_results[u'results'])
            else:
                json_results = query_cohd_mysql.query_association(method=self._method, concept_id_1=self._concept_id_1,
                                                                  concept_id_2=self._concept_id_2,
                                                                  dataset_id=self._dataset_id,
                                                                  domain_id=self._domain_ids)
                results = json_results[u'results']

            # Convert results from COHD format to Translator Reasoner standard
            trm = TranslatorResponseMessage(self._query_graph, self._query_options, self._criteria,
                                            results, self._concept_mapper, self._max_results)
            return trm.serialize()
        else:
            # Invalid query. Return the invalid query response
            return self._invalid_query_response


class ResultCriteria:
    """
    Stores a defined criterion to be applied to a COHD result
    """
    def __init__(self, function, kargs):
        """ Constructor

        Parameters
        ----------
        function: function
        kargs: keyword arguments
        """
        self.function = function
        self.kargs = kargs

    def check(self, cohd_result):
        """ Checks if the cohd_result passes the defined criterion

        Parameters
        ----------
        cohd_result: COHD result

        Returns
        -------
        True if passes
        """
        return self.function(cohd_result, **self.kargs)


def criteria_min_cooccurrence(cohd_result, cooccurrence):
    """ Checks that the raw co-occurrence count is >= the specified cooccurrence

    Parameters
    ----------
    cohd_result: COHD result
    cooccurrence: mininum co-occurrence

    Returns
    -------
    True if passes
    """
    if u'n_c1_c2' in cohd_result:
        # chi-square
        return cohd_result[u'n_c1_c2'] >= cooccurrence
    elif u'observed_count' in cohd_result:
        # obsExpRatio
        return cohd_result[u'observed_count'] >= cooccurrence
    elif u'concept_pair_count' in cohd_result:
        # relative frequency
        return cohd_result[u'concept_pair_count'] >= cooccurrence
    else:
        return False


def criteria_threshold(cohd_result, threshold):
    """ Checks that the metric passes the threshold.
    chi-square: p-value < threshold
    observed-expected frequency ratio: ln_ratio >= threshold
    relative frequency: relative_frequency >= threshold
    False for any other types of result

    Parameters
    ----------
    cohd_result
    threshold

    Returns
    -------
    True if passes
    """
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


def criteria_confidence(cohd_result, alpha):
    """ Checks the confidence interval of the result for significance using alpha. Only applies to observed-expected
    frequency ratio. Returns True for all other types of results.

    Parameters
    ----------
    cohd_result
    alpha

    Returns
    -------
    True if significant
    """
    if u'ln_ratio' in cohd_result:
        # obsExpFreq
        ci = ln_ratio_ci(cohd_result[u'observed_count'], cohd_result[u'ln_ratio'], alpha)
        return ci_significance(ci)
    else:
        # Other  methods don't have good way of dealing with confidence interval, so allow the to pass
        return True


class TranslatorResponseMessage:
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
    query_source_node_id = None
    query_target_node_id = None

    def __init__(self, query_graph, query_options, criteria=None, cohd_results=None, concept_mapper=None,
                 max_results=500):
        """ Constructor

        Parameters
        ----------
        query_graph: query_graph from query message
        query_options: query_options from query
        criteria: List of required criteria for cohd_results to be added to list of results
        cohd_results: COHD results
        concept_mapper: ConceptMapper object
        max_results: maximum number of results to add
        """
        self.edges = {}
        self.nodes = {}
        self.results = []
        self.knowledge_graph = {
            u'nodes': [],
            u'edges': []
        }
        self.query_options = query_options
        self.max_results = max_results

        # Mappings from OMOP to other vocabularies / ontologies
        self.concept_mapper = concept_mapper

        # Save info from query graph
        self.query_graph = query_graph
        self.query_edge_id = query_graph[u'edges'][0][u'id']
        self.query_source_node_id = query_graph[u'edges'][0][u'source_id']
        self.query_target_node_id = query_graph[u'edges'][0][u'target_id']

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
            'Translator::KnoweldgeGraph::add_edge() - Bad cohd_result'

        # Check if result passes all filters before adding
        if criteria is not None:
            if not all([c.check(cohd_result) for c in criteria]):
                return

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
        self.add_result(kg_node_1, kg_node_2, kg_edge)

    def add_result(self, kg_node_1, kg_node_2, kg_edge):
        """ Adds a knowledge graph edge to the results list

        Parameters
        ----------
        kg_node_1: Source node
        kg_node_2: Target node
        kg_edge: edge

        Returns
        -------
        result
        """
        result = {
            u'node_bindings': [
                {
                    u'qg_id': self.query_source_node_id,
                    u'kg_id': kg_node_1[u'id']
                },
                {
                    u'qg_id': self.query_target_node_id,
                    u'kg_id': kg_node_2[u'id']
                }
            ],
            u'edge_bindings': [{
                u'qg_id': self.query_edge_id,
                u'kg_id': kg_edge[u'id']
            }]
        }
        self.results.append(result)
        return result

    def get_node(self, concept_id, concept_name=None, domain=None):
        """ Gets the node from internal "graph" representing the OMOP concept. Creates the node if not yet created.
        Node is not added to the knowledge graph or results.

        Parameters
        ----------
        concept_id: OMOP concept ID
        concept_name: OMOP concept name
        domain: OMOP concept domain

        Returns
        -------
        Node for internal use
        """
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

            # Map to Biolink Model or other target ontologies
            mappings = []
            if self.concept_mapper:
                mappings = self.concept_mapper.map_from_omop(concept_id, domain)
            blm_type = map_omop_domain_to_blm_class(domain)

            # Create representations for the knowledge graph node and query node, but don't add them to the graphs yet
            internal_id = u'{id:06d}'.format(id=len(self.nodes))
            node = {
                u'omop_id': concept_id,
                u'name': concept_name,
                u'domain': domain,
                u'internal_id': internal_id,
                u'kg_node': {
                    u'id': omop_concept_curie(concept_id),
                    u'name': concept_name,
                    u'type': blm_type,
                    u'omop_domain': domain,
                    u'synonyms': mappings
                },
                u'in_kgraph': False
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
        kg_node = node[u'kg_node']
        if not node[u'in_kgraph']:
            self.knowledge_graph[u'nodes'].append(kg_node)
            node[u'in_kgraph'] = True

        return kg_node

    def add_kg_edge(self, node_1, node_2, cohd_result):
        """ Adds the edge to the knowledge graph

        Parameters
        ----------
        node_1: Source node
        node_2: Target node
        cohd_result: COHD result - data gets added to edge

        Returns
        -------
        kg_node_1, kg_node_2, kg_edge
        """
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
        """ Creates the response message with JSON data in Reasoner Std API format

        Returns
        -------
        Response message with JSON data in Reasoner Std API format
        """
        return jsonify({
            u'context': u'https://biolink.github.io/biolink-model/context.jsonld',
            u'type': u'translator_reasoner_message',
            u'reasoner_id': u'COHD',
            u'tool_version': u'COHD 2.2.0',
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


mappings_domain_ontology = {
    u'_DEFAULT': [u'ICD9CM', u'RxNorm', u'UMLS', u'DOID', u'MONDO']
}


def map_blm_class_to_omop_domain(node_type):
    """ Maps the Biolink Model class to OMOP domain_id, e.g., 'disease' to 'Condition'

    Note, some classes may map to multiple domains, e.g., 'population of individual organisms' maps to ['Ethnicity',
    'Gender', 'Race']

    Parameters
    ----------
    concept_type - Biolink Model class, e.g., 'disease'

    Returns
    -------
    If normalized successfully: List of OMOP domains, e.g., ['Drug']; Otherwise: None
    """
    mappings = {
        u'device': [u'Device'],
        u'disease': [u'Condition'],
        u'drug': [u'Drug'],
        u'phenomenon': [u'Measurement', u'Observation'],
        u'population of individual organisms': [u'Ethnicity', u'Gender', u'Race'],
        u'procedure': [u'Procedure'],
        # Also map OMOP domains back to themselves
        u'Condition': [u'Condition'],
        u'Device': [u'Device'],
        u'Drug': [u'Drug'],
        u'Ethnicity': [u'Ethnicity'],
        u'Gender': [u'Gender'],
        u'Measurement': [u'Measurement'],
        u'Observation': [u'Observation'],
        u'Procedure': [u'Procedure'],
        u'Race': [u'Race']
    }
    return mappings.get(node_type)


def map_omop_domain_to_blm_class(domain):
    """ Maps the OMOP domain_id to Biolink Model class, e.g., 'Condition' to 'disease'

    Parameters
    ----------
    domain - OMOP domain, e.g., 'Condition'

    Returns
    -------
    If normalized successfully: Biolink Model class
    """
    mappings = {
        u'Condition': u'disease',
        u'Device': u'device',
        u'Drug': u'drug',
        u'Ethnicity': u'population of individual organisms',
        u'Gender': u'population of individual organisms',
        u'Measurement': u'phenomenon',
        u'Observation': u'phenomenon',
        u'Procedure': u'procedure',
        u'Race': u'population of individual organisms'
    }
    return mappings.get(domain)
