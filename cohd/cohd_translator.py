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
        self._concept_mapper = BiolinkConceptMapper()
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
        default_min_cooccurrence = 0
        default_confidence_interval = 0.99
        default_dataset_id = 3
        default_local_oxo = True
        default_mapping_distance = 3
        default_biolink_only = True

        # Check that the query input has the correct structure
        input_check = self._check_query_input()
        if not input_check[0]:
            return input_check

        # Get options that don't fit into query_graph structure from query_options
        self._query_options = self._json_data.get(u'query_options')
        if self._query_options is None:
            # No query options provided. Get default options for all query options (below)
            self._query_options = dict()

        # Get the query method and check that it matches a supported type
        self._method = self._query_options.get(u'method')
        if self._method is None or not self._method or not isinstance(self._method, str):
            self._method = default_method
            self._query_options[u'method'] = default_method
        else:
            if self._method not in [u'relativeFrequency', u'obsExpRatio', u'chiSquare']:
                self._valid_query = False
                self._invalid_query_response = (u'Query method "{method}" not supported'.format(method=self._method),
                                                400)

        # Get the query_option for dataset ID
        self._dataset_id = self._query_options.get(u'dataset_id')
        if self._dataset_id is None or not self._dataset_id or not isinstance(self._dataset_id, Number):
            self._dataset_id = default_dataset_id
            self._query_options[u'dataset_id'] = default_dataset_id

        # Get the query_option for minimum co-occurrence
        self._min_cooccurrence = self._query_options.get(u'min_cooccurrence')
        if self._min_cooccurrence is None or not isinstance(self._min_cooccurrence, Number):
            self._min_cooccurrence = default_min_cooccurrence
            self._query_options[u'min_cooccurrence'] = default_min_cooccurrence

        # Get the query_option for confidence_interval. Only used for obsExpRatio. If not specified, use default.
        self._confidence_interval = self._query_options.get(u'confidence_interval')
        if self._confidence_interval is None or not isinstance(self._confidence_interval, Number) or \
                self._confidence_interval < 0 or self._confidence_interval >= 1:
            self._confidence_interval = default_confidence_interval
            self._query_options[u'confidence_interval'] = default_confidence_interval

        # Get the query_option for local_oxo
        self._local_oxo = self._query_options.get(u'local_oxo')
        if self._local_oxo is None or not isinstance(self._local_oxo, bool):
            self._local_oxo = default_local_oxo

        # Get the query_option for maximum mapping distance
        self._mapping_distance = self._query_options.get(u'mapping_distance')
        if self._mapping_distance is None or not isinstance(self._mapping_distance, Number):
            self._mapping_distance = default_mapping_distance

        # Get query_option for ontology_targets
        ontology_map = self._query_options.get(u'ontology_targets')
        if ontology_map and isinstance(ontology_map, dict):
            self._concept_mapper = BiolinkConceptMapper(ontology_map, distance=self._mapping_distance,
                                                        local_oxo=self._local_oxo)
        else:
            # Use default ontology map
            self._concept_mapper = BiolinkConceptMapper(distance=self._mapping_distance, local_oxo=self._local_oxo)

        # Get query_option for including only Biolink nodes
        self._biolink_only = self._query_options.get(u'biolink_only')
        if self._biolink_only is None or not isinstance(self._biolink_only, bool):
            self._biolink_only = default_biolink_only

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

        # Criteria for returning results
        self._criteria = []

        # Add a criterion for minimum co-occurrence
        if self._min_cooccurrence > 0:
            self._criteria.append(ResultCriteria(function=criteria_min_cooccurrence,
                                                 kargs={'cooccurrence': self._min_cooccurrence}))

        # Get query_option for threshold. Don't use filter if not specified (i.e., no default option for threshold)
        self._threshold = self._query_options.get(u'threshold')
        if self._threshold is not None and isinstance(self._threshold, Number):
            self._criteria.append(ResultCriteria(function=criteria_threshold,
                                                 kargs={'threshold': self._threshold}))

        # If the method is obsExpRatio, add a criteria for confidence interval
        if self._method == u'obsexpratio' and self._confidence_interval > 0:
            self._criteria.append(ResultCriteria(function=criteria_confidence,
                                                 kargs={'alpha': self._confidence_interval}))

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
                # Node 2 not specified, but node 2's type was specified. Query associations between Node 1 and the
                # requested types (domains)
                results = []
                for domain_id in self._domain_ids:
                    new_results = query_cohd_mysql.query_association(method=self._method,
                                                                     concept_id_1=self._concept_id_1,
                                                                     concept_id_2=self._concept_id_2,
                                                                     dataset_id=self._dataset_id,
                                                                     domain_id=domain_id,
                                                                     confidence=self._confidence_interval)
                    if new_results:
                        results.extend(new_results[u'results'])
            else:
                # Either Node 2 was specified by a CURIE or no type (domain) was specified for type 2. Query the
                # associations between Node 1 and Node 2 or between Node 1 and all domains
                json_results = query_cohd_mysql.query_association(method=self._method, concept_id_1=self._concept_id_1,
                                                                  concept_id_2=self._concept_id_2,
                                                                  dataset_id=self._dataset_id,
                                                                  domain_id=None,
                                                                  confidence=self._confidence_interval)
                results = json_results[u'results']

            # Convert results from COHD format to Translator Reasoner standard
            trm = TranslatorResponseMessage(self._query_graph, self._query_options, self._criteria,
                                            results, self._concept_mapper, self._max_results, self._biolink_only)
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
        if threshold >= 0:
            return cohd_result[u'ln_ratio'] >= threshold
        else:
            return cohd_result[u'ln_ratio'] <= threshold
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
        # relativeFrequency doesn't have a good cutoff for confidence interval, and chiSquare uses
        # p-value for signficance, so allow methods other than obsExpRatio to pass
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
            u'nodes': [],
            u'edges': []
        }
        self.query_options = query_options
        self.max_results = max_results
        self.biolink_only = biolink_only

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
            'Translator::KnowledgeGraph::add_edge() - Bad cohd_result'

        # Check if result passes all filters before adding
        if criteria is not None:
            if not all([c.check(cohd_result) for c in criteria]):
                return

        # Get node for concept 1
        concept_1_id = cohd_result[u'concept_id_1']
        node_1 = self.get_node(concept_1_id)

        if self.biolink_only and not node_1.get(u'biolink_compliant', False):
            # Only include results when node_1 maps to biolink
            return

        # Get node for concept 2
        concept_2_id = cohd_result[u'concept_id_2']
        concept_2_name = cohd_result.get(u'concept_2_name')
        concept_2_domain = cohd_result.get(u'concept_2_domain')
        node_2 = self.get_node(concept_2_id, concept_2_name, concept_2_domain)

        if self.biolink_only and not node_2.get(u'biolink_compliant', False):
            # Only include results when node_2 maps to biolink
            return

        # Add nodes and edge to knowledge graph
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
            if concept_name is None or domain is None:
                # Concept information not specified, lookup concept definition
                concept_name = concept_name if concept_name is not None else u''
                domain = domain if domain is not None else u''
                concept_def = query_cohd_mysql.omop_concept_definition(concept_id)

                if concept_def is not None and not concept_name:
                    concept_name = concept_def[u'concept_name']
                if concept_def is not None and not domain:
                    domain = concept_def[u'domain_id']

            # Map to Biolink Model or other target ontologies
            blm_type = map_omop_domain_to_blm_class(domain)
            mappings = []
            if self.concept_mapper:
                mappings = self.concept_mapper.map_from_omop(concept_id, blm_type)

            # Choose one of the mappings to be the main identifier for the node. Prioritize distance first, and then
            # choose by the order of prefixes listed in the Concept Mapper. If no biolink prefix found, use OMOP
            omop_curie = omop_concept_curie(concept_id)
            primary_curie = omop_curie
            primary_label = concept_name
            blm_prefixes = self.concept_mapper.biolink_mappings.get(blm_type, [])
            found = False
            for d in range(self.concept_mapper.distance + 1):
                if found:
                    break

                # Get all mappings with the current distance
                m_d = [m for m in mappings if m[u'distance'] == d]

                # Look for the first matching prefix in the list of biolink prefixes
                for prefix in blm_prefixes:
                    if found:
                        break

                    for m in m_d:
                        if m[u'target_curie'].split(u':')[0] == prefix:
                            primary_curie = m[u'target_curie']
                            primary_label = m[u'target_label']
                            found = True
                            break

            # Create representations for the knowledge graph node and query node, but don't add them to the graphs yet
            internal_id = u'{id:06d}'.format(id=len(self.nodes))
            node = {
                u'omop_id': concept_id,
                u'name': concept_name,
                u'domain': domain,
                u'internal_id': internal_id,
                u'kg_node': {
                    u'id': primary_curie,
                    u'name': primary_label,
                    u'type': [blm_type],
                    u'attributes': [
                        {
                            u'name': u'omop_concept_id',
                            u'value': omop_curie,
                            u'type': u'EDAM:data_1087',  # Ontology concept ID
                            u'source': u'OMOP',
                        },
                        {
                            u'name': u'omop_concept_name',
                            u'value': concept_name,
                            u'type': u'EDAM:data_2339',  # Ontology concept name
                            u'source': u'OMOP',
                        },
                        {
                            u'name': u'omop_domain',
                            u'value': domain,
                            u'type': u'EDAM:data_0967',  # Ontology concept data
                            u'source': u'OMOP',
                        },
                        {
                            u'name': u'synonyms',
                            u'value': mappings,
                            u'type': u'EDAM:data_3509',  # Ontology mapping
                            u'source': u'COHD',
                        }
                    ]
                },
                u'in_kgraph': False,
                u'biolink_compliant': found
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

        # Mint a new identifier
        ke_id = 'ke{id:06d}'.format(id=len(self.knowledge_graph[u'edges']))

        # Add properties from COHD results to the edge attributes
        attributes = list()
        if u'p-value' in cohd_result:
            attributes.append({
                u'name': u'p-value',
                u'value': cohd_result[u'p-value'],
                u'type': u'EDAM:data_1669',  # P-value
                u'url': u'http://edamontology.org/data_1669',
                u'source': u'COHD'
            })
        if u'confidence_interval' in cohd_result:
            attributes.append({
                u'name': u'confidence_interval',
                u'value': cohd_result[u'confidence_interval'],
                u'type': u'EDAM:data_0951',  # Statistical estimate score
                u'source': u'COHD'
            })
        if u'dataset_id' in cohd_result:
            attributes.append({
                u'name': u'dataset_id',
                u'value': cohd_result[u'dataset_id'],
                u'type': u'EDAM:data_1048',  # Database ID
                u'source': u'COHD'
            })
        if u'expected_count' in cohd_result:
            attributes.append({
                u'name': u'expected_count',
                u'value': cohd_result[u'expected_count'],
                u'type': u'EDAM:operation_3438',  # Calculation (not sure if it's correct to use an operation)
                u'source': u'COHD'
            })
        # Some properties are handled together as a group based on their type
        for key in [u'ln_ratio', u'relative_frequency']:
            if key in cohd_result:
                attributes.append({
                    u'name': key,
                    u'value': cohd_result[key],
                    u'type': u'EDAM:data_1772',  # Score
                    u'source': u'COHD'
                })
        for key in [u'observed_count',  # From ln_ratio
                    u'concept_pair_count', u'concept_2_count',  # From relative_frequency
                    u'n', u'n_c1', u'n_c1_c2', u'n_c1_~c2', u'n_c2', u'n_~c1_c2', u'n_~c1_~c2'  # From chi_square
                    ]:
            if key in cohd_result:
                attributes.append({
                    u'name': key,
                    u'value': cohd_result[key],
                    u'type': u'EDAM:data_0006',  # Data
                    u'source': u'COHD'
                })

        # Set the knowledge graph edge properties
        kg_edge = {
            u'id': ke_id,
            u'type': 'association',
            u'source_id': omop_concept_curie(node_1[u'omop_id']),
            u'target_id': omop_concept_curie(node_2[u'omop_id']),
            u'attributes': attributes
        }

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
            u'schema_version': u'0.9.2',
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
        u'population_of_individual_organisms': [u'Ethnicity', u'Gender', u'Race'],
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
    """ Maps the OMOP domain_id to Biolink Model class, e.g., 'Condition' to 'Disease'

    Parameters
    ----------
    domain - OMOP domain, e.g., 'Condition'

    Returns
    -------
    If normalized successfully: Biolink Model semantic type. If no mapping found, use NamedThing
    """
    mappings = {
        u'Condition': u'disease',
        u'Device': u'device',
        u'Drug': u'drug',
        u'Ethnicity': u'population_of_individual_organisms',
        u'Gender': u'population_of_individual_organisms',
        u'Measurement': u'phenomenon',
        u'Observation': u'phenomenon',
        u'Procedure': u'procedure',
        u'Race': u'population_of_individual_organisms'
    }
    default_type = u'named_thing'
    return mappings.get(domain, default_type)


class BiolinkConceptMapper:
    """ Maps between OMOP concepts and Biolink Model

    When mapping from OMOP conditions to Biolink Model diseases, since SNOMED-CT, ICD10CM, ICD9CM, and MedDRA are now
    included in Biolink Model, map to these source vocabularies using the OMOP concept definitions. When mapping from
    other ontologies to OMOP, leverage the OxO as well.
    """

    _mappings_prefixes_blm_to_oxo = {
        # Disease prefixes
        u'MONDO': u'MONDO',
        u'DOID': u'DOID',
        u'OMIM': u'OMIM',
        u'ORPHANET': u'Orphanet',
        u'ORPHA': None,
        u'EFO': u'EFO',
        u'UMLS': u'UMLS',
        u'MESH': u'MeSH',
        u'MEDDRA': u'MedDRA',
        u'NCIT': u'NCIT',
        u'SNOMEDCT': u'SNOMEDCT',
        u'medgen': None,
        u'ICD10': u'ICD10CM',
        u'ICD9': u'ICD9CM',
        u'ICD0': None,
        u'HP': u'HP',
        u'MP': u'MP',
        # Drug prefixes
        u'PHARMGKB.DRUG': None,
        u'CHEBI': u'CHEBI',
        u'CHEMBL.COMPOUND': None,
        u'DRUGBANK': u'DrugBank',
        u'PUBCHEM.COMPOUND': u'PubChem_Compound',
        u'HMDB': u'HMDB',
        u'INCHI': None,
        u'UNII': None,
        u'KEGG': u'KEGG',
        u'gtpo': None,
        # Procedure prefixes
        u'ICD10PCS': None
    }

    _mappings_prefixes_oxo_to_blm = {
        # Disease prefixes
        u'MONDO': u'MONDO',
        u'DOID': u'DOID',
        u'OMIM': u'OMIM',
        u'Orphanet': u'ORPHANET',
        u'EFO': u'EFO',
        u'UMLS': u'UMLS',
        u'MeSH': u'MESH',
        u'MedDRA': u'MEDDRA',
        u'NCIT': u'NCIT',
        u'SNOMEDCT': u'SNOMEDCT',
        u'ICD10CM': u'ICD10',
        u'ICD9CM': u'ICD9',
        u'HP': u'HP',
        u'MP': u'MP',
        # Drug prefixes
        u'CHEBI': u'CHEBI',
        u'DrugBank': u'DRUGBANK',
        u'PubChem_Compound': u'PUBCHEM.COMPOUND',
        u'HMDB': u'HMDB',
        u'KEGG': u'KEGG',
        # Procedure prefixes
    }

    _default_ontology_map = {
        u'disease': [u'MONDO', u'DOID', u'OMIM', u'ORPHANET', u'ORPHA', u'EFO', u'UMLS', u'MESH', u'MEDDRA',
                       u'NCIT', u'SNOMEDCT', u'medgen', u'ICD10', u'ICD9', u'ICD0', u'HP', u'MP'],
        # Note: for Drug, also map to some of the prefixes specified in ChemicalSubstance
        u'drug': [u'PHARMGKB.DRUG', u'CHEBI', u'CHEMBL.COMPOUND', u'DRUGBANK', u'PUBCHEM.COMPOUND', u'MESH',
                  u'HMDB', u'INCHI', u'UNII', u'KEGG', u'gtpo'],
        # Note: There are currently no prefixes allowed for Procedure in Biolink, so use some standard OMOP mappings
        u'procedure': [u'ICD10PCS', u'SNOMEDCT'],
        u'_DEFAULT': []
    }

    @staticmethod
    def map_blm_prefixes_to_oxo_prefixes(s):
        """ Attempts to map the Biolink Model prefix to OxO prefix, e.g., 'ICD10' to 'ICD10CM'. If the mapping is not
        available, the function returns the original prefix.

        Parameters
        ----------
        s - Biolink Model prefix (e.g., 'ICD10') or CURIE (e.g., 'ICD10:45576876')

        Returns
        -------
        The OxO prefix/CURIE if it exists, else the original input is returned
        """
        split = s.split(u':')
        if len(split) == 2:
            # Assume s is a curie. Replace the prefix
            prefix, suffix = split
            prefix = BiolinkConceptMapper._mappings_prefixes_blm_to_oxo.get(prefix, prefix)
            curie = u'{prefix}:{suffix}'.format(prefix=prefix, suffix=suffix)
            return curie
        else:
            # Assume s is a prefix
            return BiolinkConceptMapper._mappings_prefixes_blm_to_oxo.get(s, s)

    @staticmethod
    def map_oxo_prefixes_to_blm_prefixes(s):
        """ Attempts to map the OxO prefix to Biolink Model prefix, e.g., 'ICD10CM' to 'ICD10'. If the mapping is not
        available, the function returns the original prefix.

        Parameters
        ----------
        s - OxO prefix (e.g., 'ICD10CM') or curie with OxO prefix (e.g., 'ICD10CM:45576876')

        Returns
        -------
        The prefix or CURIE with the prefix converted to the Biolink Model convention if the mapping exists, otherwise
        the original prefix or CURIE is returned.
        """
        split = s.split(u':')
        if len(split) == 2:
            # Assume s is a curie. Replace the prefix only
            prefix, suffix = split
            prefix = BiolinkConceptMapper._mappings_prefixes_oxo_to_blm.get(prefix, prefix)
            curie = u'{prefix}:{suffix}'.format(prefix=prefix, suffix=suffix)
            return curie
        else:
            # Assume s is a prefix
            return BiolinkConceptMapper._mappings_prefixes_oxo_to_blm.get(s, s)

    def __init__(self, biolink_mappings=_default_ontology_map, distance=2, local_oxo=True):
        """ Constructor

        Parameters
        ----------
        biolink_mappings: mappings between domain and ontology. See documentation for ConceptMapper.
        distance: maximum allowed total distance (as opposed to OxO distance)
        local_oxo: use local implementation of OxO (default: True)
        """
        self.biolink_mappings = biolink_mappings
        self.distance = distance
        self.local_oxo = local_oxo

        # Convert Biolink Model prefix conventions to OxO conventions
        oxo_mappings = dict()
        for domain, prefixes in self.biolink_mappings.items():
            # Map each prefix
            domain_mappings = [BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(prefix) for prefix in prefixes]
            # Remove None from list
            domain_mappings = [m for m in domain_mappings if m is not None]
            oxo_mappings[domain] = domain_mappings

        self._oxo_concept_mapper = ConceptMapper(oxo_mappings, self.distance, self.local_oxo)

    def map_to_omop(self, curie):
        """ Map to OMOP concept from ontology

        Parameters
        ----------
        curie: CURIE

        Returns
        -------
        Dict like:
        {
            "omop_concept_name": "Osteoarthritis",
            "omop_concept_id": 80180,
            "distance": 2
        }
        or None
        """
        # Convert from Biolink Model prefix to OxO prefix. If the prefix isn't in the mappings between BLM and OxO, try
        # the mapping with the original prefix
        oxo_curie = BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(curie)

        # Get mappings from ConceptMapper
        return self._oxo_concept_mapper.map_to_omop(oxo_curie)

    def map_from_omop(self, concept_id, domain_id=None):
        """ Map from OMOP concept to appropriate domain-specific ontology.

        Parameters
        ----------
        concept_id: OMOP concept ID
        domain_id: OMOP concept's domain. Will look it up if not specified

        Returns
        -------
        Array of dict like:
        [{
            "target_curie": "UMLS:C0154091",
            "target_label": "Carcinoma in situ of bladder",
            "distance": 1
        }]
        or None
        """
        # Get mappings from ConceptMapper
        mappings = self._oxo_concept_mapper.map_from_omop(concept_id, domain_id)

        # For each of the mappings, change the prefix to the Biolink Model convention
        for mapping in mappings:
            # Convert from OxO prefix to BLM prefix. If the prefix isn't in the mappings between BLM and OxO, keep the
            # OxO prefix
            mapping[u'target_curie'] = BiolinkConceptMapper.map_oxo_prefixes_to_blm_prefixes(mapping[u'target_curie'])

        return mappings
