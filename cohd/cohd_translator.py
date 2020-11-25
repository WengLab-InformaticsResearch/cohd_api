"""
Implementation of the NCATS Biodmedical Data Translator Reasoner API Spec
https://github.com/NCATS-Tangerine/NCATS-ReasonerStdAPI/tree/master/API

Current version 0.9.1
"""

from datetime import datetime
from numbers import Number

from flask import jsonify

from . import query_cohd_mysql
from .cohd_utilities import ln_ratio_ci, ci_significance, omop_concept_curie
from .omop_xref import ConceptMapper


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

    @staticmethod
    def _fix_blm_types(blm_type):
        """ Checks and fixes blm_type.

        Translator Reasoner API changed conventions for blm node types from snake case without 'biolink' prefix (e.g.,
        biolink:population_of_individual_organisms) to camel case requiring prefix (e.g.,
        biolink:PopulationOfIndividualOrganisms). This method attempts to correct the input if it matches the old spec.

        Parameters
        ----------
        blm_type - (String)

        Returns
        -------
        blm_type
        """
        # Don't process None or empty string
        if blm_type is None or not blm_type:
            return blm_type

        # Remove any existing prefix and add biolink prefix
        suffix = blm_type.split(':')[-1]
        blm_type = 'biolink:' + suffix

        # Convert snake case to camel case. Keep the original input if not in this dictionary.
        supported_type_conversions = {
            'biolink:device': 'biolink:Device',
            'biolink:disease': 'biolink:Disease',
            'biolink:drug': 'biolink:Drug',
            'biolink:phenomenon': 'biolink:Phenomenon',
            'biolink:population_of_individual_organisms': 'biolink:PopulationOfIndividualOrganisms',
            'biolink:procedure': 'biolink:Procedure'
        }
        blm_type = supported_type_conversions.get(blm_type, blm_type)

        return blm_type

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

        for query_node in self._query_graph['nodes']:
            if query_node['id'] == query_node_id:
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
        default_method = 'obsExpRatio'
        default_min_cooccurrence = 0
        default_confidence_interval = 0.99
        default_dataset_id = 3
        default_local_oxo = True
        default_mapping_distance = 3
        default_biolink_only = True

        # set of edge types that are supported by the COHD Reasoner
        supported_edge_types = {
            'biolink:correlated_with',  # Currently, COHD models all relations using biolink:correlated_with
            'biolink:related_to',  # Ancestor of biolink:correlated_with
            # Allow edge without biolink prefix
            'correlated_with',
            'related_to',
            # Old documentation incorrectly suggested using 'association'. Permit this for now, but remove in future
            'biolink:association',
            'association',
        }

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
            self._method = default_method
            self._query_options['method'] = default_method
        else:
            if self._method not in ['relativeFrequency', 'obsExpRatio', 'chiSquare']:
                self._valid_query = False
                self._invalid_query_response = ('Query method "{method}" not supported'.format(method=self._method),
                                                400)

        # Get the query_option for dataset ID
        self._dataset_id = self._query_options.get('dataset_id')
        if self._dataset_id is None or not self._dataset_id or not isinstance(self._dataset_id, Number):
            self._dataset_id = default_dataset_id
            self._query_options['dataset_id'] = default_dataset_id

        # Get the query_option for minimum co-occurrence
        self._min_cooccurrence = self._query_options.get('min_cooccurrence')
        if self._min_cooccurrence is None or not isinstance(self._min_cooccurrence, Number):
            self._min_cooccurrence = default_min_cooccurrence
            self._query_options['min_cooccurrence'] = default_min_cooccurrence

        # Get the query_option for confidence_interval. Only used for obsExpRatio. If not specified, use default.
        self._confidence_interval = self._query_options.get('confidence_interval')
        if self._confidence_interval is None or not isinstance(self._confidence_interval, Number) or \
                self._confidence_interval < 0 or self._confidence_interval >= 1:
            self._confidence_interval = default_confidence_interval
            self._query_options['confidence_interval'] = default_confidence_interval

        # Get the query_option for local_oxo
        self._local_oxo = self._query_options.get('local_oxo')
        if self._local_oxo is None or not isinstance(self._local_oxo, bool):
            self._local_oxo = default_local_oxo

        # Get the query_option for maximum mapping distance
        self._mapping_distance = self._query_options.get('mapping_distance')
        if self._mapping_distance is None or not isinstance(self._mapping_distance, Number):
            self._mapping_distance = default_mapping_distance

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
            self._biolink_only = default_biolink_only

        # Get query information from query_graph
        self._query_graph = self._json_data['message']['query_graph']

        # Check that the query_graph is supported by the COHD reasoner (1-hop query)
        edges = self._query_graph['edges']
        if len(edges) != 1:
            self._valid_query = False
            self._invalid_query_response = ('COHD reasoner only supports 1-hop queries', 422)
            return self._valid_query, self._invalid_query_response

        # Check if the edge type is supported by COHD Reasoner
        edge = edges[0]
        edge_types = edge['type']
        if not isinstance(edge_types, list):
            # In TRAPI, QEdge type can be string or list. If it's not currently a list, convert to a list to simplify
            # following processing
            edge_types = [edge_types]
        edge_supported = False
        for edge_type in edge_types:
            if edge_type in supported_edge_types:
                edge_supported = True
                break
        if not edge_supported:
            self._valid_query = False
            self._invalid_query_response = ('QEdge.type not supported by COHD Reasoner API', 422)
            return self._valid_query, self._invalid_query_response

        # Get concept_id_1
        source_node = self._find_query_node(edge['source_id'])
        curie_1 = source_node['curie']  # source node must contain a CURIE
        self._source_concept_mapping = self._concept_mapper.map_to_omop(curie_1)
        if self._source_concept_mapping:
            self._concept_id_1 = self._source_concept_mapping['omop_concept_id']

            # Keep track of this mapping in the query_graph for the response
            source_node['mapped_omop_concept'] = self._source_concept_mapping
        else:
            self._valid_query = False
            self._invalid_query_response = ('Could not map source node to OMOP concept', 422)
            return self._valid_query, self._invalid_query_response

        # Check the formatting of node 1's type (even though it's not used)
        if 'type' in source_node:
            source_node['type'] = COHDTranslatorReasoner._fix_blm_types(source_node['type'])

        # Get the desired association concept or type
        target_node = self._find_query_node(edge['target_id'])
        curie_2 = target_node.get('curie')
        node_type_2 = target_node.get('type')
        if curie_2 is not None and curie_2:
            # If CURIE of target node is specified, then query the association between concept_1 and concept_2
            self._domain_ids = None
            self._target_concept_mapping = self._concept_mapper.map_to_omop(curie_2)

            if self._target_concept_mapping:
                self._concept_id_2 = self._target_concept_mapping['omop_concept_id']

                # Keep track of this mapping in the query graph for the response
                target_node['mapped_omop_concept'] = self._target_concept_mapping
            else:
                self._valid_query = False
                self._invalid_query_response = ('Could not map target node to OMOP concept', 422)
                return self._valid_query, self._invalid_query_response
        elif node_type_2 is not None and node_type_2:
            # Attempt to correct the node type if necessary
            node_type_2 = COHDTranslatorReasoner._fix_blm_types(node_type_2)
            target_node['type'] = node_type_2

            # If CURIE is not specified and target node's type is specified, then query the association between
            # concept_1 and all concepts in the domain
            self._concept_id_2 = None
            self._domain_ids = map_blm_class_to_omop_domain(node_type_2)

            # Keep track of this mapping in the query graph for the response
            target_node['mapped_omop_domains'] = self._domain_ids
        else:
            # No CURIE or type specified, query for associations against all concepts
            self._concept_id_2 = None
            self._domain_ids = None

        # Get the desired maximum number of results
        max_results = self._json_data.get('max_results')
        if max_results:
            self._max_results = min(max_results, self._max_results)  # Don't allow user to specify more than default

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
        if self._method == 'obsexpratio' and self._confidence_interval > 0:
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
    if 'n_c1_c2' in cohd_result:
        # chi-square
        return cohd_result['n_c1_c2'] >= cooccurrence
    elif 'observed_count' in cohd_result:
        # obsExpRatio
        return cohd_result['observed_count'] >= cooccurrence
    elif 'concept_pair_count' in cohd_result:
        # relative frequency
        return cohd_result['concept_pair_count'] >= cooccurrence
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
    if 'p-value' in cohd_result:
        # chi-square
        return cohd_result['p-value'] < threshold
    elif 'ln_ratio' in cohd_result:
        # obsExpRatio
        if threshold >= 0:
            return cohd_result['ln_ratio'] >= threshold
        else:
            return cohd_result['ln_ratio'] <= threshold
    elif 'relative_frequency' in cohd_result:
        # relative frequency
        return cohd_result['relative_frequency'] >= threshold
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
    if 'ln_ratio' in cohd_result:
        # obsExpFreq
        ci = ln_ratio_ci(cohd_result['observed_count'], cohd_result['ln_ratio'], alpha)
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
            'nodes': [],
            'edges': []
        }
        self.query_options = query_options
        self.max_results = max_results
        self.biolink_only = biolink_only

        # Mappings from OMOP to other vocabularies / ontologies
        self.concept_mapper = concept_mapper

        # Save info from query graph
        self.query_graph = query_graph
        query_edge = query_graph['edges'][0]
        self.query_edge_id = query_edge['id']
        self.query_edge_type = query_edge['type']
        self.query_source_node_id = query_graph['edges'][0]['source_id']
        self.query_target_node_id = query_graph['edges'][0]['target_id']

        # Get the input CURIEs from the query graph
        for qnode in self.query_graph['nodes']:
            if qnode['id'] == self.query_source_node_id:
                self.query_source_node_curie = qnode.get('curie', None)
            elif qnode['id'] == self.query_target_node_id:
                self.query_target_node_curie = qnode.get('curie', None)

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
        node_1 = self.get_node(concept_1_id, query_node_curie=self.query_source_node_curie)

        if self.biolink_only and not node_1.get('biolink_compliant', False):
            # Only include results when node_1 maps to biolink
            return

        # Get node for concept 2
        concept_2_id = cohd_result['concept_id_2']
        concept_2_name = cohd_result.get('concept_2_name')
        concept_2_domain = cohd_result.get('concept_2_domain')
        node_2 = self.get_node(concept_2_id, concept_2_name, concept_2_domain, self.query_target_node_curie)

        if self.biolink_only and not node_2.get('biolink_compliant', False):
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
            'node_bindings': [
                {
                    'qg_id': self.query_source_node_id,
                    'kg_id': kg_node_1['id']
                },
                {
                    'qg_id': self.query_target_node_id,
                    'kg_id': kg_node_2['id']
                }
            ],
            'edge_bindings': [{
                'qg_id': self.query_edge_id,
                'kg_id': kg_edge['id']
            }]
        }
        self.results.append(result)
        return result

    def get_node(self, concept_id, concept_name=None, domain=None, query_node_curie=None):
        """ Gets the node from internal "graph" representing the OMOP concept. Creates the node if not yet created.
        Node is not added to the knowledge graph or results.

        Parameters
        ----------
        concept_id: OMOP concept ID
        concept_name: OMOP concept name
        domain: OMOP concept domain
        query_node_curie: CURIE used in the QNode corresponding to this KG Node

        Returns
        -------
        Node for internal use
        """
        node = self.nodes.get(concept_id)

        if node is None:
            # Create the node
            if concept_name is None or domain is None:
                # Concept information not specified, lookup concept definition
                concept_name = concept_name if concept_name is not None else ''
                domain = domain if domain is not None else ''
                concept_def = query_cohd_mysql.omop_concept_definition(concept_id)

                if concept_def is not None and not concept_name:
                    concept_name = concept_def['concept_name']
                if concept_def is not None and not domain:
                    domain = concept_def['domain_id']

            # Map to Biolink Model or other target ontologies
            blm_type = map_omop_domain_to_blm_class(domain)
            mappings = []
            if self.concept_mapper:
                mappings = self.concept_mapper.map_from_omop(concept_id, domain)

            # If we don't find better mappings (below) for this concept, default to OMOP CURIE and label
            omop_curie = omop_concept_curie(concept_id)
            primary_curie = omop_curie
            primary_label = concept_name

            found = False
            if query_node_curie is not None and query_node_curie:
                # The CURIE was specified for this node in the query_graph, use that CURIE to identify this node
                found = True
                primary_curie = query_node_curie

                # Find the label from the mappings
                for mapping in mappings:
                    if mapping['target_curie'] == query_node_curie:
                        primary_label = mapping['target_label']
                        break
            else:
                # Choose one of the mappings to be the main identifier for the node. Prioritize distance first, and then
                # choose by the order of prefixes listed in the Concept Mapper. If no biolink prefix found, use OMOP
                blm_prefixes = self.concept_mapper.biolink_mappings.get(blm_type, [])
                for d in range(self.concept_mapper.distance + 1):
                    if found:
                        break

                    # Get all mappings with the current distance
                    m_d = [m for m in mappings if m['distance'] == d]

                    # Look for the first matching prefix in the list of biolink prefixes
                    for prefix in blm_prefixes:
                        if found:
                            break

                        for m in m_d:
                            if m['target_curie'].split(':')[0] == prefix:
                                primary_curie = m['target_curie']
                                primary_label = m['target_label']
                                found = True
                                break

            # Create representations for the knowledge graph node and query node, but don't add them to the graphs yet
            internal_id = '{id:06d}'.format(id=len(self.nodes))
            node = {
                'omop_id': concept_id,
                'name': concept_name,
                'domain': domain,
                'internal_id': internal_id,
                'kg_node': {
                    'id': primary_curie,
                    'name': primary_label,
                    'type': [blm_type],
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
                        },
                        {
                            'name': 'synonyms',
                            'value': mappings,
                            'type': 'EDAM:data_3509',  # Ontology mapping
                            'source': 'COHD',
                        }
                    ]
                },
                'in_kgraph': False,
                'biolink_compliant': found
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
            self.knowledge_graph['nodes'].append(kg_node)
            node['in_kgraph'] = True

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
            'id': ke_id,
            'type': self.query_edge_type,
            'source_id': node_1['kg_node']['id'],
            'target_id': node_2['kg_node']['id'],
            'attributes': attributes
        }

        # Add the new edge
        self.knowledge_graph['edges'].append(kg_edge)

        return kg_node_1, kg_node_2, kg_edge

    def serialize(self):
        """ Creates the response message with JSON data in Reasoner Std API format

        Returns
        -------
        Response message with JSON data in Reasoner Std API format
        """
        return jsonify({
            'context': 'https://biolink.github.io/biolink-model/context.jsonld',
            'type': 'translator_reasoner_message',
            'reasoner_id': 'COHD',
            'tool_version': 'COHD 2.2.0',
            'schema_version': '0.9.2',
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'n_results': len(self.results),
            'message_code': 'OK',
            'code_description': '{n} result(s) found'.format(n=len(self.results)),
            'query_options': self.query_options,
            'results': self.results,
            'query_graph': self.query_graph,
            'knowledge_graph': self.knowledge_graph
        })


mappings_domain_ontology = {
    '_DEFAULT': ['ICD9CM', 'RxNorm', 'UMLS', 'DOID', 'MONDO']
}


def map_blm_class_to_omop_domain(node_type):
    """ Maps the Biolink Model class to OMOP domain_id, e.g., 'biolink:Disease' to 'Condition'

    Note, some classes may map to multiple domains, e.g., 'biolink:PopulationOfIndividualOrganisms' maps to
    ['Ethnicity', 'Gender', 'Race']

    Parameters
    ----------
    concept_type - Biolink Model class, e.g., 'biolink:Disease'

    Returns
    -------
    If normalized successfully: List of OMOP domains, e.g., ['Drug']; Otherwise: None
    """
    mappings = {
        'biolink:Device': ['Device'],
        'biolink:Disease': ['Condition'],
        'biolink:Drug': ['Drug'],
        'biolink:Phenomenon': ['Measurement', 'Observation'],
        'biolink:PopulationOfIndividualOrganisms': ['Ethnicity', 'Gender', 'Race'],
        'biolink:Procedure': ['Procedure']
    }
    return mappings.get(node_type)


def map_omop_domain_to_blm_class(domain):
    """ Maps the OMOP domain_id to Biolink Model class, e.g., 'Condition' to 'biolink:Disease'

    Parameters
    ----------
    domain - OMOP domain, e.g., 'Condition'

    Returns
    -------
    If normalized successfully: Biolink Model semantic type. If no mapping found, use NamedThing
    """
    mappings = {
        'Condition': 'biolink:Disease',
        'Device': 'biolink:Device',
        'Drug': 'biolink:Drug',
        'Ethnicity': 'biolink:PopulationOfIndividualOrganisms',
        'Gender': 'biolink:PopulationOfIndividualOrganisms',
        'Measurement': 'biolink:Phenomenon',
        'Observation': 'biolink:Phenomenon',
        'Procedure': 'biolink:Procedure',
        'Race': 'biolink:PopulationOfIndividualOrganisms'
    }
    default_type = 'named_thing'
    return mappings.get(domain, default_type)


class BiolinkConceptMapper:
    """ Maps between OMOP concepts and Biolink Model

    When mapping from OMOP conditions to Biolink Model diseases, since SNOMED-CT, ICD10CM, ICD9CM, and MedDRA are now
    included in Biolink Model, map to these source vocabularies using the OMOP concept definitions. When mapping from
    other ontologies to OMOP, leverage the OxO as well.
    """

    _mappings_prefixes_blm_to_oxo = {
        # Disease prefixes
        'MONDO': 'MONDO',
        'DOID': 'DOID',
        'OMIM': 'OMIM',
        'ORPHANET': 'Orphanet',
        'ORPHA': None,
        'EFO': 'EFO',
        'UMLS': 'UMLS',
        'MESH': 'MeSH',
        'MEDDRA': 'MedDRA',
        'NCIT': 'NCIT',
        'SNOMEDCT': 'SNOMEDCT',
        'medgen': None,
        'ICD10': 'ICD10CM',
        'ICD9': 'ICD9CM',
        'ICD0': None,
        'HP': 'HP',
        'MP': 'MP',
        # Drug prefixes
        'PHARMGKB.DRUG': None,
        'CHEBI': 'CHEBI',
        'CHEMBL.COMPOUND': None,
        'DRUGBANK': 'DrugBank',
        'PUBCHEM.COMPOUND': 'PubChem_Compound',
        'HMDB': 'HMDB',
        'INCHI': None,
        'UNII': None,
        'KEGG': 'KEGG',
        'gtpo': None,
        # Procedure prefixes
        'ICD10PCS': None
    }

    _mappings_prefixes_oxo_to_blm = {
        # Disease prefixes
        'MONDO': 'MONDO',
        'DOID': 'DOID',
        'OMIM': 'OMIM',
        'Orphanet': 'ORPHANET',
        'EFO': 'EFO',
        'UMLS': 'UMLS',
        'MeSH': 'MESH',
        'MedDRA': 'MEDDRA',
        'NCIT': 'NCIT',
        'SNOMEDCT': 'SNOMEDCT',
        'ICD10CM': 'ICD10',
        'ICD9CM': 'ICD9',
        'HP': 'HP',
        'MP': 'MP',
        # Drug prefixes
        'CHEBI': 'CHEBI',
        'DrugBank': 'DRUGBANK',
        'PubChem_Compound': 'PUBCHEM.COMPOUND',
        'HMDB': 'HMDB',
        'KEGG': 'KEGG',
        # Procedure prefixes
    }

    _default_ontology_map = {
        'biolink:Disease': ['MONDO', 'DOID', 'OMIM', 'ORPHANET', 'ORPHA', 'EFO', 'UMLS', 'MESH', 'MEDDRA',
                       'NCIT', 'SNOMEDCT', 'medgen', 'ICD10', 'ICD9', 'ICD0', 'HP', 'MP'],
        # Note: for Drug, also map to some of the prefixes specified in ChemicalSubstance
        'biolink:Drug': ['PHARMGKB.DRUG', 'CHEBI', 'CHEMBL.COMPOUND', 'DRUGBANK', 'PUBCHEM.COMPOUND', 'MESH',
                  'HMDB', 'INCHI', 'UNII', 'KEGG', 'gtpo'],
        # Note: There are currently no prefixes allowed for Procedure in Biolink, so use some standard OMOP mappings
        'biolink:Procedure': ['ICD10PCS', 'SNOMEDCT'],
        '_DEFAULT': []
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
        split = s.split(':')
        if len(split) == 2:
            # Assume s is a curie. Replace the prefix
            prefix, suffix = split
            prefix = BiolinkConceptMapper._mappings_prefixes_blm_to_oxo.get(prefix, prefix)
            curie = '{prefix}:{suffix}'.format(prefix=prefix, suffix=suffix)
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
        split = s.split(':')
        if len(split) == 2:
            # Assume s is a curie. Replace the prefix only
            prefix, suffix = split
            prefix = BiolinkConceptMapper._mappings_prefixes_oxo_to_blm.get(prefix, prefix)
            curie = '{prefix}:{suffix}'.format(prefix=prefix, suffix=suffix)
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
        for blm_type, prefixes in list(self.biolink_mappings.items()):
            omop_domains = map_blm_class_to_omop_domain(blm_type)
            if omop_domains is None or not omop_domains:
                continue

            # Map each prefix
            domain_mappings = [BiolinkConceptMapper.map_blm_prefixes_to_oxo_prefixes(prefix) for prefix in prefixes]
            # Remove None from list
            domain_mappings = [m for m in domain_mappings if m is not None]
            for omop_domain in omop_domains:
                oxo_mappings[omop_domain] = domain_mappings

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
            mapping['target_curie'] = BiolinkConceptMapper.map_oxo_prefixes_to_blm_prefixes(mapping['target_curie'])

        return mappings
