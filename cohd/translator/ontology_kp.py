import logging
import requests
from requests.compat import urljoin
from typing import Any, Optional, Dict, List, Set

from ..app import cache
from .sri_node_normalizer import SriNodeNormalizer


class OntologyKP:
    base_url = 'https://stars-app.renci.org/sparql-kp/'
    endpoint_query = 'query'
    endpoint_meta_kg = 'meta_knowledge_graph'
    _TIMEOUT = 30  # Query timeout (seconds)

    @staticmethod
    @cache.memoize(timeout=86400, cache_none=False)
    def get_meta_kg():
        """ Get Ontology KP meta_knowledge_graph """
        try:
            url = urljoin(OntologyKP.base_url, OntologyKP.endpoint_meta_kg)
            resp = requests.get(url, timeout=OntologyKP._TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            else:
                # Return None, indicating an error occurred
                logging.warning(f'Received a non-200 status response code from Ontology KP meta_kg ({url}): ' 
                                f'{(resp.status_code, resp.text)}')
                return None
        except requests.RequestException:
            # Return None, indicating an error occurred
            logging.warning(f'Encountered an RequestException when querying Ontology KP meta_kg: {url}')
            return None

    @staticmethod
    def get_allowed_prefixes(categories: List[str]) -> Optional[Set[str]]:
        """ Get the set of id_prefixes for categories from meta_knowledge_graph

        Parameters
        ----------
        categories

        Returns
        -------
        Set[str] or None if error
        """
        meta_kg = OntologyKP.get_meta_kg()
        if meta_kg is None:
            return None
        nodes = meta_kg.get('nodes')
        if nodes is None:
            logging.warning('Ontology KP meta_kg has missing "nodes"')
            return None

        allowed_prefixes = set()
        for cat in categories:
            if cat in nodes and 'id_prefixes' in nodes[cat]:
                allowed_prefixes = allowed_prefixes.union(nodes[cat]['id_prefixes'])

        return allowed_prefixes

    @staticmethod
    def convert_to_preferred(curies: List[str], categories: List[str]) -> List[str]:
        """ Converts the input CURIEs into the prefixes prefered by Ontology KP

        Parameters
        ----------
        curies - List[str]
        categories - List[str]

        Returns
        -------
        List of CURIEs converted to preferred prefixes, if successful. Otherwise, the CURIEs are returned unaltered.
        """
        allowed_prefixes = OntologyKP.get_allowed_prefixes(categories)
        if allowed_prefixes is not None:
            # Get normalized nodes for any of the CURIEs with prefixes that are not in the allowed list
            curies_to_convert = [c for c in curies if c.split(':')[0] not in allowed_prefixes]
            norm_nodes = SriNodeNormalizer.get_normalized_nodes(curies_to_convert)
            if norm_nodes is None:
                # Failed node normalizer. Return the original curies
                return curies

            preferred_curies = list()
            for curie in curies:
                if curie not in curies_to_convert:
                    # This CURIE already allowed
                    preferred_curies.append(curie)
                else:
                    # Get the ID with the prefix that appears earliest in the allowed prefixes
                    new_ids = [v['identifier'] for v in norm_nodes[curie]['equivalent_identifiers']]
                    preferred_curie = None
                    for prefix in allowed_prefixes:
                        for nid in new_ids:
                            if nid.split(':')[0] == prefix:
                                preferred_curie = nid
                                break
                        if preferred_curie is not None:
                            break
                    if preferred_curie is None:
                        # No CURIE with allowed prefix found. Just try with the original CURIE
                        preferred_curie = curie
                    preferred_curies.append(preferred_curie)
            return preferred_curies
        else:
            # Didn't get a valid response from meta_knowledge_graph. Don't alter the input CURIEs
            return curies

    @staticmethod
    def get_descendants(curies: List[str], categories: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """ Get descendant CURIEs from Ontology KP

        Parameters
        ----------
        curies - list of curies
        categories - list of biolink categories, or None

        Returns
        -------
        All knowledge graph nodes returned by the Ontology KP. If any errors, an emtpy dict is returned.
        """
        # Ontology KP doesn't seem to like it when categories is null. Replace it with NamedThing for functionally
        # equivalent TRAPI
        if categories is None:
            categories = ['biolink:NamedThing']

        preferred_curies = OntologyKP.convert_to_preferred(curies, categories)

        try:
            # Query Ontology KP for descendants
            m = {
                "message": {
                    "query_graph": {
                        "nodes": {
                            "a": {
                                "ids": preferred_curies
                            },
                            "b": {
                                "categories": categories
                            }
                        },
                        "edges": {
                            "ab": {
                                "subject": "b",
                                "object": "a",
                                "predicate": "biolink:subclass_of"
                            }
                        }
                    }
                }
            }
            url = urljoin(OntologyKP.base_url, OntologyKP.endpoint_query)
            response = requests.post(url=url, json=m, timeout=OntologyKP._TIMEOUT)
            if response.status_code == 200:
                j = response.json()
                if 'message' in j and 'knowledge_graph' in j['message']:
                    nodes = j['message']['knowledge_graph'].get('nodes')
                    if nodes is not None:
                        # Return all nodes in KG except for reflexive CURIE node if it's different from original CURIE
                        # This is to prevent the same concept from being queried twice with 2 different IDs
                        for pc in preferred_curies:
                            if pc not in curies:
                                del nodes[pc]
                        return nodes
                    else:
                        # Return an empty dict, indicating no descendants found
                        return dict()
            else:
                logging.warning(f'Ontology KP returned status code {response.status_code}: {response.content}')
        except requests.RequestException:
            # Return None, indicating an error occurred
            logging.warning('Encountered an RequestException when querying descendants from Ontology KP')
            return None

        # Return None, indicating an error occurred
        return None
