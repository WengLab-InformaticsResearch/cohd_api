import logging
import requests
from requests.compat import urljoin
from typing import Any, Optional, Dict, List, Set, Tuple

from ..app import app, cache
from .sri_node_normalizer import SriNodeNormalizer


def _bypass_cache(f, *args, **kwargs):
    return kwargs.get('bypass', False)


class Ubergraph:
    base_url_default = 'https://automat.transltr.io/ubergraph/1.4/'
    base_urls = {
        'dev': 'https://automat.renci.org/ubergraph/1.4/',
        'ITRB-CI': 'https://automat.ci.transltr.io/ubergraph/1.4/',
        'ITRB-TEST': 'https://automat.test.transltr.io/ubergraph/1.4/',
        'ITRB-PROD': 'https://automat.transltr.io/ubergraph/1.4/'
    }    
    endpoint_query = 'query'
    endpoint_meta_kg = 'meta_knowledge_graph'
    INFORES_ID = 'infores:automat-ubergraph'
    _TIMEOUT = 10  # Query timeout (seconds)    

    deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev')
    base_url = base_urls.get(deployment_env, base_url_default)
    logging.info(f'Deployment environment "{deployment_env}" --> using Node Norm @ {base_url}')
    

    @staticmethod
    @cache.memoize(timeout=86400, cache_none=False)
    def get_meta_kg():
        """ Get Ontology KP meta_knowledge_graph """
        try:
            url = urljoin(Ubergraph.base_url, Ubergraph.endpoint_meta_kg)
            resp = requests.get(url, timeout=Ubergraph._TIMEOUT)
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
        meta_kg = Ubergraph.get_meta_kg()
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
    def convert_to_preferred(curies: List[str], categories: List[str]) -> Dict[str, str]:
        """ Converts the input CURIEs into the prefixes prefered by Ontology KP

        Parameters
        ----------
        curies - List[str]
        categories - List[str]

        Returns
        -------
        Dict of CURIEs converted to preferred prefixes, if successful. Otherwise, the CURIEs are returned unaltered.
        """
        allowed_prefixes = Ubergraph.get_allowed_prefixes(categories)
        if allowed_prefixes is not None:
            # Get normalized nodes for any of the CURIEs with prefixes that are not in the allowed list
            curies_to_convert = [c for c in curies if c.split(':')[0] not in allowed_prefixes]
            norm_nodes = SriNodeNormalizer.get_normalized_nodes(curies_to_convert)
            if norm_nodes is None:
                # Failed node normalizer. Return the original curies
                return {c:c for c in curies}

            preferred_curies = dict()
            for curie in curies:
                if curie not in curies_to_convert:
                    # This CURIE already allowed
                    preferred_curies[curie] = curie
                else:
                    if norm_nodes.get(curie) is None:
                        # No node normalizer info for this CURIE, try to use the CURIE as is
                        preferred_curies[curie] = curie
                        continue

                    # Get the ID with the prefix that appears earliest in the allowed
                    new_ids = [v.id for v in norm_nodes[curie].equivalent_identifiers]
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
                    preferred_curies[curie] = preferred_curie
            return preferred_curies
        else:
            # Didn't get a valid response from meta_knowledge_graph. Don't alter the input CURIEs
            return {curie:curie for curie in curies}


    @staticmethod
    @cache.memoize(timeout=3600, cache_none=False, unless=_bypass_cache)
    def get_descendants(curies: List[str], categories: Optional[List[str]] = None, timeout: int = _TIMEOUT, bypass: bool = False) -> \
            Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
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

        preferred_curies = Ubergraph.convert_to_preferred(curies, categories)
        # Reverse mapping to get original CURIE from preferred CURIE
        original_curies = {v:k for (k,v) in preferred_curies.items()}

        try:
            # Query Ontology KP for descendants
            m = {
                "message": {
                    "query_graph": {
                        "nodes": {
                            "a": {
                                "ids": list(preferred_curies.values())
                            },
                            "b": {
                                "categories": categories
                            }
                        },
                        "edges": {
                            "ab": {
                                "subject": "b",
                                "object": "a",
                                "predicates": ["biolink:subclass_of"]
                            }
                        }
                    }
                }
            }

            logging.debug(m)

            url = urljoin(Ubergraph.base_url, Ubergraph.endpoint_query)
            response = requests.post(url=url, json=m, timeout=timeout)
            if response.status_code == 200:
                j = response.json()
                if 'message' in j and 'knowledge_graph' in j['message']:
                    kg = j['message']['knowledge_graph']
                    nodes = kg.get('nodes')
                    edges = kg.get('edges')
                    if nodes is not None and edges is not None:
                        # Replace preferred CURIEs with the original queried CURIE
                        for curie, pc in preferred_curies.items():
                            if pc in nodes and curie != pc:
                                nodes[curie] = nodes[pc]
                                del nodes[pc]

                        # Also return a dictionary indicating the QNode IDs that are ancestors of each descendant
                        ancestor_dict = {original_curies[e['subject']] if e['subject'] in original_curies
                                else e['subject']:original_curies[e['object']]
                                for e in edges.values() if e['predicate'] == 'biolink:subclass_of'}

                        return nodes, ancestor_dict
                    else:
                        # Return an empty dict, indicating no descendants found
                        return dict(), dict()
            else:
                logging.warning(f'Automat-Ubergraph returned status code {response.status_code}: {response.content}')
        except requests.Timeout:
            logging.warning(f'Automat-Ubergraph timed out when querying for descendants ({Ubergraph._TIMEOUT} sec)')
            return None
        except requests.RequestException:
            # Return None, indicating an error occurred
            logging.warning('Encountered an RequestException when querying descendants from Automat-Ubergraph')
            return None

        # Return None, indicating an error occurred
        return None
