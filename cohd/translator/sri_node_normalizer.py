import logging
import requests
import json
from requests.compat import urljoin
from typing import Union, Any, Optional, Dict, List
from ..app import app

class NormalizedNodeIdentifier:
    def __init__(self, node_identifier_response):
        self.node_identifier_response = node_identifier_response
        self.id = self.node_identifier_response['identifier']
        self.label = self.node_identifier_response.get('label', '')


class NormalizedNode:
    def __init__(self, node_response: Dict):
        self.node_response = node_response
        self.normalized_identifier = NormalizedNodeIdentifier(self.node_response['id'])
        self.equivalent_identifiers = [NormalizedNodeIdentifier(i) for i in self.node_response['equivalent_identifiers']]
        self.categories = self.node_response['type']


class SriNodeNormalizer:
    # base_url = 'https://nodenormalization-sri.renci.org/'
    # base_url = 'https://nodenormalization-sri-dev.renci.org/'
    # base_url = 'https://nodenormalization-sri.renci.org/'

    base_url_default = 'https://nodenorm.transltr.io/'
    base_urls = {
        'dev': 'https://nodenormalization-sri.renci.org/',
        # CI and TEST have been non-functional for a long time...
        # 'ITRB-CI': 'https://nodenorm.ci.transltr.io/',
        # 'ITRB-TEST': 'https://nodenorm.test.transltr.io/',
        'ITRB-PROD': 'https://nodenorm.transltr.io/'
    }
    endpoint_get_normalized_nodes = 'get_normalized_nodes'
    INFORES_ID = 'infores:sri-node-normalizer'
    _TIMEOUT = 10  # Query timeout (seconds)

    deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev')
    base_url = base_urls.get(deployment_env, base_url_default)
    logging.info(f'Deployment environment "{deployment_env}" --> using Node Norm @ {base_url}')

    @staticmethod
    def get_normalized_nodes_raw(curies: List[str]) -> Optional[Dict[str, Any]]:
        """ Straightforward call to get_normalized_nodes. Returns json from response.
        Parameters
        ----------
        curies - list of curies
        Returns
        -------
        JSON response from endpoint or None. Each input curie will be a key in the response. If no normalized node is
        found, the entry will be null.
        """
        if not curies:
            return None

        url = urljoin(SriNodeNormalizer.base_url, SriNodeNormalizer.endpoint_get_normalized_nodes)
        data = {'curies': curies}
        response = requests.post(url=url, json=data, timeout=SriNodeNormalizer._TIMEOUT)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error('Received a non-200 response code from SRI Node Normalizer: '
                          f'{(response.status_code, response.text)}\n'
                          f'Posted data:\n{json.dumps(data)}'
                          )
            return None

    @staticmethod
    def get_normalized_nodes(curies: List[str]) -> Optional[Dict[str, NormalizedNode]]:
        """ Wraps a NodeNorm call to return a dictionary of NormalizedNode objects per response item

        Parameters
        ----------
        curies - list of curies

        Returns
        -------
        Dict of NormalizedNodes. Each input curie will be a key in the response. If no normalized node is
        found, the entry will be None.
        """
        response = SriNodeNormalizer.get_normalized_nodes_raw(curies)
        if response is not None:
            return {k: NormalizedNode(v) if v is not None else None for (k, v) in response.items()}
        else:
            return None

    @staticmethod
    def get_canonical_identifiers(curies: List[str]) -> Optional[Dict[str, Optional[str]]]:
        """ Retrieve the canonical identifier

        Parameters
        ----------
        curies - list of CURIES

        Returns
        -------
        dict of canonical identifiers for each curie. If curie not found, then None
        """
        j = SriNodeNormalizer.get_normalized_nodes(curies)
        if j is None:
            return None

        canonical = dict()
        for curie in curies:
            if curie in j and j[curie] is not None:
                canonical[curie] = j[curie]['id']
            else:
                canonical[curie] = None
        return canonical

    @staticmethod
    def remove_equivalents(curies: List[str]) -> List[str]:
        """ Remove equivalent identifiers from a list.

        If the canonical node is in the list, keep it, and remove other equivalents IDs. Otherwise, keep the first equivalent ID.

        Parameters
        ----------
        curies - list of CURIEs

        Returns
        -------
        list of curies with duplicates removed
        """
        if len(curies) <= 1:
            # Not enough curies to have equivalents
            return curies

        normalized_nodes = SriNodeNormalizer.get_normalized_nodes(curies)
        if normalized_nodes is None:
            logging.warning('Unable to check for duplicates in QNode IDs because of error calling SRI Node Normalizer')
            return curies

        index = 0
        while index < len(curies):
            curie = curies[index]
            normalized_node = normalized_nodes.get(curie)

            if normalized_node is None:
                # Couldn't find normalized nodes for this CURIE. Keep it
                index += 1
                continue

            canonical_id = normalized_node.normalized_identifier.id
            if canonical_id in curies:
                # Canonical ID is in the list. Keep the canonical ID and remove all other equivalent IDs
                ids_to_remove = {eq_id.id for eq_id in normalized_node.equivalent_identifiers if eq_id.id != canonical_id}
                new_curies = [c for c in curies if c not in ids_to_remove]

                if curie == canonical_id:
                    # CURIE at current index was kept, increment index
                    index += 1
                elif len(new_curies) == len(curies):
                    # Unexpected: no CURIEs removed and the current CURIE is not the canonical CURIE
                    # Log the error, and move onto next index to prevent infinite loop
                    logging.error('Expected at least 1 CURIE to be removed, but none were')
                    index += 1
                curies = new_curies
            else:
                # Canonical ID not in the list. Keep the current ID and remove all other equivalent IDs
                ids_to_remove = {eq_id.id for eq_id in normalized_node.equivalent_identifiers if eq_id.id != curie}
                curies = [c for c in curies if c not in ids_to_remove]
                index += 1

        return curies