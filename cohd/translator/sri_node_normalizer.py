import logging
import requests
from requests.compat import urljoin
from typing import Union, Any, Optional, Dict, List


class SriNodeNormalizer:
    base_url = 'https://nodenormalization-sri.renci.org/1.1/'
    # base_url = 'https://nodenormalization-sri-dev.renci.org/1.1/'
    endpoint_get_normalized_nodes = 'get_normalized_nodes'

    @staticmethod
    def get_normalized_nodes(curies: List[str]) -> Optional[Dict[str, Any]]:
        """ Straightforward call to get_normalized_nodes. Returns json from response.

        Parameters
        ----------
        curies - list of curies

        Returns
        -------
        JSON response from endpoint or None. Each input curie will be a key in the response. If no normalized node is
        found, the entry will be null.
        """
        url = urljoin(SriNodeNormalizer.base_url, SriNodeNormalizer.endpoint_get_normalized_nodes)
        response = requests.post(url=url, json={'curies': curies})
        if response.status_code == 200:
            return response.json()
        else:
            logging.warning('Received a non-200 response code from SRI Node Normalizer: '
                            f'{(response.status_code, response.text)}')
            return None

    @staticmethod
    def get_canonical_identifiers(curies: List[str]) -> Union[Dict[str, Union[str, None]], None]:
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
