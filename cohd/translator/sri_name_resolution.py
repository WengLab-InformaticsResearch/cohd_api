import requests
from urllib.parse import urljoin
import logging


class SriNameResolution:
    server_url = url= 'https://name-resolution-sri.renci.org/'

    @staticmethod
    def name_lookup(text, offset=0, limit=10):
        """ Lookup CURIEs by name using SRI Name Resolution service

        Parameters
        ----------
        text - name to search for
        offset - ???
        limit - max number of search results

        Returns
        -------
        JSON response from endpoint or None. The response is a dictionary with CURIEs for keys.
        The keys seem to be ordered by match preference. 
        """
        endpoint = 'lookup'
        url = urljoin(SriNameResolution.server_url, endpoint)
        params = {
            'string': text,
            'offset': offset,
            'limit': limit
        }
        response = requests.post(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error('Received a non-200 response code from SRI Name Resolution: '
                          f'{(response.status_code, response.text)}')
            return None
