import requests
from urllib.parse import urljoin
import logging
from ..app import app


class SriNameResolution:
    # server_url = url= 'https://name-resolution-sri.renci.org/'

    server_url_default = 'https://name-lookup.transltr.io'
    server_urls = {
        'dev': 'https://name-resolution-sri.renci.org',
        'ITRB-CI': 'https://name-lookup.ci.transltr.io',
        'ITRB-TEST': 'https://name-lookup.test.transltr.io',
        'ITRB-PROD': 'https://name-lookup.transltr.io'
    }
    _TIMEOUT = 10  # Query timeout (seconds)

    deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev')
    server_url = server_urls.get(deployment_env, server_url_default)
    logging.info(f'Deployment environment "{deployment_env}" --> using Node Resolution @ {server_url}')

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
        response = requests.post(url, params=params, timeout=SriNameResolution._TIMEOUT)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error('Received a non-200 response code from SRI Name Resolution: '
                          f'{(response.status_code, response.text)}')
            return None
