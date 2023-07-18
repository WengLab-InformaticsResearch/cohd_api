import requests
from urllib.parse import urljoin
import logging
import json
from ..app import app


class SriNameResolution:
    # server_url = url= 'https://name-resolution-sri.renci.org/'

    server_url_default = 'http://name-resolution-sri-dev.apps.renci.org/'
    server_urls = {
        'dev': 'http://name-resolution-sri-dev.apps.renci.org/',
        # 'ITRB-CI': 'https://name-lookup.ci.transltr.io',
        # 'ITRB-TEST': 'https://name-lookup.test.transltr.io',
        # 'ITRB-PROD': 'https://name-lookup.transltr.io'
    }
    _TIMEOUT = 10  # Query timeout (seconds)

    deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev')
    server_url = server_urls.get(deployment_env, server_url_default)
    logging.info(f'Deployment environment "{deployment_env}" --> using Node Resolution @ {server_url}')

    @staticmethod
    def name_lookup(text, offset=0, limit=10, biolink_type=None, only_prefixes=None):
        """ Lookup CURIEs by name using SRI Name Resolution service

        Parameters
        ----------
        text - name to search for
        offset - The number of results to skip. Can be used to page through the results of a query.
        limit - max number of search results
        biolink_type - The Biolink type to filter to (with or without the biolink: prefix), e.g. biolink:Disease or
                       Disease
        only_prefixes - Pipe-separated, case-sensitive list of prefixes to filter to, e.g. MONDO|EFO

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
        if biolink_type is not None:
            params['biolink_type'] = biolink_type
        if only_prefixes is not None:
            params['only_prefixes'] = only_prefixes

        try:
            response = requests.post(url, params=params, timeout=SriNameResolution._TIMEOUT)
        except requests.exceptions.Timeout:
            logging.error(f'SRI Name Resolution timed out after {SriNameResolution._TIMEOUT} sec\n'
                          f'Posted params:\n{json.dumps(params)}'
                          )
            return None
        if response.status_code == 200:
            return response.json()
        else:
            logging.error('Received a non-200 response code from SRI Name Resolution: '
                          f'{(response.status_code, response.text)}')
            return None
