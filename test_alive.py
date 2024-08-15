"""
This test module tests the COHD API by making requests to cohd.io/api and checking the schema of the response JSONs and
checking the results against known values.

Intended to be run with pytest: pytest -s test_cohd_io.py
"""
from urllib.parse import urljoin
import requests
import time

# Choose which server to test
servers = ['https://cohd-api.ci.transltr.io/api',
           'https://cohd-api.test.transltr.io/api',
           'https://cohd.io/api',
           'https://cohd-api.transltr.io/api']

other_tests = [
            'https://openpredict.ci.transltr.io/meta_knowledge_graph',
            'https://openpredict.test.transltr.io/meta_knowledge_graph',
            'https://openpredict.transltr.io/meta_knowledge_graph',
            'https://collaboratory-api.ci.transltr.io/meta_knowledge_graph',
            'https://collaboratory-api.test.transltr.io/meta_knowledge_graph',
            'https://collaboratory-api.transltr.io/meta_knowledge_graph',
            'https://automat.transltr.io/ubergraph/meta_knowledge_graph',
            'https://automat.ci.transltr.io/icees-kg/meta_knowledge_graph',
            'https://automat.test.transltr.io/icees-kg/meta_knowledge_graph',
            'https://automat.transltr.io/icees-kg/meta_knowledge_graph',
            'https://chp-api.ci.transltr.io/meta_knowledge_graph',
            'https://chp-api.transltr.io/meta_knowledge_graph',
            'https://cooccurrence.ci.transltr.io/meta_knowledge_graph',
            'https://cooccurrence.transltr.io/meta_knowledge_graph',
            'https://bte.ci.transltr.io/v1/meta_knowledge_graph',
            'https://bte.transltr.io/v1/meta_knowledge_graph',
            ]


def test_alive():
    """ Check the /health endpoint of each server to check that it's alive.
    """
    unhealthy = False
    for server in servers:
        try:
            print(f'\ntest_alive: testing /health on {server}..... ')
            headers = {
                'User-Agent': 'Casey Testing 2024-07-26'
            }
            response = requests.get(urljoin(server, '/health'), timeout=10, headers=headers)

            if response.status_code == 200:
                print('\t' + response.text)
            else:
                print(f'\tUNHEALTHY: {response.status_code}\n{response.text}')
                unhealthy = True
        except Exception as e:
            print(f'UNHEALTHY: {str(e)}')
        time.sleep(2)
        
    for server in other_tests:
        try:
            print(f'\ntest_alive: testing /health on {server}..... ')
            headers = {
                'User-Agent': 'Casey Testing 2024-07-26'
            }
            response = requests.get(server, timeout=20, headers=headers)

            if response.status_code == 200:
                print('\tSuccessful')
            else:
                print(f'\tUNHEALTHY: {response.status_code}\n{response.text}')
                unhealthy = True
        except Exception as e:
            print(f'UNHEALTHY: {str(e)}')
        time.sleep(1)    

    # No server should be unhealthy
    assert not unhealthy
