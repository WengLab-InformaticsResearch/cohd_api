from collections import defaultdict
from uuid import uuid4
import requests


class GoogleAnalytics:
    # Track IP addresses to assign UUIDs so that Google Analytics can somewhat differentiate users
    # Store in memory only
    uuids = defaultdict(uuid4)

    @staticmethod
    def google_analytics(request, tid, endpoint=None, service=None, meta=None):
        """ Reports the endpoint to Google Analytics

        Reports the endpoint as a pageview to Google Analytics. If endpoint is specified, then endpoint is reported.
        Otherwise, if service and meta are specified, then /api/{service}/{meta} is reported.

        Uses Google Analytics Measurement Protocol for reporting:
        https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide

        :param request: The FLASK request object to get remote_addr and user_agent
        :param endpoint: The endpoint to submit as the document page
        :param service: Combine with meta to submit /api/{service}/{meta} as the document page.
        :param meta: Combine with service to submit /api/{service}/{meta} as the document page.
        :return: None
        """

        # Report the endpoint if specified, otherwise /api/{service}/{meta}
        if endpoint is None:
            if service is None or meta is None:
                # Insufficient information.
                print 'Insufficient endpoint information for cohd.py::google_analytics'
                return

            endpoint = u'/api/{service}/{meta}'.format(service=service, meta=meta)

        try:
            # Use a small timeout so that the Google Analytics request does not cause delays if there is an issue
            endpoint_ga = u'http://www.google-analytics.com/collect'
            payload = {
                u'v': 1,
                u'tid': tid,
                u'cid': GoogleAnalytics.uuids[request.remote_addr],
                u't': u'pageview',
                u'dh': u'cohd.nsides.io',
                u'dp': endpoint,
                u'uip': request.remote_addr,
                u'ua': request.user_agent
            }
            requests.post(endpoint_ga, data=payload, timeout=0.1)

        except requests.exceptions.Timeout:
            # Log the timeout
            print 'Google Analytics timeout: ' + endpoint
