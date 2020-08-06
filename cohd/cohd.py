u"""
Columbia Open Health Data (COHD) API

implemented in Flask

@author: Joseph D. Romano
@author: Rami Vanguri
@author: Choonhan Youn
@author: Casey Ta

(c) 2017 Tatonetti Lab
"""

from flask import Flask, request, redirect, jsonify
from flask_cors import CORS
import query_cohd_mysql
from google_analytics import GoogleAnalytics
import cohd_temporal
import cohd_translator

#########
# INITS #
#########

app = Flask(__name__)
CORS(app)
app.config.from_pyfile(u'cohd_flask.conf')


##########
# ROUTES #
##########


@app.route(u'/')
@app.route(u'/api')
@app.route(u'/api/')
def api_cohd():
    google_analytics(endpoint=u'/')
    return redirect("http://cohd.smart-api.info/", code=302)


@app.route(u'/api/omop/findConceptIDs')
@app.route(u'/api/v1/omop/findConceptIDs')
def api_omop_reference():
    return api_call(u'omop', u'findConceptIDs')


@app.route(u'/api/omop/concepts')
@app.route(u'/api/v1/omop/concepts')
def api_omop_concepts():
    return api_call(u'omop', u'concepts')


@app.route(u'/api/omop/conceptAncestors')
def api_omop_conceptAncestors():
    return api_call(u'omop', u'conceptAncestors')


@app.route(u'/api/omop/conceptDescendants')
def api_omop_conceptDescendants():
    return api_call(u'omop', u'conceptDescendants')


@app.route(u'/api/omop/mapToStandardConceptID')
def api_omop_mapToStandardConceptID():
    return api_call(u'omop', u'mapToStandardConceptID')


@app.route(u'/api/omop/mapFromStandardConceptID')
def api_omop_mapFromStandardConceptID():
    return api_call(u'omop', u'mapFromStandardConceptID')


@app.route(u'/api/omop/vocabularies')
def api_omop_vocabularies():
    return api_call(u'omop', u'vocabularies')


@app.route(u'/api/omop/xrefToOMOP')
def api_omop_xrefToOMOP():
    return api_call(u'omop', u'xrefToOMOP')


@app.route(u'/api/omop/xrefFromOMOP')
def api_omop_xrefFromOMOP():
    return api_call(u'omop', u'xrefFromOMOP')


@app.route(u'/api/metadata/datasets')
def api_metadata_datasets():
    return api_call(u'metadata', u'datasets')


@app.route(u'/api/metadata/domainCounts')
def api_metadata_domainCounts():
    return api_call(u'metadata', u'domainCounts')


@app.route(u'/api/metadata/domainPairCounts')
def api_metadata_domainPairCounts():
    return api_call(u'metadata', u'domainPairCounts')


@app.route(u'/api/metadata/patientCount')
def api_metadata_patientCount():
    return api_call(u'metadata', u'patientCount')


@app.route(u'/api/frequencies/singleConceptFreq')
@app.route(u'/api/v1/frequencies/singleConceptFreq')
def api_frequencies_singleConceptFreq():
    return api_call(u'frequencies', u'singleConceptFreq')


@app.route(u'/api/frequencies/pairedConceptFreq')
@app.route(u'/api/v1/frequencies/pairedConceptFreq')
def api_frequencies_pairedConceptFreq():
    return api_call(u'frequencies', u'pairedConceptFreq')


@app.route(u'/api/frequencies/associatedConceptFreq')
@app.route(u'/api/v1/frequencies/associatedConceptFreq')
def api_frequencies_associatedConceptFreq():
    return api_call(u'frequencies', u'associatedConceptFreq')


@app.route(u'/api/frequencies/associatedConceptDomainFreq')
@app.route(u'/api/v1/frequencies/associatedConceptDomainFreq')
def api_frequencies_associatedConceptDomainFreq():
    return api_call(u'frequencies', u'associatedConceptDomainFreq')


@app.route(u'/api/frequencies/mostFrequentConcepts')
@app.route(u'/api/v1/frequencies/mostFrequentConcepts')
def api_frequencies_mostFrequentConcept():
    return api_call(u'frequencies', u'mostFrequentConcepts')


@app.route(u'/api/association/chiSquare')
def api_association_chiSquare():
    return api_call(u'association', u'chiSquare')


@app.route(u'/api/association/obsExpRatio')
def api_association_obsExpRatio():
    return api_call(u'association', u'obsExpRatio')


@app.route(u'/api/association/relativeFrequency')
def api_association_relativeFrequency():
    return api_call(u'association', u'relativeFrequency')


@app.route(u'/api/temporal/conceptAgeCounts')
def api_temporal_conceptAgeCounts():
    return api_call(u'temporal', u'conceptAgeCounts')


@app.route(u'/api/temporal/conceptPairDeltaCounts')
def api_temporal_conceptPairDeltaCounts():
    return api_call(u'temporal', u'conceptPairDeltaCounts')


@app.route(u'/api/temporal/findSimilarAgeDistributions')
def api_temporal_findSimilarAgeDistributions():
    return api_call(u'temporal', u'findSimilarAgeDistributions')


@app.route(u'/api/temporal/sourceToTarget')
def api_temporal_sourceToTarget():
    return api_call(u'temporal', u'sourceToTarget')


@app.route(u'/api/translator/query', methods=['POST'])
def api_translator_query():
    return api_call(u'translator', u'query')


# Retrieves the desired arg_names from args and stores them in the queries dictionary. Returns None if any of arg_names
# are missing
def args_to_query(args, arg_names):
    query = {}
    for arg_name in arg_names:
        arg_value = args[arg_name]
        if arg_value is None or arg_value == [u'']:
            return None
        query[arg_name] = arg_value
    return query


def google_analytics(endpoint=None, service=None, meta=None):
    # Report to Google Analytics iff the tracking ID is specified in the configuration file
    if u'GA_TID' in app.config:
        tid = app.config[u'GA_TID']
        GoogleAnalytics.google_analytics(request, tid, endpoint, service, meta)


@app.route(u'/api/query')
@app.route(u'/api/v1/query')
def api_call(service=None, meta=None, query=None):
    if service is None:
        service = request.args.get(u'service')
    if meta is None:
        meta = request.args.get(u'meta')

    print u"Service: ", service
    print u"Meta/Method: ", meta

    if service == [u''] or service is None:
        result = u'No service selected', 400
    elif service == u'metadata':
        if meta == u'datasets' or \
                meta == u'domainCounts' or \
                meta == u'domainPairCounts' or \
                meta == u'patientCount':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = u'meta not recognized', 400
    elif service == u'omop':
        if meta == u'findConceptIDs' or \
                meta == u'concepts' or \
                meta == u'conceptAncestors' or \
                meta == u'conceptDescendants' or \
                meta == u'mapToStandardConceptID' or \
                meta == u'mapFromStandardConceptID' or \
                meta == u'vocabularies' or \
                meta == u'xrefToOMOP' or \
                meta == u'xrefFromOMOP':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = u'meta not recognized', 400
    elif service == u'frequencies':
        if meta == u'singleConceptFreq' or \
                meta == u'pairedConceptFreq' or \
                meta == u'associatedConceptFreq' or \
                meta == u'mostFrequentConcepts' or \
                meta == u'associatedConceptDomainFreq':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = u'meta not recognized', 400
    elif service == u'association':
        if meta == u'chiSquare' or \
                meta == u'obsExpRatio' or \
                meta == u'relativeFrequency':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = u'meta not recognized', 400
    elif service == u'temporal':
        if meta == u'conceptAgeCounts' or \
                meta == u'conceptPairDeltaCounts' or \
                meta == u'findSimilarAgeDistributions' or \
                meta == u'sourceToTarget':
            result = cohd_temporal.query_cohd_temporal(service, meta, request.args)
        else:
            result = u'meta not recognized', 400
    elif service == u'translator':
        if meta == u'query':
            reasoner = cohd_translator.COHDTranslatorReasoner(request)
            result = reasoner.reason()
        else:
            result = u'meta not recognized', 400
    else:
        result = u'service not recognized', 400

    # Report the API call to Google Analytics
    google_analytics(service=service, meta=meta)

    return result


if __name__ == u"__main__":
    app.run(host=u'localhost')
