"""
Columbia Open Health Data (COHD) API

implemented in Flask

@author: Joseph D. Romano
@author: Rami Vanguri
@author: Choonhan Youn
@author: Casey Ta

(c) 2017 Tatonetti Lab
"""

from flask import Flask, request, redirect
from flask_cors import CORS

from .google_analytics import GoogleAnalytics
from . import query_cohd_mysql
from . import cohd_translator

#########
# INITS #
#########

app = Flask(__name__)
CORS(app)
app.config.from_pyfile('cohd_flask.conf')


##########
# ROUTES #
##########


@app.route('/')
@app.route('/api')
@app.route('/api/')
def api_cohd():
    google_analytics(endpoint='/')
    return redirect("http://cohdcovid.smart-api.info/", code=302)


@app.route('/api/omop/findConceptIDs')
@app.route('/api/v1/omop/findConceptIDs')
def api_omop_reference():
    return api_call('omop', 'findConceptIDs')


@app.route('/api/omop/concepts')
@app.route('/api/v1/omop/concepts')
def api_omop_concepts():
    return api_call('omop', 'concepts')


@app.route('/api/omop/conceptAncestors')
def api_omop_conceptAncestors():
    return api_call('omop', 'conceptAncestors')


@app.route('/api/omop/conceptDescendants')
def api_omop_conceptDescendants():
    return api_call('omop', 'conceptDescendants')


@app.route('/api/omop/mapToStandardConceptID')
def api_omop_mapToStandardConceptID():
    return api_call('omop', 'mapToStandardConceptID')


@app.route('/api/omop/mapFromStandardConceptID')
def api_omop_mapFromStandardConceptID():
    return api_call('omop', 'mapFromStandardConceptID')


@app.route('/api/omop/vocabularies')
def api_omop_vocabularies():
    return api_call('omop', 'vocabularies')


@app.route('/api/omop/xrefToOMOP')
def api_omop_xrefToOMOP():
    return api_call('omop', 'xrefToOMOP')


@app.route('/api/omop/xrefFromOMOP')
def api_omop_xrefFromOMOP():
    return api_call('omop', 'xrefFromOMOP')


@app.route('/api/metadata/datasets')
def api_metadata_datasets():
    return api_call('metadata', 'datasets')


@app.route('/api/metadata/domainCounts')
def api_metadata_domainCounts():
    return api_call('metadata', 'domainCounts')


@app.route('/api/metadata/domainPairCounts')
def api_metadata_domainPairCounts():
    return api_call('metadata', 'domainPairCounts')


@app.route('/api/metadata/visitCount')
def api_metadata_patientCount():
    return api_call('metadata', 'visitCount')


@app.route('/api/frequencies/singleConceptFreq')
@app.route('/api/v1/frequencies/singleConceptFreq')
def api_frequencies_singleConceptFreq():
    return api_call('frequencies', 'singleConceptFreq')


@app.route('/api/frequencies/pairedConceptFreq')
@app.route('/api/v1/frequencies/pairedConceptFreq')
def api_frequencies_pairedConceptFreq():
    return api_call('frequencies', 'pairedConceptFreq')


@app.route('/api/frequencies/associatedConceptFreq')
@app.route('/api/v1/frequencies/associatedConceptFreq')
def api_frequencies_associatedConceptFreq():
    return api_call('frequencies', 'associatedConceptFreq')


@app.route('/api/frequencies/associatedConceptDomainFreq')
@app.route('/api/v1/frequencies/associatedConceptDomainFreq')
def api_frequencies_associatedConceptDomainFreq():
    return api_call('frequencies', 'associatedConceptDomainFreq')


@app.route('/api/frequencies/mostFrequentConcepts')
@app.route('/api/v1/frequencies/mostFrequentConcepts')
def api_frequencies_mostFrequentConcept():
    return api_call('frequencies', 'mostFrequentConcepts')


@app.route('/api/association/chiSquare')
def api_association_chiSquare():
    return api_call('association', 'chiSquare')


@app.route('/api/association/obsExpRatio')
def api_association_obsExpRatio():
    return api_call('association', 'obsExpRatio')


@app.route('/api/association/relativeFrequency')
def api_association_relativeFrequency():
    return api_call('association', 'relativeFrequency')


@app.route('/api/query', methods=['POST'])
@app.route('/api/translator/query', methods=['POST'])
def api_translator_query():
    return api_call('translator', 'query')


@app.route('/api/predicates', methods=['GET'])
@app.route('/api/translator/predicates', methods=['GET'])
def api_transator_predicates():
    return api_call('translator', 'predicates')


# Retrieves the desired arg_names from args and stores them in the queries dictionary. Returns None if any of arg_names
# are missing
def args_to_query(args, arg_names):
    query = {}
    for arg_name in arg_names:
        arg_value = args[arg_name]
        if arg_value is None or arg_value == ['']:
            return None
        query[arg_name] = arg_value
    return query


def google_analytics(endpoint=None, service=None, meta=None):
    # Report to Google Analytics iff the tracking ID is specified in the configuration file
    if 'GA_TID' in app.config:
        tid = app.config['GA_TID']
        GoogleAnalytics.google_analytics(request, tid, endpoint, service, meta)


@app.route('/api/query')
@app.route('/api/v1/query')
def api_call(service=None, meta=None, query=None):
    if service is None:
        service = request.args.get('service')
    if meta is None:
        meta = request.args.get('meta')

    print("Service: ", service)
    print("Meta/Method: ", meta)

    if service == [''] or service is None:
        result = 'No service selected', 400
    elif service == 'metadata':
        if meta == 'datasets' or \
                meta == 'domainCounts' or \
                meta == 'domainPairCounts' or \
                meta == 'visitCount':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = 'meta not recognized', 400
    elif service == 'omop':
        if meta == 'findConceptIDs' or \
                meta == 'concepts' or \
                meta == 'conceptAncestors' or \
                meta == 'conceptDescendants' or \
                meta == 'mapToStandardConceptID' or \
                meta == 'mapFromStandardConceptID' or \
                meta == 'vocabularies' or \
                meta == 'xrefToOMOP' or \
                meta == 'xrefFromOMOP':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = 'meta not recognized', 400
    elif service == 'frequencies':
        if meta == 'singleConceptFreq' or \
                meta == 'pairedConceptFreq' or \
                meta == 'associatedConceptFreq' or \
                meta == 'mostFrequentConcepts' or \
                meta == 'associatedConceptDomainFreq':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = 'meta not recognized', 400
    elif service == 'association':
        if meta == 'chiSquare' or \
                meta == 'obsExpRatio' or \
                meta == 'relativeFrequency':
            result = query_cohd_mysql.query_db(service, meta, request.args)
        else:
            result = 'meta not recognized', 400
    elif service == 'translator':
        if meta == 'query':
            reasoner = cohd_translator.COHDTranslatorReasoner(request)
            result = reasoner.reason()
        elif meta == 'predicates':
            result = cohd_translator.translator_predicates()
        else:
            result = 'meta not recognized', 400
    else:
        result = 'service not recognized', 400

    # Report the API call to Google Analytics
    google_analytics(service=service, meta=meta)

    return result


if __name__ == "__main__":
    app.run(host='localhost')
