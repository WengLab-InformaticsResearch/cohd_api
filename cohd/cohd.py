"""
Columbia Open Health Data (COHD) API

implemented in Flask

@author: Joseph D. Romano
@author: Rami Vanguri
@author: Choonhan Youn
@author: Casey Ta

(c) 2017 Tatonetti Lab
"""

from flask import request, redirect

from .google_analytics import GoogleAnalytics

# Flask app and cache
from .app import app, cache

# app needs to be loaded before loading other COHD modules
from . import query_cohd_mysql
from . import cohd_translator
from . import cohd_trapi
from . import scheduled_tasks


##########
# ROUTES #
##########


@app.route('/')
def website():
    google_analytics(endpoint='/')
    return redirect("site/index.html", code=302)


@app.route('/api')
@app.route('/api/')
def api_cohd():
    google_analytics(endpoint='/api')
    return redirect("https://cohdcovid.smart-api.info/", code=302)


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
    return api_call('translator', 'query', version=None)


@app.route('/api/<string:version>/query', methods=['POST'])
@app.route('/api/<string:version>/translator/query', methods=['POST'])
def api_translator_query_version(version):
    return api_call('translator', 'query', version=version)


@app.route('/api/predicates', methods=['GET'])
@app.route('/api/translator/predicates', methods=['GET'])
def api_translator_predicates():
    return api_call('translator', 'predicates')


@app.route('/api/meta_knowledge_graph', methods=['GET'])
@app.route('/api/translator/meta_knowledge_graph', methods=['GET'])
def api_translator_meta_knowledge_graph():
    return api_call('translator', 'meta_knowledge_graph')


@app.route('/api/translator/omop_to_biolink', methods=['POST'])
def api_translator_omop_to_biolink():
    return api_call('translator', 'omop_to_biolink')


@app.route('/api/translator/biolink_to_omop', methods=['POST'])
def api_translator_biolink_to_omop():
    return api_call('translator', 'biolink_to_omop')


@app.route('/api/dev/build_cache_map_from', methods=['GET'])
def api_internal_build_cache_map_from():
    return api_call('dev', 'build_cache_map_from')


@app.route('/api/dev/clear_cache', methods=['GET'])
def api_internal_clear_cache():
    return api_call('dev', 'clear_cache')


@app.route('/api/dev/clear_biolink_cache', methods=['GET'])
def api_internal_clear_biolink_cache():
    return api_call('dev', 'clear_biolink_cache')


@app.route('/api/dev/force_biolink_cache_update', methods=['GET'])
def api_internal_force_cahce_update():
    return api_call('dev', 'force_biolink_cache_update')


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
def api_call(service=None, meta=None, query=None, version=None):
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
            result = cohd_translator.translator_query(request, version)
        elif meta == 'predicates':
            result = cohd_translator.translator_predicates()
        elif meta == 'meta_knowledge_graph':
            result = cohd_translator.translator_meta_knowledge_graph()
        elif meta == 'omop_to_biolink':
            result = cohd_translator.omop_to_biolink(request)
        elif meta == 'biolink_to_omop':
            result = cohd_translator.biolink_to_omop(request)
        else:
            result = 'meta not recognized', 400
    elif service == 'dev':
        # Requires a key to run
        if 'DEV_KEY' in app.config and app.config['DEV_KEY'] == request.args.get('q', None):
            if meta == 'build_cache_map_from':
                result = cohd_trapi.BiolinkConceptMapper.build_cache_map_from()
            elif meta == 'clear_cache':
                cache.clear()
                result = 'Cleared cache', 200
            elif meta == 'clear_biolink_cache':
                cohd_trapi.BiolinkConceptMapper.clear_cache()
                result = 'Cleared Biolink cache', 200
            elif meta == 'force_biolink_cache_update':
                result = cohd_trapi.BiolinkConceptMapper.set_cache_update(request)
            else:
                result = 'meta not recognized', 400
        else:
            # Pretend like the 'dev' service doesn't exist
            result = 'service not recognized', 400
    else:
        result = 'service not recognized', 400

    # Report the API call to Google Analytics
    google_analytics(service=service, meta=meta)

    return result


if __name__ == "__main__":
    app.run(host='localhost')
