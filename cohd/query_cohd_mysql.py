import pymysql
from flask import jsonify
from scipy.stats import chisquare
from numpy import argsort

from .omop_xref import xref_to_omop_standard_concept, omop_map_to_standard, omop_map_from_standard, \
    xref_from_omop_standard_concept, xref_from_omop_local, xref_to_omop_local
from .cohd_utilities import ln_ratio_ci, rel_freq_ci

# Configuration
CONFIG_FILE = "cohd_mysql.cnf"  # log-in credentials for database
DATASET_ID_DEFAULT = 1
DATASET_ID_DEFAULT_HIER = 3
DEFAULT_CONFIDENCE = 0.99

# OXO API configuration
URL_OXO_SEARCH = 'https://www.ebi.ac.uk/spot/oxo/api/search'
_DEFAULT_OXO_DISTANCE = 2
DEFAULT_OXO_MAPPING_TARGETS = ["ICD9CM", "ICD10CM", "SNOMEDCT", "MeSH"]


def sql_connection():
    # Connect to MySQL database
    # print u"Connecting to MySQL database"
    return pymysql.connect(read_default_file=CONFIG_FILE,
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def get_arg_dataset_id(args, default_dataset_id=DATASET_ID_DEFAULT):
    dataset_id = args.get('dataset_id')
    if dataset_id is None or dataset_id.isspace() or not dataset_id.strip().isdigit():
        dataset_id = default_dataset_id
    else:
        dataset_id = int(dataset_id.strip())

    return dataset_id


def get_arg_concept_id(args, param_name='concept_id'):
    concept_id = args.get(param_name)
    if concept_id is None or concept_id == [''] or not concept_id.strip().isdigit():
        return None
    else:
        return int(concept_id)


def get_arg_int(args, param_name):
    param = args.get(param_name)
    if param is None or param == [''] or not param.strip().isnumeric():
        return None
    else:
        try:
            return int(param.strip())
        except ValueError:
            return None


def get_arg_float(args, param_name):
    param = args.get(param_name)
    if param is None or param == ['']:
        return None
    else:
        try:
            return float(param.strip())
        except ValueError:
            return None


def get_arg_boolean(args, param_name):
    param = args.get(param_name)
    if param is None or param == ['']:
        return None
    else:
        try:
            return param.strip().lower() in ['true', '1', 't']
        except AttributeError:
            return None
    return None


def query_db(service, method, args):

    # print u"Connecting to the MySQL API..."

    # Connect to MYSQL database
    conn = sql_connection()
    cur = conn.cursor()

    json_return = []

    query = args.get('q')

    print("Service: {s}; Method: {m}, Query: {q}".format(s=service, m=method, q=query))

    if service == 'metadata':
        # The datasets in the COHD database
        # endpoint: /api/v1/query?service=metadata&meta=datasets
        if method == 'datasets':
            sql = '''SELECT * 
                FROM cohd.dataset;'''
            cur.execute(sql)
            json_return = cur.fetchall()

        # The number of concepts in each domain
        # endpoint: /api/v1/query?service=metadata&meta=domainCounts&dataset_id=1
        elif method == 'domainCounts':
            dataset_id = get_arg_dataset_id(args)
            sql = '''SELECT * 
                FROM cohd.domain_concept_counts 
                WHERE dataset_id=%(dataset_id)s;'''
            params = {'dataset_id': dataset_id}
            cur.execute(sql, params)
            json_return = cur.fetchall()

        # The number of pairs of concepts in each pair of domains
        # endpoint: /api/v1/query?service=metadata&meta=domainPairCounts&dataset_id=1
        elif method == 'domainPairCounts':
            dataset_id = get_arg_dataset_id(args)
            sql = '''SELECT * 
                FROM cohd.domain_pair_concept_counts 
                WHERE dataset_id=%(dataset_id)s;'''
            params = {'dataset_id': dataset_id}
            cur.execute(sql, params)
            json_return = cur.fetchall()

        # The number of patients in the dataset
        # endpoint: /api/v1/query?service=metadata&meta=patientCount&dataset_id=1
        elif method == 'patientCount':
            dataset_id = get_arg_dataset_id(args)
            sql = '''SELECT * 
                FROM cohd.patient_count 
                WHERE dataset_id=%(dataset_id)s;'''
            params = {'dataset_id': dataset_id}
            cur.execute(sql, params)
            json_return = cur.fetchall()

    elif service == 'omop':
        # Find concept_ids and concept_names that are similar to the query
        # e.g. /api/v1/query?service=omop&meta=findConceptIDs&q=cancer
        if method == 'findConceptIDs':
            # Check query parameter
            if query is None or query == [''] or query.isspace():
                return 'q parameter is missing', 400

            dataset_id = get_arg_dataset_id(args)

            sql = '''SELECT c.concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code,
                    CAST(IFNULL(concept_count, 0) AS UNSIGNED) AS concept_count
                FROM cohd.concept c
                LEFT JOIN cohd.concept_counts cc ON (cc.dataset_id = %(dataset_id)s AND cc.concept_id = c.concept_id)
                WHERE concept_name like %(like_query)s AND standard_concept IN ('S','C') 
                    {domain_filter} 
                    {count_filter}
                ORDER BY cc.concept_count DESC
                LIMIT 1000;'''
            params = {
                'like_query': '%' + query + '%',
                'dataset_id': dataset_id,
                'query': query
            }

            # Filter concepts by domain
            domain_id = args.get('domain')
            if domain_id is None or domain_id == [''] or domain_id.isspace():
                domain_filter = ''
            else:
                domain_filter = 'AND domain_id = %(domain_id)s'
                params['domain_id'] = domain_id

            # Filter concepts by minimum count
            min_count = args.get('min_count')
            if min_count is None or min_count == ['']:
                # Default to set min_count = 1
                count_filter = 'AND cc.concept_count >= 1'
            else:
                if min_count.strip().isdigit():
                    min_count = int(min_count.strip())
                    if min_count > 0:
                        count_filter = 'AND cc.concept_count >= %(min_count)s'
                        params['min_count'] = min_count
                    else:
                        count_filter = ''
                else:
                    return 'min_count parameter should be an integer', 400

            sql = sql.format(domain_filter=domain_filter, count_filter=count_filter)

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Looks up concepts for a list of concept_ids
        # e.g. /api/v1/query?service=omop&meta=concepts&q=4196636,437643
        elif method == 'concepts':
            # Check query parameter
            if query is None or query == [''] or query.isspace():
                return 'q parameter is missing', 400
            for concept_id in query.split(','):
                if not concept_id.strip().isdigit():
                    return 'Error in q: concept_ids should be integers', 400

            # Convert query paramter to a list of concept ids
            concept_ids = [int(x.strip()) for x in query.split(',')]

            sql = '''SELECT concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, concept_code 
                FROM cohd.concept
                WHERE concept_id IN (%s);''' % ','.join(['%s' for _ in concept_ids])

            cur.execute(sql, concept_ids)
            json_return = cur.fetchall()

        # Looks up ancestors of a given concept
        # e.g. /api/query?service=omop&meta=conceptAncestors&concept_id=313217
        elif method == 'conceptAncestors':
            # Get non-required parameters
            dataset_id = get_arg_dataset_id(args, DATASET_ID_DEFAULT_HIER)

            # concept_id is required
            concept_id = args.get('concept_id')
            if concept_id is None or concept_id == [''] or not concept_id.strip().isdigit():
                return 'No concept_id specified', 400
            concept_id = int(concept_id)

            sql = '''SELECT ca.ancestor_concept_id, ca.min_levels_of_separation, ca.max_levels_of_separation, 
                    c.concept_name, c.domain_id, c.vocabulary_id, c.concept_class_id, c.standard_concept, 
                    c.concept_code, CAST(IFNULL(concept_count, 0) AS UNSIGNED) AS concept_count
                FROM concept_ancestor ca
                JOIN concept c ON ca.ancestor_concept_id = c.concept_id
                LEFT JOIN concept_counts cc ON ca.ancestor_concept_id = cc.concept_id
                WHERE ca.descendant_concept_id = %(concept_id)s
                    {vocabulary_filter}
                    {concept_class_filter}
                    AND (cc.dataset_id IS NULL OR cc.dataset_id = %(dataset_id)s)
                ORDER BY concept_count ASC
                LIMIT 1000;'''

            params = {
                'concept_id': concept_id,
                'dataset_id': dataset_id,
            }

            # Filter concepts by vocabulary
            vocabulary_id = args.get('vocabulary_id')
            if vocabulary_id is None or vocabulary_id == [''] or vocabulary_id.isspace():
                vocabulary_filter = ''
            else:
                vocabulary_filter = 'AND vocabulary_id = %(vocabulary_id)s'
                params['vocabulary_id'] = vocabulary_id

            # Filter concepts by concept_class
            concept_class_id = args.get('concept_class_id')
            if concept_class_id is None or concept_class_id == [''] or concept_class_id.isspace():
                concept_class_filter = ''
            else:
                concept_class_filter = 'AND concept_class_id = %(concept_class_id)s'
                params['concept_class_id'] = concept_class_id

            # Add filter code to SQL
            sql = sql.format(vocabulary_filter=vocabulary_filter, concept_class_filter=concept_class_filter)

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Looks up descendants of a given concept
        # e.g. /api/query?service=omop&meta=conceptDescendants&concept_id=313217
        elif method == 'conceptDescendants':
            # Get non-required parameters
            dataset_id = get_arg_dataset_id(args, DATASET_ID_DEFAULT_HIER)

            # concept_id is required
            concept_id = args.get('concept_id')
            if concept_id is None or concept_id == [''] or not concept_id.strip().isdigit():
                return 'No concept_id specified', 400
            concept_id = int(concept_id)

            sql = '''SELECT ca.descendant_concept_id, ca.min_levels_of_separation, ca.max_levels_of_separation, 
                    c.concept_name, c.domain_id, c.vocabulary_id, c.concept_class_id, c.standard_concept, 
                    c.concept_code, CAST(IFNULL(concept_count, 0) AS UNSIGNED) AS concept_count
                FROM concept_ancestor ca
                JOIN concept c ON ca.descendant_concept_id = c.concept_id
                LEFT JOIN concept_counts cc ON ca.descendant_concept_id = cc.concept_id
                WHERE ca.ancestor_concept_id = %(concept_id)s
                    {vocabulary_filter}
                    {concept_class_filter}
                    AND (cc.dataset_id IS NULL OR cc.dataset_id = %(dataset_id)s)
                ORDER BY concept_count DESC
                LIMIT 1000;'''

            params = {
                'concept_id': concept_id,
                'dataset_id': dataset_id,
            }

            # Filter concepts by vocabulary
            vocabulary_id = args.get('vocabulary_id')
            if vocabulary_id is None or vocabulary_id == [''] or vocabulary_id.isspace():
                vocabulary_filter = ''
            else:
                vocabulary_filter = 'AND vocabulary_id = %(vocabulary_id)s'
                params['vocabulary_id'] = vocabulary_id

            # Filter concepts by concept_class
            concept_class_id = args.get('concept_class_id')
            if concept_class_id is None or concept_class_id == [''] or concept_class_id.isspace():
                concept_class_filter = ''
            else:
                concept_class_filter = 'AND concept_class_id = %(concept_class_id)s'
                params['concept_class_id'] = concept_class_id

            # Add filter code to SQL
            sql = sql.format(vocabulary_filter=vocabulary_filter, concept_class_filter=concept_class_filter)

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Find concept_ids and concept_names that are similar to the query
        # e.g. /api/v1/query?service=omop&meta=mapToStandardConceptID&concept_code=715.3&vocabulary_id=ICD9CM
        elif method == 'mapToStandardConceptID':
            # Check concept_code parameter
            concept_code = args.get('concept_code')
            if concept_code is None or concept_code == [''] or concept_code.isspace():
                return 'No concept_code was specified', 400

            # Check vocabulary_id parameter
            vocabulary_id = args.get('vocabulary_id')
            if vocabulary_id is None or vocabulary_id == [''] or vocabulary_id.isspace():
                vocabulary_id = None

            # Map
            json_return = omop_map_to_standard(cur, concept_code, vocabulary_id)

        # Find concept_ids and concept_names that are similar to the query
        # e.g. /api/v1/query?service=omop&meta=mapFromStandardConceptID&concept_code=715.3&vocabulary_id=ICD9CM
        elif method == 'mapFromStandardConceptID':
            # Get concept_id parameter
            concept_id = args.get('concept_id')
            if concept_id is None or concept_id == ['']:
                return 'No concept_id was specified', 400

            # Get vocabulary_id parameter
            vocabulary_id = args.get('vocabulary_id')
            if vocabulary_id is not None:
                if vocabulary_id == ['']:
                    vocabulary_id = None
                else:
                    vocabulary_id = [x.strip() for x in vocabulary_id.split(',')]

            # Map
            json_return = omop_map_from_standard(cur, concept_id, vocabulary_id)

        # List of vocabularies
        # e.g. /api/v1/query?service=omop&meta=vocabularies
        elif method == 'vocabularies':
            sql = '''SELECT DISTINCT vocabulary_id FROM concept;'''
            cur.execute(sql)
            json_return = cur.fetchall()

        # Cross reference to OMOP using OXO service
        # e.g. /api/v1/query?service=omop&meta=xrefToOMOP?curie=DOID:8398&distance=1
        elif method == 'xrefToOMOP':
            # curie is required
            curie = args.get('curie')
            if curie is None or curie == ['']:
                return 'No curie was specified', 400

            distance = args.get('distance')
            if distance is None or distance == [''] or not distance.isdigit():
                distance = _DEFAULT_OXO_DISTANCE
            else:
                distance = int(distance)

            # check whether user wants recommended mappings (true) or all mappings (false)
            recommend = args.get('recommend')
            best = False
            if recommend is not None and recommend.strip().lower() == 'true':
                best = True

            # Check if user wants to use OxO API or local OxO implementation
            local_oxo = args.get('local')
            if local_oxo is not None and local_oxo.lower() == 'true':
                json_return = xref_to_omop_local(cur, curie, distance, best)
            else:
                json_return = xref_to_omop_standard_concept(cur, curie, distance, best)

        # Cross reference from OMOP using OXO service
        # e.g. /api/v1/query?service=omop&meta=xrefFromOMOP?concept_id=192855&distance=1
        elif method == 'xrefFromOMOP':
            # curie is required
            concept_id = args.get('concept_id')
            if concept_id is None or concept_id == [''] or not concept_id.strip().isdigit():
                return 'No curie was specified', 400
            else:
                concept_id = int(concept_id)

            # get mapping_targets, if specified
            mapping_targets = args.get('mapping_targets')
            if mapping_targets is None or mapping_targets == ['']:
                mapping_targets = []
            else:
                # convert to list of mapping targets
                mapping_targets = [x.strip() for x in mapping_targets.split(',')]

            # get distance, if specified
            distance = args.get('distance')
            if distance is None or distance == [''] or not distance.isdigit():
                distance = _DEFAULT_OXO_DISTANCE
            else:
                distance = int(distance)

            # check whether user wants recommended mappings (true) or all mappings (false)
            recommend = args.get('recommend')
            best = False
            if recommend is not None and recommend.strip().lower() == 'true':
                best = True

            # Check if user wants to use OxO API or local OxO implementation
            local_oxo = args.get('local')
            if local_oxo is not None and local_oxo.strip().lower() == 'true':
                json_return = xref_from_omop_local(cur, concept_id, mapping_targets, distance, best)
            else:
                json_return = xref_from_omop_standard_concept(cur, concept_id, mapping_targets, distance, best)

    elif service == 'frequencies':
        # Looks up observed clinical frequencies for a comma separated list of concepts
        # e.g. /api/v1/query?service=frequencies&meta=singleConceptFreq&dataset_id=1&q=4196636,437643
        if method == 'singleConceptFreq':
            dataset_id = get_arg_dataset_id(args)

            # Check concept_ids parameter
            if query is None or query == [''] or query.isspace():
                return 'q parameter is missing', 400

            for x in query.split(','):
                if not x.strip().isdigit():
                    return 'Error in q: concept_ids should be integers', 400

            # Convert query parameter to list of concept IDs
            concept_ids = [int(x.strip()) for x in query.split(',') if x.strip().isdigit()]

            sql = '''SELECT 
                    cc.dataset_id,
                    cc.concept_id,
                    cc.concept_count,
                    cc.concept_count / (pc.count + 0E0) AS concept_frequency
                FROM cohd.concept_counts cc
                JOIN cohd.patient_count pc ON cc.dataset_id = pc.dataset_id
                WHERE cc.dataset_id = %s AND concept_id IN ({concepts});'''.format(
                concepts=','.join(['%s' for _ in concept_ids]))
            params = [dataset_id] + concept_ids

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Looks up observed clinical frequencies for a comma separated list of concepts
        # e.g. /api/v1/query?service=frequencies&meta=pairedConceptFreq&dataset_id=1&q=4196636,437643
        elif method == 'pairedConceptFreq':
            dataset_id = get_arg_dataset_id(args)

            # Check q parameter
            if query is None or query == [''] or query.isspace():
                return 'q parameter is missing', 400

            # q parameter should be 2 concept_ids separated by comma
            qs = query.split(',')
            if len(qs) != 2 or not qs[0].strip().isdigit() or not qs[1].strip().isdigit():
                return 'Error in q: should be two concept IDs, e.g., 4196636,437643', 400

            concept_id_1 = int(qs[0])
            concept_id_2 = int(qs[1])
            sql = '''SELECT 
                    cpc.dataset_id,
                    cpc.concept_id_1,
                    cpc.concept_id_2,
                    cpc.concept_count,
                    cpc.concept_count / (pc.count + 0E0) AS concept_frequency
                FROM cohd.concept_pair_counts cpc
                JOIN cohd.patient_count pc ON pc.dataset_id = cpc.dataset_id
                WHERE cpc.dataset_id = %(dataset_id)s AND  
                    ((concept_id_1 = %(concept_id_1)s AND concept_id_2 = %(concept_id_2)s) OR 
                    (concept_id_1 = %(concept_id_2)s AND concept_id_2 = %(concept_id_1)s));'''
            params = {
                'dataset_id': dataset_id,
                'concept_id_1': concept_id_1,
                'concept_id_2': concept_id_2
            }

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Looks up observed clinical frequencies of all pairs of concepts given a concept id
        # e.g. /api/v1/query?service=frequencies&meta=associatedConceptFreq&dataset_id=1&q=4196636
        elif method == 'associatedConceptFreq':
            dataset_id = get_arg_dataset_id(args)

            # Check q parameter
            if query is None or query == [''] or query.isspace():
                return 'q parameter is missing', 400

            if not query.strip().isdigit():
                return 'Error in q: concept_id should be an integer'

            concept_id = int(query)

            sql = '''SELECT *
                FROM
                    ((SELECT 
                        cpc.dataset_id, 
                        cpc.concept_id_1 AS concept_id,
                        cpc.concept_id_2 AS associated_concept_id,                    
                        cpc.concept_count, 
                        cpc.concept_count / (pc.count + 0E0) AS concept_frequency,
                        c.concept_name AS associated_concept_name, 
                        c.domain_id AS associated_domain_id
                    FROM cohd.concept_pair_counts cpc
                    JOIN cohd.concept c ON concept_id_2 = c.concept_id     
                    JOIN cohd.patient_count pc ON cpc.dataset_id = pc.dataset_id          
                    WHERE cpc.dataset_id = %(dataset_id)s AND concept_id_1 = %(concept_id)s)
                    UNION
                    (SELECT 
                        cpc.dataset_id, 
                        cpc.concept_id_2 AS concept_id,
                        cpc.concept_id_1 AS associated_concept_id,                    
                        cpc.concept_count, 
                        cpc.concept_count / (pc.count + 0E0) AS concept_frequency,
                        c.concept_name AS associated_concept_name, 
                        c.domain_id AS associated_domain_id
                    FROM cohd.concept_pair_counts cpc
                    JOIN cohd.concept c ON concept_id_1 = c.concept_id             
                    JOIN cohd.patient_count pc ON cpc.dataset_id = pc.dataset_id      
                    WHERE cpc.dataset_id = %(dataset_id)s AND concept_id_2 = %(concept_id)s)) x
                ORDER BY concept_count DESC;'''
            params = {
                'dataset_id': dataset_id,
                'concept_id': concept_id
            }

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Looks up observed clinical frequencies of all pairs of concepts given a concept id restricted by domain of the
        # associated concept_id
        # e.g. /api/v1/query?service=frequencies&meta=associatedConceptDomainFreq&dataset_id=1&concept_id=4196636&domain=Procedure
        elif method == 'associatedConceptDomainFreq':
            dataset_id = get_arg_dataset_id(args)
            concept_id = args.get('concept_id')
            domain_id = args.get('domain')

            if concept_id is None or concept_id == [''] or concept_id.isspace():
                return 'No concept_id selected', 400

            if domain_id is None or domain_id == [''] or domain_id.isspace():
                return 'No domain selected', 400

            if not concept_id.strip().isdigit():
                return 'concept_id should be numeric', 400

            concept_id = int(concept_id)

            sql = '''SELECT *
                FROM
                    ((SELECT 
                        cpc.dataset_id, 
                        cpc.concept_id_1 AS concept_id,
                        cpc.concept_id_2 AS associated_concept_id,                    
                        cpc.concept_count, 
                        cpc.concept_count / (pc.count + 0E0) AS concept_frequency,
                        c.concept_name AS associated_concept_name, 
                        c.domain_id AS associated_domain_id
                    FROM cohd.concept_pair_counts cpc
                    JOIN cohd.concept c ON concept_id_2 = c.concept_id     
                    JOIN cohd.patient_count pc ON cpc.dataset_id = pc.dataset_id          
                    WHERE cpc.dataset_id = %(dataset_id)s AND concept_id_1 = %(concept_id)s
                        AND c.domain_id = %(domain_id)s)
                    UNION
                    (SELECT 
                        cpc.dataset_id, 
                        cpc.concept_id_2 AS concept_id,
                        cpc.concept_id_1 AS associated_concept_id,                    
                        cpc.concept_count, 
                        cpc.concept_count / (pc.count + 0E0) AS concept_frequency,
                        c.concept_name AS associated_concept_name, 
                        c.domain_id AS associated_domain_id
                    FROM cohd.concept_pair_counts cpc
                    JOIN cohd.concept c ON concept_id_1 = c.concept_id             
                    JOIN cohd.patient_count pc ON cpc.dataset_id = pc.dataset_id      
                    WHERE cpc.dataset_id = %(dataset_id)s AND concept_id_2 = %(concept_id)s
                        AND c.domain_id = %(domain_id)s)) x
                ORDER BY concept_count DESC;'''
            params = {
                'dataset_id': dataset_id,
                'concept_id': concept_id,
                'domain_id': domain_id
            }

            cur.execute(sql, params)
            json_return = cur.fetchall()

        # Returns most common single concept frequencies
        # e.g. /api/v1/query?service=frequencies&meta=mostFrequentConcept&dataset_id=1&q=100
        elif method == 'mostFrequentConcepts':
            sql = '''SELECT cc.dataset_id, 
                        cc.concept_id, 
                        cc.concept_count, 
                        cc.concept_count / (pc.count + 0E0) AS concept_frequency,
                        c.domain_id, c.concept_name, c.vocabulary_id, c.concept_class_id
                    FROM cohd.concept_counts cc
                    JOIN cohd.concept c ON cc.concept_id = c.concept_id
                    JOIN cohd.patient_count pc ON cc.dataset_id = pc.dataset_id
                    WHERE cc.dataset_id = %(dataset_id)s
                        {domain_filter}
                        {vocabulary_filter}
                        {concept_class_filter}
                    ORDER BY concept_count DESC 
                    {limit}
                    ;    
                    '''

            # Get dataset_id
            dataset_id = get_arg_dataset_id(args)
            params = {
                'dataset_id': dataset_id
            }

            # Check q parameter (limit)
            if query is None or query == [''] or query.isspace() or not query.strip().isdigit():
                limit = ''
            else:
                limit_n = int(query)
                if limit_n > 0:
                    limit = 'LIMIT %(limit_n)s'
                    params['limit_n'] = limit_n
                else:
                    limit = ''

            # Check domain parameter
            domain_id = args.get('domain')
            if domain_id is None or domain_id == [''] or domain_id.isspace():
                domain_filter = ''
            else:
                domain_filter = 'AND c.domain_id = %(domain_id)s'
                params['domain_id'] = domain_id

            # Filter concepts by vocabulary
            vocabulary_ids = args.get('vocabulary_id')
            if vocabulary_ids is None or vocabulary_ids == [''] or vocabulary_ids.isspace():
                vocabulary_filter = ''
            else:
                vids = []
                for i, vocabulary_id in enumerate(vocabulary_ids.split(',')):
                    vid = 'vid{x}'.format(x=i)
                    vids.append('%({x})s'.format(x=vid))
                    params[vid] = vocabulary_id
                vocabulary_filter = 'AND vocabulary_id IN ({vids})'.format(vids=','.join(vids))

            # Filter concepts by concept_class
            concept_class_ids = args.get('concept_class_id')
            if concept_class_ids is None or concept_class_ids == [''] or concept_class_ids.isspace():
                concept_class_filter = ''
            else:
                ccids = []
                for i, concept_class_id in enumerate(concept_class_ids.split(',')):
                    ccid = 'ccid{x}'.format(x=i)
                    ccids.append('%({x})s'.format(x=ccid))
                    params[ccid] = concept_class_id
                concept_class_filter = 'AND concept_class_id IN ({ccids})'.format(ccids=','.join(ccids))

            # Add filter code to SQL
            sql = sql.format(limit=limit, domain_filter=domain_filter, vocabulary_filter=vocabulary_filter,
                             concept_class_filter=concept_class_filter)

            cur.execute(sql, params)
            json_return = cur.fetchall()

    elif service == 'association':
        # Returns chi-square between pairs of concepts
        # e.g. /api/v1/query?service=association&meta=chiSquare&dataset_id=1&concept_id_1=192855&concept_id_2=2008271
        if method == 'chiSquare':
            # Get non-required parameters
            dataset_id = get_arg_dataset_id(args)
            concept_id_2 = args.get('concept_id_2')
            domain_id = args.get('domain')

            # concept_id_1 is required
            concept_id_1 = args.get('concept_id_1')
            if concept_id_1 is None or concept_id_1 == [''] or not concept_id_1.strip().isdigit():
                return 'No concept_id_1 selected', 400
            concept_id_1 = int(concept_id_1)

            # Get the total number of pairs for Bonferonni adjustment
            sql = '''SELECT SUM(count) AS pair_count
                FROM domain_pair_concept_counts
                WHERE dataset_id = %(dataset_id)s;'''
            params = {'dataset_id': dataset_id}
            cur.execute(sql, params)
            results = cur.fetchall()
            pair_count = int(results[0]['pair_count'])

            if concept_id_2 is not None and concept_id_2.strip().isdigit():
                # concept_id_2 is specified, only return the chi-square for the pair (concept_id_1, concept_id_2)
                concept_id_2 = int(concept_id_2)
                sql = '''SELECT 
                        cp.dataset_id, 
                        cp.concept_id_1, 
                        cp.concept_id_2,
                        cp.concept_count AS concept_pair_count,
                        c1.concept_count AS concept_count_1,
                        c2.concept_count AS concept_count_2,
                        pc.count AS patient_count
                    FROM cohd.concept_pair_counts cp
                    JOIN cohd.concept_counts c1 ON cp.concept_id_1 = c1.concept_id
                    JOIN cohd.concept_counts c2 ON cp.concept_id_2 = c2.concept_id
                    JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id
                    WHERE cp.dataset_id = %(dataset_id)s 
                        AND c1.dataset_id = %(dataset_id)s 
                        AND c2.dataset_id = %(dataset_id)s
                        AND cp.concept_id_1 IN (%(concept_id_1)s, %(concept_id_2)s)
                        AND cp.concept_id_2 IN (%(concept_id_1)s, %(concept_id_2)s);'''
                params = {
                    'dataset_id': dataset_id,
                    'concept_id_1': concept_id_1,
                    'concept_id_2': concept_id_2
                }

            else:
                # If concept_id_2 is not specified, get results for all pairs that include concept_id_1
                concept_id_2 = None
                sql = '''SELECT * 
                    FROM
                        ((SELECT 
                            cp.dataset_id, 
                            cp.concept_id_1, 
                            cp.concept_id_2,
                            cp.concept_count AS concept_pair_count,
                            c1.concept_count AS concept_count_1,
                            c2.concept_count AS concept_count_2,
                            pc.count AS patient_count,
                            c.concept_name AS concept_2_name, 
                            c.domain_id AS concept_2_domain
                        FROM cohd.concept_pair_counts cp
                        JOIN cohd.concept_counts c1 ON cp.concept_id_1 = c1.concept_id
                        JOIN cohd.concept_counts c2 ON cp.concept_id_2 = c2.concept_id
                        JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id
                        JOIN cohd.concept c ON cp.concept_id_2 = c.concept_id
                        WHERE cp.dataset_id = %(dataset_id)s 
                            AND c1.dataset_id = %(dataset_id)s 
                            AND c2.dataset_id = %(dataset_id)s
                            AND cp.concept_id_1 = %(concept_id_1)s 
                            {domain_filter})
                        UNION
                        (SELECT 
                            cp.dataset_id, 
                            cp.concept_id_2 AS concept_id_1, 
                            cp.concept_id_1 AS concept_id_2,
                            cp.concept_count AS concept_pair_count,
                            c2.concept_count AS concept_count_1,
                            c1.concept_count AS concept_count_2,
                            pc.count AS patient_count,
                            c.concept_name AS concept_2_name, 
                            c.domain_id AS concept_2_domain
                        FROM cohd.concept_pair_counts cp
                        JOIN cohd.concept_counts c1 ON cp.concept_id_1 = c1.concept_id
                        JOIN cohd.concept_counts c2 ON cp.concept_id_2 = c2.concept_id
                        JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id
                        JOIN cohd.concept c ON cp.concept_id_1 = c.concept_id
                        WHERE cp.dataset_id = %(dataset_id)s 
                            AND c1.dataset_id = %(dataset_id)s 
                            AND c2.dataset_id = %(dataset_id)s
                            AND cp.concept_id_2 = %(concept_id_1)s 
                            {domain_filter})) x;'''
                params = {
                    'dataset_id': dataset_id,
                    'concept_id_1': concept_id_1
                }

                if domain_id is not None and not domain_id == ['']:
                    domain_filter = 'AND c.domain_id = %(domain_id)s'
                    params['domain_id'] = domain_id
                else:
                    domain_filter = ''
                sql = sql.format(domain_filter=domain_filter)

            cur.execute(sql, params)
            results = cur.fetchall()

            # Calculate the p-value using chi-square distribution with 1 degree of freedom
            chi_squares = []
            for r in results:
                # Get observed counts
                cpc = float(r['concept_pair_count'])
                c1 = float(r['concept_count_1'])
                c2 = float(r['concept_count_2'])
                pts = float(r['patient_count'])
                neg = pts - c1 - c2 + cpc

                # Create the observed and expected RxC tables and perform chi-square
                o = [neg, c1 - cpc, c2 - cpc, cpc]
                e = [(pts - c1) * (pts - c2) / pts, c1 * (pts - c2) / pts, c2 * (pts - c1) / pts, c1 * c2 / pts]
                cs = chisquare(o, e, 2)
                new_r = {
                    'dataset_id': r['dataset_id'],
                    'concept_id_1': r['concept_id_1'],
                    'concept_id_2': r['concept_id_2'],
                    'n': int(pts),
                    'n_c1': int(c1),
                    'n_c2': int(c2),
                    'n_~c1_~c2': int(neg),
                    'n_c1_~c2': int(c1 - cpc),
                    'n_~c1_c2': int(c2 - cpc),
                    'n_c1_c2': int(cpc),
                    'chi_square': cs.statistic,
                    'p-value': cs.pvalue,
                    'adj_p-value': min(cs.pvalue * pair_count, 1.0)
                }
                if concept_id_2 is None:
                    new_r['concept_2_name'] = r['concept_2_name']
                    new_r['concept_2_domain'] = r['concept_2_domain']

                json_return.append(new_r)
                chi_squares.append(cs.statistic)

            # Sort results by chi-square
            json_return = [json_return[i] for i in list(reversed(argsort(chi_squares)))]

        # Returns ratio of observed to expected frequency between pairs of concepts
        # e.g. /api/v1/query?service=association&meta=obsExpRatio&dataset_id=1&concept_id_1=192855&concept_id_2=2008271
        elif method == 'obsExpRatio':
            # Get non-required parameters
            dataset_id = get_arg_dataset_id(args)
            domain_id = args.get('domain')

            # concept_id_1 is required
            concept_id_1 = get_arg_concept_id(args, 'concept_id_1')
            if concept_id_1 is None:
                return 'No concept_id_1 selected', 400

            concept_id_2 = get_arg_concept_id(args, 'concept_id_2')
            if concept_id_2 is not None:
                # concept_id_2 is specified, only return the results for the pair (concept_id_1, concept_id_2)
                concept_id_2 = int(concept_id_2)
                order = concept_id_1 < concept_id_2
                sql = '''SELECT 
                        cp.dataset_id, 
                        cp.concept_id_1 AS {rename_1}, 
                        cp.concept_id_2 AS {rename_2},
                        cp.concept_count AS observed_count,
                        c1.concept_count * c2.concept_count / (pc.count + 0E0) AS expected_count,
                        log(cp.concept_count * pc.count / (c1.concept_count * c2.concept_count + 0E0)) AS ln_ratio
                    FROM cohd.concept_pair_counts cp
                    JOIN cohd.concept_counts c1 ON cp.concept_id_1 = c1.concept_id
                    JOIN cohd.concept_counts c2 ON cp.concept_id_2 = c2.concept_id
                    JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id
                    WHERE cp.dataset_id = %(dataset_id)s 
                        AND c1.dataset_id = %(dataset_id)s 
                        AND c2.dataset_id = %(dataset_id)s
                        AND cp.concept_id_1 = %(concept_id_1)s
                        AND cp.concept_id_2 = %(concept_id_2)s;'''
                params = {
                    'dataset_id': dataset_id,
                    'concept_id_1': concept_id_1 if order else concept_id_2,
                    'concept_id_2': concept_id_2 if order else concept_id_1
                }
                rename_1 = 'concept_id_1'
                rename_2 = 'concept_id_2'
                if not order:
                    rename_1 = 'concept_id_2'
                    rename_2 = 'concept_id_1'
                sql = sql.format(rename_1=rename_1, rename_2=rename_2)

            else:
                # If concept_id_2 is not specified, get results for all pairs that include concept_id_1
                sql = '''SELECT * 
                    FROM
                        ((SELECT 
                            cp.dataset_id, 
                            cp.concept_id_1, 
                            cp.concept_id_2,
                            cp.concept_count AS observed_count,
                            c1.concept_count * c2.concept_count / (pc.count + 0E0) AS expected_count,
                            log(cp.concept_count * pc.count / (c1.concept_count * c2.concept_count + 0E0)) AS ln_ratio,
                            c.concept_name AS concept_2_name, 
                            c.domain_id AS concept_2_domain
                        FROM cohd.concept_pair_counts cp
                        JOIN cohd.concept_counts c1 ON cp.concept_id_1 = c1.concept_id
                        JOIN cohd.concept_counts c2 ON cp.concept_id_2 = c2.concept_id
                        JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id
                        JOIN cohd.concept c ON cp.concept_id_2 = c.concept_id
                        WHERE cp.dataset_id = %(dataset_id)s 
                            AND c1.dataset_id = %(dataset_id)s 
                            AND c2.dataset_id = %(dataset_id)s
                            AND cp.concept_id_1 = %(concept_id_1)s 
                            {domain_filter})
                        UNION
                        (SELECT 
                            cp.dataset_id, 
                            cp.concept_id_2 AS concept_id_1, 
                            cp.concept_id_1 AS concept_id_2,
                            cp.concept_count AS observed_count,
                            c1.concept_count * c2.concept_count / (pc.count + 0E0) AS expected_count,
                            log(cp.concept_count * pc.count / (c1.concept_count * c2.concept_count + 0E0)) AS ln_ratio,
                            c.concept_name AS concept_2_name, 
                            c.domain_id AS concept_2_domain
                        FROM cohd.concept_pair_counts cp
                        JOIN cohd.concept_counts c1 ON cp.concept_id_1 = c1.concept_id
                        JOIN cohd.concept_counts c2 ON cp.concept_id_2 = c2.concept_id
                        JOIN cohd.patient_count pc ON cp.dataset_id = pc.dataset_id
                        JOIN cohd.concept c ON cp.concept_id_1 = c.concept_id
                        WHERE cp.dataset_id = %(dataset_id)s 
                            AND c1.dataset_id = %(dataset_id)s 
                            AND c2.dataset_id = %(dataset_id)s
                            AND cp.concept_id_2 = %(concept_id_1)s 
                            {domain_filter})) x
                    ORDER BY ln_ratio DESC;'''
                params = {
                    'dataset_id': dataset_id,
                    'concept_id_1': concept_id_1,
                }

                if domain_id is not None and not domain_id == ['']:
                    # Restrict the associated concept by domain
                    domain_filter = 'AND c.domain_id = %(domain_id)s'
                    params['domain_id'] = domain_id
                else:
                    # Unrestricted domain
                    domain_filter = ''
                sql = sql.format(domain_filter=domain_filter)

            cur.execute(sql, params)
            json_return = cur.fetchall()

            # Add confidence interval to results
            confidence_level = args.get('confidence', DEFAULT_CONFIDENCE)
            try:
                confidence_level = float(confidence_level)
            except ValueError:
                return 'Confidence is not a number 0-1', 400
            if confidence_level < 0 or confidence_level >= 1:
                return 'Confidence should be a number between 0-1'
            for row in json_return:
                ci = ln_ratio_ci(row['observed_count'], row['ln_ratio'], confidence_level)
                # The lower bound may hit -Inf which causes issues with JSON serialization. Limit it to -999
                row['confidence_interval'] = max(ci[0], -999), ci[1]


        # Returns relative frequency between pairs of concepts
        # e.g. /api/v1/query?service=association&meta=relativeFrequency&dataset_id=1&concept_id_1=192855&concept_id_2=2008271
        elif method == 'relativeFrequency':
            # Get non-required parameters
            dataset_id = get_arg_dataset_id(args)
            concept_id_2 = args.get('concept_id_2')
            domain_id = args.get('domain')

            # concept_id_1 is required
            concept_id_1 = args.get('concept_id_1')
            if concept_id_1 is None or concept_id_1 == [''] or not concept_id_1.strip().isdigit():
                return 'No concept_id_1 selected', 400

            if concept_id_2 is not None and concept_id_2.strip().isdigit():
                # concept_id_2 is specified, only return the results for the pair (concept_id_1, concept_id_2)
                sql = '''(SELECT
                        cp.dataset_id,
                        cp.concept_id_1,
                        cp.concept_id_2,
                        cp.concept_count AS concept_pair_count,
                        cc.concept_count AS concept_2_count,
                        cp.concept_count / (cc.concept_count + 0E0) AS relative_frequency
                    FROM cohd.concept_pair_counts cp
                    JOIN cohd.concept_counts cc ON cp.concept_id_2 = cc.concept_id
                    WHERE cp.dataset_id = %(dataset_id)s
                        AND cc.dataset_id = %(dataset_id)s
                        AND cp.concept_id_1 = %(concept_id_1)s
                        AND cp.concept_id_2 = %(concept_id_2)s)
                    UNION
                    (SELECT
                        cp.dataset_id,
                        cp.concept_id_2 AS concept_id_1,
                        cp.concept_id_1 AS concept_id_2,
                        cp.concept_count AS concept_pair_count,
                        cc.concept_count AS concept_2_count,
                        cp.concept_count / (cc.concept_count + 0E0) AS relative_frequency
                    FROM cohd.concept_pair_counts cp
                    JOIN cohd.concept_counts cc ON cp.concept_id_1 = cc.concept_id
                    WHERE cp.dataset_id = %(dataset_id)s
                        AND cc.dataset_id = %(dataset_id)s
                        AND cp.concept_id_1 = %(concept_id_2)s
                        AND cp.concept_id_2 = %(concept_id_1)s);'''
                params = {
                    'dataset_id': dataset_id,
                    'concept_id_1': concept_id_1,
                    'concept_id_2': int(concept_id_2)
                }

            else:
                # If concept_id_2 is not specified, get results for all pairs that include concept_id_1
                sql = '''SELECT *
                    FROM
                        ((SELECT
                            cp.dataset_id,
                            cp.concept_id_1,
                            cp.concept_id_2,
                            cp.concept_count AS concept_pair_count,
                            cc.concept_count AS concept_2_count,
                            cp.concept_count / (cc.concept_count + 0E0) AS relative_frequency,
                            c.concept_name AS concept_2_name,
                            c.domain_id AS concept_2_domain
                        FROM cohd.concept_pair_counts cp
                        JOIN cohd.concept_counts cc ON cp.concept_id_2 = cc.concept_id
                        JOIN cohd.concept c ON cp.concept_id_2 = c.concept_id
                        WHERE cp.dataset_id = %(dataset_id)s
                            AND cc.dataset_id = %(dataset_id)s
                            AND cp.concept_id_1 = %(concept_id_1)s
                            {domain_filter})
                        UNION
                        (SELECT
                            cp.dataset_id,
                            cp.concept_id_2 AS concept_id_1,
                            cp.concept_id_1 AS concept_id_2,
                            cp.concept_count AS concept_pair_count,
                            cc.concept_count AS concept_2_count,
                            cp.concept_count / (cc.concept_count + 0E0) AS relative_frequency,
                            c.concept_name AS concept_2_name,
                            c.domain_id AS concept_2_domain
                        FROM cohd.concept_pair_counts cp
                        JOIN cohd.concept_counts cc ON cp.concept_id_1 = cc.concept_id
                        JOIN cohd.concept c ON cp.concept_id_1 = c.concept_id
                        WHERE cp.dataset_id = %(dataset_id)s
                            AND cc.dataset_id = %(dataset_id)s
                            AND cp.concept_id_2 = %(concept_id_1)s
                            {domain_filter})) x
                    ORDER BY relative_frequency DESC;'''
                params = {
                    'dataset_id': dataset_id,
                    'concept_id_1': concept_id_1,
                }

                if domain_id is not None and not domain_id == ['']:
                    # Restrict the associated concept by domain
                    domain_filter = 'AND c.domain_id = %(domain_id)s'
                    params['domain_id'] = domain_id
                else:
                    # Unrestricted domain
                    domain_filter = ''
                sql = sql.format(domain_filter=domain_filter)

            cur.execute(sql, params)
            json_return = cur.fetchall()

            # Add confidence interval to results
            confidence_level = args.get('confidence', DEFAULT_CONFIDENCE)
            try:
                confidence_level = float(confidence_level)
            except ValueError:
                return 'Confidence is not a number 0-1', 400
            if confidence_level < 0 or confidence_level >= 1:
                return 'Confidence should be a number between 0-1'
            for row in json_return:
                row['confidence_interval'] = rel_freq_ci(row['concept_pair_count'], row['concept_2_count'],
                                                          confidence_level)

    # print cur._executed
    # print(json_return)

    cur.close()
    conn.close()

    json_return = {"results": json_return}
    json_return = jsonify(json_return)

    return json_return


def query_count(concept_ids, dataset_id=None):
    """ Gets the single concept count of a list of concept_ids

    Parameters
    ----------
    concept_ids: list of concept IDs (string or int)
    dataset_id: (optional) String - COHD dataset ID

    Returns
    -------
    dict[concept_id (int)] -> singeConceptFreq result
    """
    # Make sure concept_ids is an iterable list of strings
    try:
        concept_ids = [str(x) for x in concept_ids]
    except TypeError:
        concept_ids = [str(concept_ids)]

    args = {'q': ','.join(concept_ids)}
    if dataset_id is not None and str(dataset_id):
        args['dataset_id'] = str(dataset_id)

    response = query_db(service='frequencies', method='singleConceptFreq', args=args)

    counts = dict()
    for result in response.get_json()['results']:
        counts[result['concept_id']] = result

    return counts


def query_concept_pair_count(concept_id_1, concept_id_2, dataset_id=None):
    """ Calls the desired association method and returns the results

    Parameters
    ----------
    concept_id_1: OMOP concept ID (String or int)
    concept_id_2: OMOP concept ID (String or int)
    dataset_id: COHD dataset ID (String or int)

    Returns
    -------
    result
    """
    assert concept_id_1 is not None and str(concept_id_1) and concept_id_2 is not None and str(concept_id_2), \
        'query_cohd_mysql.py::query_concept_pair_count()'

    args = {
        'q': str(concept_id_1) + ',' + str(concept_id_2)
    }
    if dataset_id is not None and str(dataset_id):
        args['dataset_id'] = str(dataset_id)

    response = query_db(service='frequencies', method='pairedConceptFreq', args=args)
    return response.get_json()


def query_association(method, concept_id_1, concept_id_2=None, dataset_id=None, domain_id=None, confidence=None):
    """ Calls the desired association method and returns the results

    Parameters
    ----------
    method: String - 'chiSquare', 'obsExpRatio', or 'relativeFrequency'
    concept_id_1: String - OMOP concept ID
    concept_id_2: (optional) String - OMOP concept ID
    dataset_id: (optional) String - COHD dataset ID
    domain_id: (optional) String - OMOP domain ID
    confidence: (optional) String - Confidence level

    Returns
    -------
    Dict results
    """
    assert method is not None and method and concept_id_1 is not None and str(concept_id_1), \
        'query_cohd_mysql.py::query_association() - Bad input. method={method}; concept_id_1={concept_id_1}'.format(
            method=method, concept_id_1=str(concept_id_1)
        )

    args = {
        'concept_id_1': str(concept_id_1)
    }
    if concept_id_2 is not None and str(concept_id_2):
        args['concept_id_2'] = str(concept_id_2)
    if dataset_id is not None and str(dataset_id):
        args['dataset_id'] = str(dataset_id)
    if domain_id is not None and domain_id:
        args['domain'] = domain_id
    if confidence is not None:
        args['confidence'] = str(confidence)

    response = query_db(service='association', method=method, args=args)
    return response.get_json()


def omop_concept_definition(concept_id):
    """ Get the OMOP concept definition

    Parameters
    ----------
    concept_id: OMOP concept ID (String or int)

    Returns
    -------
    Concept definition, or None
    """
    response = query_db(service='omop', method='concepts', args={'q': str(concept_id)})

    concept_result = response.get_json()
    if concept_result is None or 'results' not in concept_result or len(concept_result['results']) != 1:
        return None
    return concept_result['results'][0]


def omop_concept_definitions(concept_ids):
    """ Get the OMOP concept definition

    Parameters
    ----------
    concept_ids: iterable of OMOP concept IDs (String or int)

    Returns
    -------
    dict[concept_ids] = concept definition row
    """
    response = query_db(service='omop', method='concepts', args={'q': ','.join(str(c) for c in concept_ids)})

    concept_defs = dict()
    concept_results = response.get_json()
    if concept_results is None or 'results' not in concept_results:
        return concept_defs

    for r in concept_results['results']:
        concept_defs[r['concept_id']] = r

    return concept_defs