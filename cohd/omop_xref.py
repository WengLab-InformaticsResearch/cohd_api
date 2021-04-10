from difflib import SequenceMatcher
from typing import Optional, Union, Dict, List, Tuple, Iterable, Any

import requests
from numpy import argsort

from .cohd import cache
from .cohd_utilities import DomainClass


# OXO API configuration
_URL_OXO_SEARCH = 'https://www.ebi.ac.uk/spot/oxo/api/search'
_OXO_OMOP_MAPPING_TARGETS = ['ICD9CM', 'ICD10CM', 'SNOMEDCT', 'MeSH', 'UMLS']
_OXO_OMOP_VOCABULARIES = ['ICD9CM', 'ICD10CM', 'SNOMED', 'MeSH']
_OXO_PREFIX_TO_OMOP_VOCAB = {
    'ICD9CM': 'ICD9CM',
    'ICD10CM': 'ICD10CM',
    'SNOMEDCT': 'SNOMED',
    'MeSH': 'MeSH'
}
_OMOP_VOCAB_TO_OXO_PREFIX = {
    'ICD9CM': 'ICD9CM',
    'ICD10CM': 'ICD10CM',
    'SNOMED': 'SNOMEDCT',
    'MeSH': 'MeSH'
}
_OMOP_VOCABULARIES = ['ABMS', 'AMT', 'APC', 'ATC', 'BDPM', 'CDT', 'CIEL', 'Cohort', 'Concept Class', 'Condition Type',
                      'Cost Type', 'CPT4', 'Currency', 'CVX', 'Death Type', 'Device Type', 'dm+d', 'Domain', 'DPD',
                      'DRG', 'Drug Type', 'EphMRA ATC', 'Ethnicity', 'GCN_SEQNO', 'Gender', 'HCPCS', 'HES Specialty',
                      'ICD10', 'ICD10CM', 'ICD10PCS', 'ICD9CM', 'ICD9Proc', 'ICDO3', 'ISBT', 'ISBT Attribute', 'LOINC',
                      'MDC', 'Meas Type', 'MedDRA', 'MeSH', 'MMI', 'Multum', 'NDC', 'NDFRT', 'NFC', 'None', 'Note Type',
                      'NUCC', 'Obs Period Type', 'Observation Type', 'OPCS4', 'OXMIS', 'PCORNet', 'Place of Service',
                      'PPI', 'Procedure Type', 'Race', 'Read', 'Relationship', 'Revenue Code', 'RxNorm',
                      'RxNorm Extension', 'SMQ', 'SNOMED', 'Specialty', 'Specimen Type', 'SPL', 'UCUM', 'VA Class',
                      'VA Product', 'Visit', 'Visit Type', 'Vocabulary']


def omop_vocab_to_oxo_prefix(vocab):
    """ Attempt to lookup the corresponding OxO prefix from the OMOP vocabulary ID

    Uses the mapping defined in _OMOP_VOCAB_TO_OXO_PREFIX, but if no mapping is found, returns the vocabulary

    :param vocab: string - OMOP vocabulary_id
    :return: string - OxO prefix
    """
    prefix = vocab
    if vocab in _OMOP_VOCAB_TO_OXO_PREFIX:
        prefix = _OMOP_VOCAB_TO_OXO_PREFIX[vocab]
    return prefix


def omop_concept_lookup(cur, concept_id):
    """ Look up concept info

    :param cur: SQL cursor
    :param concept_id: int - concept_id
    :return: row from concept table
    """
    sql = '''SELECT *
        FROM cohd.concept
        WHERE concept_id = %(concept_id)s;'''
    params = {'concept_id': concept_id}

    cur.execute(sql, params)
    return cur.fetchall()


def omop_map_to_standard(cur, concept_code, vocabulary_id=None):
    """ OMOP map from concept code to standard concept_id

    :param cur: sql cursor
    :param concept_code: String - source concept code
    :param vocabulary_id: String - source vocabulary (optional)
    :return: List of mappings to standard concept_id
    """
    sql = '''SELECT
                c1.concept_id AS source_concept_id, 
                c1.concept_code AS source_concept_code,
                c1.concept_name AS source_concept_name, 
                c1.vocabulary_id AS source_vocabulary_id, 
                c2.concept_id AS standard_concept_id, 
                c2.concept_name AS standard_concept_name,
                c2.domain_id AS standard_domain_id,
                c2.concept_code AS standard_concept_code,
                c2.vocabulary_id AS standard_vocabulary_id
            FROM concept c1
            JOIN concept_relationship cr ON c1.concept_id = cr.concept_id_1
            JOIN concept c2 ON cr.concept_id_2 = c2.concept_id
            WHERE c1.concept_code = %(concept_code)s AND relationship_id = 'Maps to'
        '''
    params = {'concept_code': concept_code}

    # Restrict by vocabulary_id if specified
    if vocabulary_id is not None:
        sql += "    AND c1.vocabulary_id = %(vocabulary_id)s"
        params['vocabulary_id'] = vocabulary_id

    sql += ';'

    cur.execute(sql, params)
    return cur.fetchall()


def omop_map_from_standard(cur, concept_id, vocabularies=None):
    """ OMOP map from standard concept_id to concept codes

    :param cur: sql cursor
    :param concept_id: int
    :param vocabularies: List of strings - target vocabularies to map to
    :return: List of mappings
    """
    sql = '''SELECT 
            c.concept_id,
            c.concept_code, 
            c.concept_name, 
            c.domain_id, 
            c.vocabulary_id, 
            c.concept_class_id,
            c.standard_concept
        FROM concept_relationship cr
        JOIN concept c ON cr.concept_id_1 = c.concept_id
        WHERE cr.concept_id_2 = %s AND relationship_id = 'Maps to'
        '''
    params = [concept_id]

    # Restrict by vocabulary_id if specified
    if vocabularies is not None and len(vocabularies) > 0:
        sql += '''    AND c.vocabulary_id IN (%s)
            ''' % ','.join(['%s' for _ in vocabularies])
        params += vocabularies

    sql += 'ORDER BY c.vocabulary_id ASC, c.concept_code ASC;'

    cur.execute(sql, params)
    results = cur.fetchall()
    if results == ():
        # If no results, return an empty list
        results = []

    return results


def oxo_search(ids, input_source=None, mapping_targets=None, distance=2):
    """ Wrapper to the OxO search method.

    :param ids: List of strings - CURIEs to search for
    :param input_source: String
    :param mapping_targets: List of strings - Prefixes for target ontologies
    :param distance: Integer [1-3], default=2
    :return: JSON return from /oxo/api/search
    """
    if mapping_targets is None:
        mapping_targets = []

    # Call OXO search to map from the CURIE to vocabularies that OMOP knows
    data = {
        "ids": ids,
        "inputSource": input_source,
        "mappingTarget": mapping_targets,
        "distance": distance
    }

    r = requests.post(url=_URL_OXO_SEARCH, data=data)
    if r.status_code == 200:
        json_return = r.json()
        return json_return
    else:
        print('omop_xref.py::oxo_search - Non-200 response code from OxO:')
        print(r)

    return None


def xref_to_omop_standard_concept(cur, curie, distance=2, best=False):
    """ Map from external ontologies to OMOP

    Use OxO to map to OMOP vocabularies (ICD9, ICD10, SNOMEDCT, MeSH), then concept_relationship table to map to
    OMOP standard concept_id

    :param cur: SQL cursor
    :param curie: String - CURIE (e.g., 'DOID:8398')
    :param distance: Integer - OxO distance parameter [1-3], default=2
    :param best: True: return the best mapping; False: return all mappings
    :return: List of mappings
    """

    mappings = []
    total_distances = []

    # Call OxO to map to a vocabulary that OMOP knows
    j = oxo_search([curie], mapping_targets=_OXO_OMOP_MAPPING_TARGETS, distance=distance)
    if j is None:
        # Error from OxO, return the empty mappings list
        return mappings

    search_result = j['_embedded']['searchResults'][0]
    mrl = search_result['mappingResponseList']

    # Map each OxO mapping using OMOP concept_relationship 'Maps_to'
    for mr in mrl:
        prefix, concept_code = mr['curie'].split(':')
        oxo_distance = mr['distance']

        if prefix == 'UMLS':
            # Intermediate concept is UMLS, try using J. Banda's mappings
            umls_to_omop_mappings = banda_umls_to_omop(cur, concept_code)

            for u2o_mapping in umls_to_omop_mappings:
                omop_distance = 1
                total_distance = omop_distance + oxo_distance
                mapping = {
                    'source_oxo_id': search_result['queryId'],
                    'source_oxo_label': search_result['label'],
                    'intermediate_oxo_id': mr['curie'],
                    'intermediate_oxo_label': mr['label'],
                    'oxo_distance': oxo_distance,
                    'omop_standard_concept_id': u2o_mapping['concept_id'],
                    'omop_concept_name': u2o_mapping['concept_name'],
                    'omop_domain_id': u2o_mapping['domain_id'],
                    'omop_distance': omop_distance,
                    'total_distance': total_distance
                }
                mappings.append(mapping)
                total_distances.append(total_distance)

        else:
            # Use OMOP mappings to map from intermediate concept to OMOP standard concept
            # Determine the corresponding vocabulary_id
            vocabulary_id = _OXO_PREFIX_TO_OMOP_VOCAB.get(prefix)
            if prefix is None:
                # Conversion from OxO prefix to OMOP vocabulary_id is unknown
                continue

            # Map to the standard concept_id
            results = omop_map_to_standard(cur, concept_code, vocabulary_id)
            for result in results:
                omop_distance = int(result['source_concept_id'] != result['standard_concept_id'])
                total_distance = omop_distance + oxo_distance
                mapping = {
                    'source_oxo_id': search_result['queryId'],
                    'source_oxo_label': search_result['label'],
                    'intermediate_oxo_id': mr['curie'],
                    'intermediate_oxo_label': mr['label'],
                    'oxo_distance': oxo_distance,
                    'omop_standard_concept_id': result['standard_concept_id'],
                    'omop_concept_name': result['standard_concept_name'],
                    'omop_domain_id': result['standard_domain_id'],
                    'omop_distance': omop_distance,
                    'total_distance': total_distance
                }
                mappings.append(mapping)
                total_distances.append(total_distance)

    # Sort the list of mappings by total distance
    mappings_sorted = [mappings[i] for i in argsort(total_distances)]

    # Get the mapping to the best OMOP concept
    if best and mappings_sorted:
        mappings_sorted = _xref_best_to(mappings_sorted)

    return mappings_sorted


def xref_from_omop_standard_concept(cur, concept_id, mapping_targets=None, distance=2, best=False):
    """ Map from OMOP to external ontologies

    Use OMOP's concept_relationship table to map OMOP standard concept_ids to vocabularies supported in OxO
    (ICD9, ICD10, SNOMEDCT, MeSH) and UMLS using J. Banda's mappings, then use OxO to map to other ontologies

    :param cur: SQL cursor
    :param concept_id: int OMOP standard concept_id
    :param mapping_targets: List of string - target ontology prefixes
    :param distance: OxO distance
    :param best: True: return the best mappings; False: return all mappings
    :return: List of mappings
    """
    curies = []
    mappings = []
    search_results = []
    total_distances = []

    if mapping_targets is None:
        mapping_targets = []

    # Get concept ID info
    source_info = omop_concept_lookup(cur, concept_id)
    if len(source_info) == 0:
        # concept_id not found, return empty results
        return []
    source_info = source_info[0]

    # Map to compatible vocabularies (ICD9CM, ICD10CM, MeSH, and SNOMED)
    omop_mappings = omop_map_from_standard(cur, concept_id, _OXO_OMOP_VOCABULARIES)
    found_source = False
    for omop_mapping in omop_mappings:
        prefix = omop_vocab_to_oxo_prefix(omop_mapping['vocabulary_id'])
        curie = prefix + ':' + omop_mapping['concept_code']
        curies.append(curie)

        # Check if the source concept is included in the mappings
        found_source = found_source or (omop_mapping['concept_id'] == source_info['concept_id'])

        # If the intermediate vocab matches the target ontology, add it to the mappings
        if (mapping_targets is None) or (len(mapping_targets) == 0) or (prefix in mapping_targets):
            omop_distance = int(omop_mapping['concept_id'] != concept_id)
            mapping = {
                'source_omop_concept_id': concept_id,
                'source_omop_concept_name': source_info['concept_name'],
                'source_omop_vocabulary_id': source_info['vocabulary_id'],
                'source_omop_concept_code': source_info['concept_code'],
                'intermediate_omop_concept_id': omop_mapping['concept_id'],
                'intermediate_omop_vocabulary_id': omop_mapping['vocabulary_id'],
                'intermediate_omop_concept_code': omop_mapping['concept_code'],
                'intermediate_omop_concept_name': omop_mapping['concept_name'],
                'omop_distance': omop_distance,
                'intermediate_oxo_curie': 'N/A (OMOP mapping)',
                'intermediate_oxo_label': 'N/A (OMOP mapping)',
                'target_curie': '{prefix}:{code}'.format(prefix=prefix, code=omop_mapping['concept_code']),
                'target_label': omop_mapping['concept_name'],
                'oxo_distance': 0,
                'total_distance': omop_distance
            }
            mappings.append(mapping)
            total_distances.append(omop_distance)

    # Map to UMLS CUIs using J. Banda's mappings
    umls_mappings = banda_omop_to_umls(cur, concept_id)
    for umls_mapping in umls_mappings:
        cui = umls_mapping['cui']
        curie = 'UMLS:{cui}'.format(cui=cui)
        curies.append(curie)

        # If UMLS is in the target ontologies, add it to the mappings
        if (mapping_targets is None) or (len(mapping_targets) == 0) or ('UMLS' in mapping_targets):
            mapping = {
                'source_omop_concept_id': concept_id,
                'source_omop_concept_name': source_info['concept_name'],
                'source_omop_vocabulary_id': source_info['vocabulary_id'],
                'source_omop_concept_code': source_info['concept_code'],
                'intermediate_omop_concept_id': 'N/A (OMOP-UMLS mapping)',
                'intermediate_omop_vocabulary_id': 'N/A (OMOP-UMLS mapping)',
                'intermediate_omop_concept_code': 'N/A (OMOP-UMLS mapping)',
                'intermediate_omop_concept_name': 'N/A (OMOP-UMLS mapping)',
                'omop_distance': 1,
                'intermediate_oxo_curie': 'N/A (OMOP-UMLS mapping)',
                'intermediate_oxo_label': 'N/A (OMOP-UMLS mapping)',
                'target_curie': curie,
                'target_label': umls_mapping['label'],
                'oxo_distance': 0,
                'total_distance': 1
            }
            mappings.append(mapping)
            total_distances.append(1)

    # Add the source concept definition if not already in OMOP mappings (e.g., source concept is not a standard concept)
    if not found_source and source_info['vocabulary_id'] in _OMOP_VOCAB_TO_OXO_PREFIX:
        prefix = omop_vocab_to_oxo_prefix(source_info['vocabulary_id'])
        curie = prefix + ':' + source_info['concept_code']
        curies.append(curie)
        omop_mappings.append(source_info)

    # Call OxO to map from these intermediate CURIEs to the external ontologies
    if len(curies) > 0:
        j = oxo_search(curies, mapping_targets=mapping_targets, distance=distance)
        if j is not None:
            search_results = j['_embedded']['searchResults']
        else:
            # Error from OxO
            return []

    # Combine OxO mappings with OMOP mappings
    for i, search_result in enumerate(search_results):
        mrl = search_result['mappingResponseList']

        if len(mrl) == 0:
            continue

        if i < len(omop_mappings):
            # The first intermediate CURIEs came from OMOP mappings. Add info from OMOP mapping to the search result
            omop_mapping = omop_mappings[i]
            omop_distance = int(omop_mapping['concept_id'] != concept_id)
            intermediate_omop_concept_id = omop_mapping['concept_id']
            intermediate_omop_vocab_id = omop_mapping['vocabulary_id']
            intermediate_omop_concept_code = omop_mapping['concept_code']
            intermediate_omop_concept_name = omop_mapping['concept_name']
        else:
            # The following intermediate CURIEs came from UMLS mappings. Add info from the UMLS mappings to the results
            omop_distance = 1
            intermediate_omop_concept_id = 'N/A (OMOP-UMLS mapping)'
            intermediate_omop_vocab_id = 'N/A (OMOP-UMLS mapping)'
            intermediate_omop_concept_code = 'N/A (OMOP-UMLS mapping)'
            intermediate_omop_concept_name = 'N/A (OMOP-UMLS mapping)'

        for mr in mrl:
            oxo_distance = mr['distance']
            total_distance = omop_distance + oxo_distance
            mapping = {
                'source_omop_concept_id': concept_id,
                'source_omop_concept_name': source_info['concept_name'],
                'source_omop_vocabulary_id': source_info['vocabulary_id'],
                'source_omop_concept_code': source_info['concept_code'],
                'intermediate_omop_concept_id': intermediate_omop_concept_id,
                'intermediate_omop_vocabulary_id': intermediate_omop_vocab_id,
                'intermediate_omop_concept_code': intermediate_omop_concept_code,
                'intermediate_omop_concept_name': intermediate_omop_concept_name,
                'omop_distance': omop_distance,
                'intermediate_oxo_curie': search_result['curie'],
                'intermediate_oxo_label': search_result['label'],
                'target_curie': mr['curie'],
                'target_label': mr['label'],
                'oxo_distance': oxo_distance,
                'total_distance': total_distance
            }
            mappings.append(mapping)
            total_distances.append(total_distance)

    # sort the mappings by total distance
    mappings_sorted = [mappings[i] for i in argsort(total_distances)]

    # Get the best mappings
    if best:
        mappings_sorted = _xref_best_from(mappings_sorted)

    return mappings_sorted


def banda_umls_to_omop(cur, cui):
    sql = '''SELECT muo.cui, c.*
        FROM map_umls_omop muo
        JOIN concept c ON c.concept_id = muo.concept_id
        WHERE MATCH(cui) AGAINST(%(cui)s IN NATURAL LANGUAGE MODE) and c.standard_concept IS NOT NULL;
        '''
    params = {
        'cui': cui
    }

    nrows = cur.execute(sql, params)
    if nrows > 0:
        results = cur.fetchall()
    else:
        results = []
    return results


def banda_omop_to_umls(cur, concept_id):
    sql = '''SELECT muo.*, ot.uri, ot.label
        FROM map_umls_omop muo
        JOIN oxo_term ot ON ot.curie = CONCAT('UMLS:', muo.cui)
        WHERE concept_id = %(concept_id)s;
        '''
    params = {
        'concept_id': concept_id
    }

    nrows = cur.execute(sql, params)
    if nrows > 0:
        results = cur.fetchall()
    else:
        results = []
    return results


def oxo_term(cur, curie):
    sql = '''SELECT *
        FROM oxo_term
        WHERE curie = %(curie)s;'''
    params = {'curie': curie}
    cur.execute(sql, params)
    return cur.fetchall()


def oxo_local(cur, source_curie, distance=2, targets=None):
    sql = '''
        SELECT x.curie as target_curie, MIN(distance) AS distance, 
            IFNULL(ott.label, 'N/A (missing definition)') as target_label,  
            %(source_curie)s as source_curie, 
            IFNULL(ots.label, 'N/A (missing definition)') as source_label
        FROM
            {subquery_mapping}
        LEFT JOIN oxo_term ott ON ott.curie = x.curie
        LEFT JOIN oxo_term ots ON ots.curie = %(source_curie)s
        {filter_targets}
        GROUP BY ott.curie
        ORDER BY MIN(distance) ASC, x.curie ASC, ott.label;
        '''

    # Search distance
    distance = max(min(distance, 3), 1)
    subquery_mapping = '''
            ((SELECT mo1.curie_2 AS curie, 1 AS distance
            FROM map_oxo mo1
            WHERE mo1.curie_1 = %(source_curie)s)'''
    if distance >= 2:
        subquery_mapping += '''
            UNION
            (SELECT mo2.curie_2 AS curie, 2 AS distance
            FROM map_oxo mo1
            JOIN map_oxo mo2 ON mo1.curie_2 = mo2.curie_1 AND mo2.curie_2 != mo1.curie_1
            WHERE mo1.curie_1 = %(source_curie)s)'''
    if distance >= 3:
        subquery_mapping += '''
            UNION
            (SELECT mo3.curie_2 AS curie, 3 AS distance
            FROM map_oxo mo1
            JOIN map_oxo mo2 ON mo1.curie_2 = mo2.curie_1 AND mo2.curie_2 != mo1.curie_1
            JOIN map_oxo mo3 ON mo2.curie_2 = mo3.curie_1 AND mo3.curie_2 != mo2.curie_1 AND 
                mo3.curie_2 != %(source_curie)s
            WHERE mo1.curie_1 = %(source_curie)s)'''
    subquery_mapping += ''') x
            '''
    params = {'source_curie': source_curie}

    # Add target ontologies
    if targets is None or len(targets) == 0:
        # No target ontologies specified, include all ontologies
        filter_targets = '''WHERE ott.prefix IS NULL OR ott.prefix != %(source_prefix)s
            '''
        source_prefix = source_curie.split(':')[0]
        params['source_prefix'] = source_prefix
    else:
        # Only include certain target ontologies
        filter_targets = '''WHERE ott.prefix IN ({targets})'''
        # Create IDs for each target
        target_ids = []
        for i, target in enumerate(targets):
            target_id = 'target' + str(i)
            target_ids.append(target_id)
            params[target_id] = target
        filter_targets = filter_targets.format(targets=','.join(['%({x})s'.format(x=x) for x in target_ids]))

    sql = sql.format(subquery_mapping=subquery_mapping, filter_targets=filter_targets)
    cur.execute(sql, params)
    results = cur.fetchall()

    # print cur._last_executed

    return results


def xref_to_omop_local(cur, curie, distance=2, best=False):
    """ Map from external ontologies to OMOP using local implementation of OxO

    Use OxO to map to OMOP vocabularies (ICD9, ICD10, SNOMEDCT, MeSH) and UMLS. Use concept_relationship table to map
    OMOP vocaulbaries to OMOP standard concept_id, and J. Banda's mappings to map UMLS->OMOP

    :param cur: SQL cursor
    :param curie: String - CURIE (e.g., 'DOID:8398')
    :param distance: Integer - OxO distance parameter [1-3], default=2
    :param best: True: return the best mapping; False: return all mappings
    :return: List of mappings
    """

    mappings = []
    total_distances = []

    # Call local OxO to map to a vocabulary that OMOP knows
    oxo_mappings = oxo_local(cur, curie, distance=distance, targets=_OXO_OMOP_MAPPING_TARGETS)

    # Map each OxO mapping using OMOP concept_relationship 'Maps_to'
    for oxo_mapping in oxo_mappings:
        source_curie = oxo_mapping['source_curie']
        source_label = oxo_mapping['source_label']
        intermediate_curie = oxo_mapping['target_curie']
        intermediate_label = oxo_mapping['target_label']
        prefix, concept_code = intermediate_curie.split(':')
        oxo_distance = oxo_mapping['distance']

        if prefix == 'UMLS':
            # Intermediate concept is UMLS, try using J. Banda's mappings
            umls_to_omop_mappings = banda_umls_to_omop(cur, concept_code)

            for u2o_mapping in umls_to_omop_mappings:
                omop_distance = 1
                total_distance = omop_distance + oxo_distance
                mapping = {
                    'source_oxo_id': source_curie,
                    'source_oxo_label': source_label,
                    'intermediate_oxo_id': intermediate_curie,
                    'intermediate_oxo_label': intermediate_label,
                    'oxo_distance': oxo_distance,
                    'omop_standard_concept_id': u2o_mapping['concept_id'],
                    'omop_concept_name': u2o_mapping['concept_name'],
                    'omop_domain_id': u2o_mapping['domain_id'],
                    'omop_distance': omop_distance,
                    'total_distance': total_distance
                }
                mappings.append(mapping)
                total_distances.append(total_distance)

        else:
            # Use OMOP mappings to map from intermediate concept to OMOP standard concept
            # Determine the corresponding vocabulary_id
            vocabulary_id = _OXO_PREFIX_TO_OMOP_VOCAB.get(prefix)
            if prefix is None:
                # Conversion from OxO prefix to OMOP vocabulary_id is unknown
                continue

            # Map to the standard concept_id
            results = omop_map_to_standard(cur, concept_code, vocabulary_id)
            for result in results:
                omop_distance = int(result['source_concept_id'] != result['standard_concept_id'])
                total_distance = omop_distance + oxo_distance
                mapping = {
                    'source_oxo_id': source_curie,
                    'source_oxo_label': source_label,
                    'intermediate_oxo_id': intermediate_curie,
                    'intermediate_oxo_label': intermediate_label,
                    'oxo_distance': oxo_distance,
                    'omop_standard_concept_id': result['standard_concept_id'],
                    'omop_concept_name': result['standard_concept_name'],
                    'omop_domain_id': result['standard_domain_id'],
                    'omop_distance': omop_distance,
                    'total_distance': total_distance
                }
                mappings.append(mapping)
                total_distances.append(total_distance)

    # Sort the list of mappings by total distance
    mappings_sorted = [mappings[i] for i in argsort(total_distances)]

    # Get the best mapping
    if best and mappings_sorted:
        mappings_sorted = _xref_best_to(mappings_sorted)

    return mappings_sorted


def xref_from_omop_local(cur, concept_id, mapping_targets=None, distance=2, best=False):
    """ Map from OMOP to external ontologies using local implementation of OxO

    Use OMOP's concept_relationship table to map OMOP standard concept_ids to vocabularies supported in OxO
    (ICD9, ICD10, SNOMEDCT, MeSH) and UMLS using J. Banda's mappings, then use OxO to map to other ontologies

    :param cur: SQL cursor
    :param concept_id: int OMOP standard concept_id
    :param mapping_targets: List of string - target ontology prefixes
    :param distance: OxO distance
    :param best: True: return the best mappings; False: return all mappings
    :return: List of mappings
    """
    curies = []
    mappings = []
    total_distances = []
    if mapping_targets is None:
        mapping_targets = []

    # Get concept ID info
    source_info = omop_concept_lookup(cur, concept_id)
    if len(source_info) == 0:
        # concept_id not found, return empty results
        return []
    source_info = source_info[0]

    # Map to compatible vocabularies (ICD9CM, ICD10CM, MeSH, and SNOMED)
    omop_mappings = omop_map_from_standard(cur, concept_id, _OXO_OMOP_VOCABULARIES)
    found_source = False
    for omop_mapping in omop_mappings:
        prefix = omop_vocab_to_oxo_prefix(omop_mapping['vocabulary_id'])
        curie = prefix + ':' + omop_mapping['concept_code']
        curies.append(curie)

        # Check if the source concept is included in the mappings
        found_source = found_source or (omop_mapping['concept_id'] == source_info['concept_id'])

        # If the intermediate vocab matches the target ontology, add it to the mappings
        if (mapping_targets is None) or (len(mapping_targets) == 0) or (prefix in mapping_targets):
            omop_distance = int(omop_mapping['concept_id'] != concept_id)
            mapping = {
                'source_omop_concept_id': concept_id,
                'source_omop_concept_name': source_info['concept_name'],
                'source_omop_vocabulary_id': source_info['vocabulary_id'],
                'source_omop_concept_code': source_info['concept_code'],
                'intermediate_omop_concept_id': omop_mapping['concept_id'],
                'intermediate_omop_vocabulary_id': omop_mapping['vocabulary_id'],
                'intermediate_omop_concept_code': omop_mapping['concept_code'],
                'intermediate_omop_concept_name': omop_mapping['concept_name'],
                'omop_distance': omop_distance,
                'intermediate_oxo_curie': 'N/A (OMOP mapping)',
                'intermediate_oxo_label': 'N/A (OMOP mapping)',
                'target_curie': '{prefix}:{code}'.format(prefix=prefix, code=omop_mapping['concept_code']),
                'target_label': omop_mapping['concept_name'],
                'oxo_distance': 0,
                'total_distance': omop_distance
            }
            mappings.append(mapping)
            total_distances.append(omop_distance)

    # Map to UMLS CUIs using J. Banda's mappings
    umls_mappings = banda_omop_to_umls(cur, concept_id)
    for umls_mapping in umls_mappings:
        cui = umls_mapping['cui']
        curie = 'UMLS:{cui}'.format(cui=cui)
        curies.append(curie)

        # If UMLS is in the target ontologies, add it to the mappings
        if (mapping_targets is None) or (len(mapping_targets) == 0) or ('UMLS' in mapping_targets):
            mapping = {
                'source_omop_concept_id': concept_id,
                'source_omop_concept_name': source_info['concept_name'],
                'source_omop_vocabulary_id': source_info['vocabulary_id'],
                'source_omop_concept_code': source_info['concept_code'],
                'intermediate_omop_concept_id': 'N/A (OMOP-UMLS mapping)',
                'intermediate_omop_vocabulary_id': 'N/A (OMOP-UMLS mapping)',
                'intermediate_omop_concept_code': 'N/A (OMOP-UMLS mapping)',
                'intermediate_omop_concept_name': 'N/A (OMOP-UMLS mapping)',
                'omop_distance': 1,
                'intermediate_oxo_curie': 'N/A (OMOP-UMLS mapping)',
                'intermediate_oxo_label': 'N/A (OMOP-UMLS mapping)',
                'target_curie': curie,
                'target_label': umls_mapping['label'],
                'oxo_distance': 0,
                'total_distance': 1
            }
            mappings.append(mapping)
            total_distances.append(1)

    # Add the source concept definition if not already in OMOP mappings (e.g., source concept is not a standard concept)
    if not found_source and source_info['vocabulary_id'] in _OMOP_VOCAB_TO_OXO_PREFIX:
        prefix = omop_vocab_to_oxo_prefix(source_info['vocabulary_id'])
        curie = prefix + ':' + source_info['concept_code']
        curies.append(curie)
        omop_mappings.append(source_info)

    # Perform local OxO mappings
    for i, curie in enumerate(curies):
        oxo_mappings = oxo_local(cur, curie, distance=distance, targets=mapping_targets)

        if len(oxo_mappings) == 0:
            continue

        # Get information on the intermediate concept
        if i < len(omop_mappings):
            # The first intermediate CURIEs came from OMOP mappings. Add info from OMOP mapping to the search result
            omop_mapping = omop_mappings[i]
            omop_distance = int(omop_mapping['concept_id'] != concept_id)
            intermediate_omop_concept_id = omop_mapping['concept_id']
            intermediate_omop_vocab_id = omop_mapping['vocabulary_id']
            intermediate_omop_concept_code = omop_mapping['concept_code']
            intermediate_omop_concept_name = omop_mapping['concept_name']
        else:
            # The following intermeidate CURIEs came from UMLS mappings. Add info from the UMLS mappings to the results
            omop_distance = 1
            intermediate_omop_concept_id = 'N/A (OMOP-UMLS mapping)'
            intermediate_omop_vocab_id = 'N/A (OMOP-UMLS mapping)'
            intermediate_omop_concept_code = 'N/A (OMOP-UMLS mapping)'
            intermediate_omop_concept_name = 'N/A (OMOP-UMLS mapping)'

        # Combine information from OxO mapping and OMOP
        for oxo_mapping in oxo_mappings:
            oxo_distance = oxo_mapping['distance']
            total_distance = omop_distance + oxo_distance
            mapping = {
                'source_omop_concept_id': concept_id,
                'source_omop_concept_name': source_info['concept_name'],
                'source_omop_vocabulary_id': source_info['vocabulary_id'],
                'source_omop_concept_code': source_info['concept_code'],
                'intermediate_omop_concept_id': intermediate_omop_concept_id,
                'intermediate_omop_vocabulary_id': intermediate_omop_vocab_id,
                'intermediate_omop_concept_code': intermediate_omop_concept_code,
                'intermediate_omop_concept_name': intermediate_omop_concept_name,
                'omop_distance': omop_distance,
                'intermediate_oxo_curie': oxo_mapping['source_curie'],
                'intermediate_oxo_label': oxo_mapping['source_label'],
                'target_curie': oxo_mapping['target_curie'],
                'target_label': oxo_mapping['target_label'],
                'oxo_distance': oxo_distance,
                'total_distance': total_distance
            }
            mappings.append(mapping)
            total_distances.append(total_distance)

    # sort the mappings by total distance
    mappings_sorted = [mappings[i] for i in argsort(total_distances)]

    # Get the best mappings from each ontology
    if best and mappings_sorted:
        mappings_sorted = _xref_best_from(mappings_sorted)

    return mappings_sorted


def _xref_best_from(mappings):
    """ Get the best mapping for each target ontology

    Choose the best mapping by considering each mapping pathway to the any given target concept as independent evidence
    for mapping to that target. Weight each pathway according to the distance (shorter distance --> more weight).

    :param mappings: List of mappings
    :return: List of best mappings
    """
    # Check input
    if not mappings:
        return []

    # Keep track of all mappings per target ontology (prefix)
    mapping_scores = {}

    # Since each target_CURIE can have multiple pathways, keep track of a good representative pathway for each CURIE
    mapping_representatives = {}

    # Sum the scores for each target CURIE
    for mapping in mappings:
        target_curie = mapping['target_curie']
        prefix, code = target_curie.split(':')
        distance = int(mapping['total_distance'])

        # Get the mappings for the current prefix
        if prefix in mapping_scores:
            scores_in_prefix = mapping_scores[prefix]
        else:
            scores_in_prefix = {}
            mapping_scores[prefix] = scores_in_prefix

        # Keep track of scores per CURIE
        if target_curie in scores_in_prefix:
            # We've seen this CURIE before, add to the previous score sum
            scores_in_prefix[target_curie] += _mapping_score(distance)
        else:
            # This is the first path we've encountered for this CURIE
            scores_in_prefix[target_curie] = _mapping_score(distance)
            # Since mappings input were already sorted by distance, this pathway to the target CURIE should be shortest.
            # Use this pathway to represent the mapping to the target_curie
            mapping_representatives[target_curie] = mapping

    # Get the mappings with the best scores for each ontology
    best_mappings = []
    total_distances = []
    for prefix, scores_in_prefix in list(mapping_scores.items()):
        best_score = -1
        best_curies = None
        for curie, curie_score in list(scores_in_prefix.items()):
            if curie_score > best_score:
                best_score = curie_score
                best_curies = [curie]
            elif curie_score == best_score:
                best_curies.append(curie)

        # If there is more than one mapping with the same best score, use a basic string similarity
        # algorithm (Ratcliff-Obershelp similarity) to choose the best
        best_curie = best_curies[0]
        if len(best_curies) > 1:
            best_string_score = -1
            omop_label = mappings[0][u'source_omop_concept_name']
            for curie in best_curies:
                oxo_label = mapping_representatives[curie][u'target_label']
                string_score = SequenceMatcher(None, omop_label, oxo_label).ratio()
                if string_score > best_string_score:
                    best_string_score = string_score
                    best_curie = curie

        best_mapping = mapping_representatives[best_curie]
        best_mappings.append(best_mapping)
        total_distances.append(best_mapping['total_distance'])

    # Sort the total_distance to be consistent with previous behavior
    best_mappings_sorted = [best_mappings[i] for i in argsort(total_distances)]

    return best_mappings_sorted


def _xref_best_to(mappings):
    """ Get the best mapping from each target ontology

    Choose the best mapping by considering each mapping pathway to the any given OMOP concept as independent evidence
    for mapping to that concept. Weight each pathway according to the distance (shorter distance --> more weight).

    :param mappings: List of mappings
    :return: List of best mappings
    """
    # Check input
    if not mappings:
        return []

    # Keep track of all mappings per target ontology (prefix)
    mapping_scores = {}

    # Since each target_CURIE can have multiple pathways, keep track of a good representative pathway for each CURIE
    mapping_representatives = {}

    # Sum the scores for each target OMOP standard concept
    for mapping in mappings:
        omop_id = mapping['omop_standard_concept_id']
        distance = int(mapping['total_distance'])

        # Get the mappings for the current prefix
        if omop_id in mapping_scores:
            # We've seen this OMOP concept before, add to the previous score sum
            mapping_scores[omop_id] += _mapping_score(distance)
        else:
            # This is the first path we've encountered for this CURIE
            mapping_scores[omop_id] = _mapping_score(distance)
            # Since mappings input were already sorted by distance, this pathway to the target CURIE should be shortest.
            # Use this pathway to represent the mapping to the target_curie
            mapping_representatives[omop_id] = mapping

    # Get the mappings with the best scores
    best_score = -1
    best_omop_ids = None
    for omop_id, score in list(mapping_scores.items()):
        if score > best_score:
            best_score = score
            best_omop_ids = [omop_id]
        elif score == best_score:
            best_omop_ids.append(omop_id)

    # If there is more than one mapping with the same best score, use a basic string similarity
    # algorithm (Ratcliff-Obershelp similarity) to choose the best
    best_omop_id = best_omop_ids[0]
    if len(best_omop_ids) > 1:
        best_string_score = -1
        oxo_label = mappings[0][u'source_oxo_label']
        for omop_id in best_omop_ids:
            concept_name = mapping_representatives[omop_id][u'omop_concept_name']
            string_score = SequenceMatcher(None, oxo_label, concept_name).ratio()
            if string_score > best_string_score:
                best_string_score = string_score
                best_omop_id = omop_id

    best_mapping = mapping_representatives[best_omop_id]
    return [best_mapping]


def _mapping_score(distance, max_distance=3):
    return (max_distance + 1 - distance) ** 2


class ConceptMapper:
    """
    Maps between OMOP concepts and other vocabularies or ontologies using both OMOP mappings and OxO
    """

    def __init__(self,
                 mappings: Optional[Dict[DomainClass, str]] = None,
                 distance: int = 2,
                 local_oxo: bool = True):
        """ Constructor

        Parameters
        ----------
        mappings: mappings between domain and ontology. Should have key '_other' to specify desired ontology when a
        mapping is not specified for the domain.
        distance: maximum allowed total distance (as opposed to OxO distance)
        """
        # Maximum distance allowed for mappings (min requirement is 1)
        self.distance = max(distance, 1)

        # Whether to use the local or real implementation of OxO
        self.local_oxo = local_oxo

        # Mappings from OMOP domain to desired ontology (CURIE prefix)
        self.domain_targets = mappings
        if mappings is None:
            self.domain_targets_omop = None
            self.domain_targets_oxo = None
        else:
            # Split the list of targets between those that can be handled with OMOP mappings and those that can't
            self.domain_targets_omop = {}
            self.domain_targets_oxo = {}
            for domain, targets in list(self.domain_targets.items()):
                targets_omop, targets_oxo = ConceptMapper._split_omop_oxo_targets(targets)
                self.domain_targets_omop[domain] = targets_omop
                self.domain_targets_oxo[domain] = targets_oxo

    def __repr__(self):
        """ Used in flask cache

        Returns
        -------
        String repr
        """
        d = {
            'domain_targets': self.domain_targets,
            'distance': self.distance,
            'local_oxo': self.local_oxo
        }
        return str(d)

    @staticmethod
    def _split_omop_oxo_targets(targets: Iterable[str]) -> Tuple[List, List]:
        """ Given a list of target ontologies, figure out which ones can be retrieved by OMOP mappings. All other target
        ontologies will be separated into the list of target ontologies that should go to OxO.

        Parameters
        ----------
        targets: List of target ontologies

        Returns
        -------
        (targets_omop, targets_oxo)
        """
        omop_vocabulary_set = set(_OMOP_VOCABULARIES)

        # Get any targets that can be supported by just OMOP mapping
        targets_set = set(targets)
        targets_omop = list(omop_vocabulary_set & targets_set)
        targets_set.difference_update(omop_vocabulary_set)

        # SNOMED-CT is SNOMED-CT in OxO but SNOMED in OMOP
        if 'SNOMED-CT' in targets_set:
            targets_omop.append('SNOMED')
            targets_set.remove('SNOMED-CT')

        # OxO will be used for the remaining targets
        targets_oxo = list(targets_set)

        return targets_omop, targets_oxo

    @cache.memoize(timeout=2419200, cache_none=True)
    def map_to_omop(self, curie):
        """ Map to OMOP concept from ontology

        Parameters
        ----------
        curie: CURIE

        Returns
        -------
        Dict like:
        {
            "omop_concept_name": "Osteoarthritis",
            "omop_concept_id": 80180,
            "distance": 2
        }
        or None
        """
        from .query_cohd_mysql import sql_connection, omop_concept_definition

        mapping = None

        # Connection to MySQL database
        conn = sql_connection()
        cur = conn.cursor()

        prefix, concept_code = curie.split(':')
        omop_vocab = _OXO_PREFIX_TO_OMOP_VOCAB.get(prefix, prefix)
        if prefix == 'OMOP':
            # Already an OMOP concept
            concept_def = omop_concept_definition(concept_code)
            if concept_def:
                mapping = {
                    'omop_concept_id': concept_code,
                    'omop_concept_name': concept_def['concept_name'],
                    'distance': 0
                }
        elif omop_vocab in _OMOP_VOCABULARIES:
            # Source vocabulary is in OMOP vocab
            omop_mapping = omop_map_to_standard(cur, concept_code, omop_vocab)
            if omop_mapping:
                mapping = {
                    'omop_concept_id': omop_mapping[0]['standard_concept_id'],
                    'omop_concept_name': omop_mapping[0]['standard_concept_name'],
                    'distance': int(concept_code != omop_mapping[0]['standard_concept_code'] or
                                    prefix != omop_mapping[0]['standard_vocabulary_id'])
                }
        else:
            # Use OxO to map from external ontology
            if self.local_oxo:
                best_mapping = xref_to_omop_local(cur, curie, distance=self.distance, best=True)
            else:
                best_mapping = xref_to_omop_standard_concept(cur, curie, distance=self.distance, best=True)
            if best_mapping and best_mapping[0]['total_distance'] <= self.distance:
                # Simplify the returned information
                mapping = {
                    'omop_concept_id': best_mapping[0]['omop_standard_concept_id'],
                    'omop_concept_name': best_mapping[0]['omop_concept_name'],
                    'distance': best_mapping[0]['total_distance']
                }

        # Close MySQL connection
        cur.close()
        conn.close()

        return mapping

    def map_from_omop(self, concept_id, domain_id=None, concept_class_id=None):
        """ Map from OMOP concept to appropriate domain-specific ontology.

        Parameters
        ----------
        concept_id: OMOP concept ID
        domain_id: OMOP concept's domain. Will look it up if not specified
        concept_class_id: OMOP concept_class_id. Will look it up if not specified

        Returns
        -------
        Array of dict like:
        [{
            "target_curie": "UMLS:C0154091",
            "target_label": "Carcinoma in situ of bladder",
            "distance": 1
        }]
        or None
        """
        from .query_cohd_mysql import sql_connection, omop_concept_definition

        mappings = []

        if self.domain_targets is None:
            # No target mappings, don't perform any mapping
            return mappings

        # If the domain wasn't provided, look it up
        if domain_id is None or not domain_id or concept_class_id is None or not concept_class_id:
            concept_def = omop_concept_definition(concept_id)
            if not concept_def:
                return None
            domain_id = concept_def['domain_id']
            concept_class_id = concept_def['concept_class_id']

        conn = sql_connection()
        cur = conn.cursor()

        # Get OMOP mapping targets
        dc_with_class = DomainClass(domain_id, concept_class_id)
        dc_without_class = DomainClass(domain_id, None)
        omop_targets = self.domain_targets_omop.get(dc_with_class, self.domain_targets_omop.get(dc_without_class, None))
        if omop_targets is None and '_DEFAULT' in self.domain_targets_omop:
            # No mapping for this domain, but default specified. Use the default mapping
            omop_targets = self.domain_targets_omop['_DEFAULT']

        # Get OMOP mappings
        if omop_targets:
            omop_mappings = omop_map_from_standard(cur, concept_id, omop_targets)
            for omop_mapping in omop_mappings:
                # Make the CURIE
                prefix = omop_vocab_to_oxo_prefix(omop_mapping['vocabulary_id'])
                curie = '{prefix}:{id}'.format(prefix=prefix, id=omop_mapping['concept_code'])
                mappings.append({
                    'target_curie': curie,
                    'target_label': omop_mapping['concept_name'],
                    'distance': int(str(omop_mapping['concept_id']) != str(concept_id))
                })

        # Determine the target ontology
        oxo_targets = self.domain_targets_oxo.get(dc_with_class, self.domain_targets_oxo.get(dc_without_class, None))
        if oxo_targets is None and DomainClass(None, None) in self.domain_targets:
            # No mapping for this domain, but default specified. Use the default mapping
            oxo_targets = self.domain_targets_oxo[DomainClass(None, None)]

        # Get the OxO mappings
        if oxo_targets:
            if self.local_oxo:
                oxo_mappings = xref_from_omop_local(cur, concept_id, oxo_targets, distance=self.distance, best=True)
            else:
                print("USING REMOTE OXO")
                oxo_mappings = xref_from_omop_standard_concept(cur, concept_id, oxo_targets, distance=self.distance,
                                                               best=True)
            for mapping in oxo_mappings:
                if mapping['total_distance'] <= self.distance:
                    mappings.append({
                        'target_curie': mapping['target_curie'],
                        'target_label': mapping['target_label'],
                        'distance': mapping['total_distance']
                    })

        cur.close()
        conn.close()

        return mappings

    @cache.memoize(timeout=2419200, cache_none=True)
    def map_from_omop_to_target(self,
                                concept_id: Union[str, int],
                                target_ontologies: Iterable[str]) -> List[Dict[str, Any]]:
        """ Map from OMOP concept to one of the target ontologies.

        Parameters
        ----------
        concept_id: OMOP concept ID
        target_ontologies: OxO prefix of target ontology

        Returns
        -------
        Array of dict like:
        [{
            "target_curie": "UMLS:C0154091",
            "target_label": "Carcinoma in situ of bladder",
            "distance": 1
        }]
        or None
        """
        from .query_cohd_mysql import sql_connection

        mappings = []

        omop_targets, oxo_targets = ConceptMapper._split_omop_oxo_targets(target_ontologies)

        conn = sql_connection()
        cur = conn.cursor()

        # Get OMOP mappings
        if omop_targets:
            omop_mappings = omop_map_from_standard(cur, concept_id, omop_targets)
            for omop_mapping in omop_mappings:
                # Make the CURIE
                prefix = omop_vocab_to_oxo_prefix(omop_mapping['vocabulary_id'])
                curie = '{prefix}:{id}'.format(prefix=prefix, id=omop_mapping['concept_code'])
                mappings.append({
                    'target_curie': curie,
                    'target_label': omop_mapping['concept_name'],
                    'distance': int(str(omop_mapping['concept_id']) != str(concept_id))
                })

        # Get the OxO mappings
        if oxo_targets:
            if self.local_oxo:
                oxo_mappings = xref_from_omop_local(cur, concept_id, oxo_targets, distance=self.distance, best=True)
            else:
                print("USING REMOTE OXO")
                oxo_mappings = xref_from_omop_standard_concept(cur, concept_id, oxo_targets, distance=self.distance,
                                                               best=True)
            for mapping in oxo_mappings:
                if mapping['total_distance'] <= self.distance:
                    mappings.append({
                        'target_curie': mapping['target_curie'],
                        'target_label': mapping['target_label'],
                        'distance': mapping['total_distance']
                    })

        cur.close()
        conn.close()

        return mappings
