# This module defines helper functions for calling the COHD REST API

import requests
import pandas as pd
import numpy as np
from .cohd_temporal_analysis import AgeCounts, DeltaCounts


# COHD API server
server = 'http://tr-kp-clinical.ncats.io/api'
# server = 'http://cohd.io/api'

# ######################################################################
# Utility functions
# ######################################################################

def _process_response(response):
    # Check the response status code
    if response.status_code == requests.status_codes.codes.OK:
        # Convert COHD's JSON response to Pandas dataframe
        return response.json()
    else:
        # Raise an error if the status code indicates an issue (e.g., not 200)
        response.raise_for_status()


def _json_to_df(json):
    # Convert the results list to a DataFrame
    return pd.DataFrame(json['results'])


# ######################################################################
# COHD OMOP functions
# ######################################################################

# Find concepts by name
def find_concept(concept_name, dataset_id=None, domain=None, min_count=1):
    url = f'{server}/omop/findConceptIDs'
    
    # Params
    params = {
        'q': concept_name,
        'min_count': min_count
    }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
    if domain is not None:
        params['domain'] = domain
        
    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 7:
        # re-order the columns so that it displays in a more logical order
        df = df[['concept_id', 'concept_name', 'domain_id', 'concept_class_id', 
                 'vocabulary_id', 'concept_code', 'concept_count']]
    return json, df

# Get concept definitions from concept ID
def concept(concept_ids):   
    url = f'{server}/omop/concepts'
    
    # Convert list of concept IDs to a comma-delimited string
    if isinstance(concept_ids, list):
        concept_ids_string = ','.join([str(x) for x in concept_ids])    
    else:
        concept_ids_string = [str(concept_ids)]
    
    params = {'q': concept_ids_string}
    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 6:
        # re-order the columns so that it displays in a more logical order
        df = df[['concept_id', 'concept_name', 'domain_id', 'concept_class_id', 'vocabulary_id', 'concept_code']]
    return json, df

# Get ancestors of a concept
def concept_ancestors(concept_id, dataset_id=None, vocabulary_id=None, concept_class_id=None):
    url = f'{server}/omop/conceptAncestors'
    
    # Params
    params = {
        'concept_id': concept_id,
    }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
    if vocabulary_id is not None:
        params['vocabulary_id'] = vocabulary_id
    if concept_class_id is not None:
        params['concept_class_id'] = concept_class_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 10:
        # re-order the columns so that it displays in a more logical order
        df = df[['ancestor_concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id', 
                 'concept_code', 'standard_concept', 'concept_count', 'max_levels_of_separation', 
                 'min_levels_of_separation']]
    return json, df

# Get descendants of a concept
def concept_descendants(concept_id, dataset_id=None, vocabulary_id=None, concept_class_id=None):
    url = f'{server}/omop/conceptDescendants'
    
    # Params
    params = {
        'concept_id': concept_id,
    }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
    if vocabulary_id is not None:
        params['vocabulary_id'] = vocabulary_id
    if concept_class_id is not None:
        params['concept_class_id'] = concept_class_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 10:
        # re-order the columns so that it displays in a more logical order
        df = df[['descendant_concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id', 
                 'concept_code', 'standard_concept', 'concept_count', 'max_levels_of_separation', 
                 'min_levels_of_separation']]
    return json, df

# Get a list of OMOP vocabularies
def vocabularies():
    url = f'{server}/omop/vocabularies'
    response = requests.get(url)
    json = _process_response(response)
    df = _json_to_df(json)
    return json, df

# Map from non-standard OMOP concepts to standard OMOP concepts
def map_to_standard_concept_id(concept_code, vocabulary_id=None):
    url = f'{server}/omop/mapToStandardConceptID'
    
    # Params
    params = {'concept_code': concept_code}
    if vocabulary_id is not None:
        params['vocabulary_id'] = vocabulary_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 7:
        # re-order the columns so that it displays in a more logical order
        df = df[['source_concept_id', 'source_vocabulary_id', 'source_concept_code', 'source_concept_name', 
                 'standard_concept_id', 'standard_concept_name', 'standard_domain_id']]
    return json, df

# Reverse-map from standard OMOP concepts to non-standard OMOP concepts
def map_from_standard_concept_id(concept_id, vocabulary_id=None):
    url = f'{server}/omop/mapFromStandardConceptID'
    
    # Params
    params = {'concept_id': concept_id}
    if vocabulary_id is not None:
        params['vocabulary_id'] = vocabulary_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 7:
        # re-order the columns so that it displays in a more logical order
        df = df[['concept_id', 'concept_name', 'domain_id', 'concept_class_id', 
                 'vocabulary_id', 'concept_code', 'standard_concept']]
    return json, df

# Cross-reference a concept (CURIE) from an external ontology to OMOP (concept ID)
def xref_to_omop(curie, distance=None, local=False, recommend=False):
    url = f'{server}/omop/xrefToOMOP'
    
    # Params
    params = {
      'curie': curie,
      'local': local,
      'recommend': recommend
    }
    if distance is not None:
        params['distance'] = distance

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 10:
        # re-order the columns so that it displays in a more logical order
        df = df[['source_oxo_id', 'source_oxo_label', 'intermediate_oxo_id', 'intermediate_oxo_label', 
                'omop_standard_concept_id', 'omop_concept_name', 'omop_domain_id', 'omop_distance', 'total_distance']]
    return json, df

# Cross-reference a concept from OMOP (concept ID) to an external ontology (CURIE)
def xref_from_omop(concept_id, mapping_targets=None, distance=None, local=False, recommend=False):
    url = f'{server}/omop/xrefFromOMOP'
    
    # Params
    params = {
      'concept_id': concept_id,
      'local': local,
      'recommend': recommend
    }
    if mapping_targets is not None:
        params['mapping_targets'] = mapping_targets
    if distance is not None:
        params['distance'] = distance

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 15:
        # re-order the columns so that it displays in a more logical order
        df = df[['source_omop_concept_id', 'source_omop_concept_name', 'source_omop_vocabulary_id', 
                 'source_omop_concept_code', 'intermediate_omop_concept_id', 'intermediate_omop_concept_name',
                 'intermediate_omop_vocabulary_id', 'intermediate_omop_concept_code', 'intermediate_oxo_curie',
                 'intermediate_oxo_label', 'target_curie', 'target_label', 'omop_distance',
                 'oxo_distance', 'total_distance']]
    return json, df

# ######################################################################
# COHD metadata functions
# ######################################################################

# Get descriptions of the available data sets
def datasets():
    url = f'{server}/metadata/datasets'
    response = requests.get(url)
    json = _process_response(response)
    df = _json_to_df(json)
    
    if len(df.columns) == 3:
        # re-order the columns so that it displays in a more logical order
        df = df[['dataset_id', 'dataset_name', 'dataset_description']]
    return json, df

# Get the number of concepts in each domain
def domain_counts(dataset_id=None):
    url = f'{server}/metadata/domainCounts'
    
    # Optional params
    params = {}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 3:
        # re-order the columns so that it displays in a more logical order
        df = df[['dataset_id', 'domain_id', 'count']]
    return json, df

# Get the number of concept-pairs in each domain-paired
def domain_pair_counts(dataset_id=None):
    url = f'{server}/metadata/domainPairCounts'
    
    # Optional params
    params = {}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 4:
        # re-order the columns so that it displays in a more logical order
        df = df[['dataset_id', 'domain_id_1', 'domain_id_2', 'count']]
    return json, df

# Get the number of patients in the data set
def patient_count(dataset_id=None):
    url = f'{server}/metadata/patientCount'
    
    # Optional params
    params = {}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 2:
        # re-order the columns so that it displays in a more logical order
        df = df[['dataset_id', 'count']]
    return json, df

# ######################################################################    
# COHD Clinical Frequency functions
# ######################################################################

# Get the single-concept frequency for a concept or list of single concepts
def concept_frequency(concept_ids, dataset_id=None):
    url = f'{server}/frequencies/singleConceptFreq'
    
    # Convert list of concept IDs to a comma-delimited string
    concept_ids_string = ','.join([str(x) for x in concept_ids])
    
    # Params
    params = {'q': concept_ids_string}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 4:
        # re-order the columns so that it displays in a more logical order
        df = df[['dataset_id', 'concept_id', 'concept_count', 'concept_frequency']]
    return json, df

# Get the most frequent concepts (optionally: in a given domain)
def most_frequent_concepts(limit, dataset_id=None, domain_id=None):
    url = f'{server}/frequencies/mostFrequentConcepts'
    
    # Params
    params = {'q': limit}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
    if domain_id is not None:
        params['domain'] = domain_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 6:
        df = df[['dataset_id', 'concept_id', 'concept_name', 'domain_id', 'concept_count', 'concept_frequency']]
    return json, df

# Get the co-occurrence frequency of the pair of concepts
def paired_concepts_frequency(concept_id_1, concept_id_2, dataset_id=None):
    url = f'{server}/frequencies/pairedConceptFreq'
    
    # Params
    params = {'q': f'{concept_id_1!s},{concept_id_2!s}'}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 5:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'concept_count', 'concept_frequency']]
    return json, df

# Get the co-occurrence frequency between the given concept and all other concepts
def associated_concepts_freq(concept_id, dataset_id=None):
    url = f'{server}/frequencies/associatedConceptFreq'
    
    # Params
    params = {'q': concept_id}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 7:
        df = df[['dataset_id', 'concept_id', 'associated_concept_id', 'associated_concept_name',
                'associated_domain_id', 'concept_count', 'concept_frequency']]
    return json, df

# Get the co-occurrence frequency between the given concept and all other concepts   in the given domain  
def associated_concept_domain_freq(concept_id, domain_id, dataset_id=None):
    url = f'{server}/frequencies/associatedConceptDomainFreq'
    
    # Params
    params = {
        'concept_id': concept_id, 
        'domain': domain_id
    }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 7:
        df = df[['dataset_id', 'concept_id', 'associated_concept_id', 'associated_concept_name',
                'associated_domain_id', 'concept_count', 'concept_frequency']]
    return json, df

# ######################################################################
# COHD Concept Associations 
# ######################################################################

# Get the chi-square association analysis between:
# 1) a concept and all other concepts (specify concept_id_1 only)
# 2) a concept and all other concepts in a given domain (specify concept_id_1 and domain_id)
# 3) a pair of concepts (concept_id_1 and concept_id_2)
def chi_square(concept_id_1, concept_id_2=None, domain_id=None, dataset_id=None):
    url = f'{server}/association/chiSquare'
    
    # Params
    params = {'concept_id_1': concept_id_1}
    if concept_id_2 is not None:
        params['concept_id_2'] = concept_id_2
    if domain_id is not None:
        params['domain'] = domain_id
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 5:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'chi_square', 'p-value']]
    elif len(df.columns) == 7:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'concept_2_name', 
                 'concept_2_domain', 'chi_square', 'p-value']]
    return json, df

# Get the observed-to-expected frequency association analysis between:
# 1) a concept and all other concepts (specify concept_id_1 only)
# 2) a concept and all other concepts in a given domain (specify concept_id_1 and domain_id)
# 3) a pair of concepts (concept_id_1 and concept_id_2)
def obs_exp_ratio(concept_id_1, concept_id_2=None, domain_id=None, dataset_id=None):
    url = f'{server}/association/obsExpRatio'
    
    # Params
    params = {'concept_id_1': concept_id_1}
    if concept_id_2 is not None:
        params['concept_id_2'] = concept_id_2
    if domain_id is not None:
        params['domain'] = domain_id
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 6:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'observed_count', 'expected_count', 'ln_ratio']]
    elif len(df.columns) == 8:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'concept_2_name', 
                 'concept_2_domain', 'observed_count', 'expected_count', 'ln_ratio']]
    return json, df

# Get the relative frequency association analysis between:
# 1) a concept and all other concepts (specify concept_id_1 only)
# 2) a concept and all other concepts in a given domain (specify concept_id_1 and domain_id)
# 3) a pair of concepts (concept_id_1 and concept_id_2)
def relative_frequency(concept_id_1, concept_id_2=None, domain_id=None, dataset_id=None):
    url = f'{server}/association/relativeFrequency'
    
    # Params
    params = {'concept_id_1': concept_id_1}
    if concept_id_2 is not None:
        params['concept_id_2'] = concept_id_2
    if domain_id is not None:
        params['domain'] = domain_id
    if dataset_id is not None:
        params['dataset_id'] = dataset_id

    response = requests.get(url, params)
    json = _process_response(response)
    df = _json_to_df(json)
    if len(df.columns) == 6:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'concept_pair_count', 
                 'concept_2_count', 'relative_frequency']]
    elif len(df.columns) == 8:
        df = df[['dataset_id', 'concept_id_1', 'concept_id_2', 'concept_2_name', 'concept_2_domain', 
                 'concept_pair_count', 'concept_2_count', 'relative_frequency']]
    return json, df


# ######################################################################
# Temporal Clinical Data
# ######################################################################

def temporal_concept_age_counts(concept_id, dataset_id=None):
    """ Get concept-age distribution for concept_id 
    
    Params
    ------
    concept_id (int) - OMOP concept ID
    dataset_id (int) - COHD dataset ID
    
    Returns 
    -------
    List of AgeCounts objects
    """
    url = f'{server}/temporal/conceptAgeCounts'
    params = {'concept_id': concept_id}
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
        
    response = requests.get(url, params)
    json = _process_response(response)
    cads = list()
    if 'results' in json:
        for r in json[u'results']:
            cad = AgeCounts(dataset_id = r['dataset_id'], concept_id = r['concept_id'], concept_name = r['concept_name'], 
                            concept_count=r['concept_count'],counts = r['counts'], confidence = np.array(r['confidence_interval']).T, 
                            bin_width = r['bin_width'])
            cads.append(cad)
    return cads


def temporal_pair_delta_counts(source_concept_id, target_concept_id, dataset_id=None):
    """ Get concept-age distribution for concept_id 
    
    Params
    ------
    source_concept_id (int) - OMOP concept ID of the source concept
    target_concept_id (int) - OMOP concept ID of the target concept
    dataset_id (int) - COHD dataset ID
    
    Returns 
    -------
    List of DeltaCounts objects
    """
    url = f'{server}/temporal/conceptPairDeltaCounts'
    params = {
        'source_concept_id': source_concept_id,
        'target_concept_id': target_concept_id
        }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
        
    response = requests.get(url, params)
    json = _process_response(response)
    deltas = list()
    if 'results' in json:
        for r in json[u'results']:
            dc = DeltaCounts(dataset_id = r['dataset_id'], source_concept_id = r['source_concept_id'], 
                             target_concept_id = r['target_concept_id'], source_concept_name = r['source_concept_name'],
                             target_concept_name = r['target_concept_name'], source_concept_count = r['source_concept_count'],
                             target_concept_count = r['target_concept_count'], concept_pair_count = r['concept_pair_count'], 
                             counts = r['counts'], confidence = np.array(r['confidence_interval']).T, 
                             bin_width = r['bin_width'], n = r['n'])            
            deltas.append(dc)                        
            
    return deltas


def temporal_find_similar_age_distributions(concept_id, dataset_id=None, exclude_related=None, restrict_type=None,
                                   threshold=None, limit=None):
    url = f'{server}/temporal/findSimilarAgeDistributions'
    params = {
        'concept_id': concept_id
    }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
    if exclude_related is not None:
        params['exclude_related'] = exclude_related
    if restrict_type is not None:
        params['restrict_type'] = restrict_type
    if threshold is not None:
        params['threshold'] = threshold
    if limit is not None:
        params['limit'] = limit
    response = requests.get(url, params)
    json = _process_response(response)
    
    if 'results' not in json:
        return None
    
    similarities_binned = dict()
    cads_binned = dict()
    for result_set in json['results']:
        bin_width = result_set['bin_width']
        cads = list()
        sims = list()
        for r in result_set['concept_age_counts']:
            cad = AgeCounts(dataset_id = r['dataset_id'], concept_id = r['concept_id'], 
                            concept_name = r['concept_name'], concept_count=r['concept_count'],
                            counts = r['counts'], confidence = np.array(r['confidence_interval']).T, 
                            bin_width = r['bin_width'])
            cads.append(cad)
            sims.append(r['similarity'])
            
        cads_binned[bin_width] = cads
        similarities_binned[bin_width] = sims
     
    return cads_binned, similarities_binned


def temporal_source_to_target(source_concept_id, target_concept_id, dataset_id=None):
    """ Get concept-age distribution for concept_id 
    
    Params
    ------
    source_concept_id (int) - OMOP concept ID of the source concept
    target_concept_id (int) - OMOP concept ID of the target concept
    dataset_id (int) - COHD dataset ID
    
    Returns 
    -------
    Results from comparing the queried concept pair against similar source and target concepts
    """
    url = f'{server}/temporal/sourceToTarget'
    params = {
        'source_concept_id': source_concept_id,
        'target_concept_id': target_concept_id
        }
    if dataset_id is not None:
        params['dataset_id'] = dataset_id
        
    response = requests.get(url, params)
    json = _process_response(response)
    if u'results' not in json or len(json[u'results']) != 1:
        return None
    
    result = json[u'results'][0]
    
    queried_pair = dict()
    for x in result[u'queried_pair']:
        d = x[u'delta']
        queried_pair[x[u'bin_width']] = DeltaCounts(dataset_id = d[u'dataset_id'], source_concept_id = d[u'source_concept_id'], 
                                                    target_concept_id = d[u'target_concept_id'],
                                                    source_concept_name = d[u'source_concept_name'],                            
                                                    target_concept_name = d[u'target_concept_name'], 
                                                    source_concept_count = d[u'source_concept_count'],
                                                    concept_pair_count = d[u'concept_pair_count'],
                                                    target_concept_count = d[u'target_concept_count'],
                                                    counts = d[u'counts'], confidence = np.array(d[u'confidence_interval']).T,
                                                    bin_width = d[u'bin_width'], n = d[u'n'])      
        
    source_comparison = dict()
    for x in result[u'source_comparison']:
        source_comparison[x[u'bin_width']] = {
            u'cad_similarities': x[u'cad_similarities'],
            u'deltas': [DeltaCounts(dataset_id = d[u'dataset_id'], source_concept_id = d[u'source_concept_id'], 
                                    target_concept_id = d[u'target_concept_id'],
                                    source_concept_name = d[u'source_concept_name'],                            
                                    target_concept_name = d[u'target_concept_name'], 
                                    source_concept_count = d[u'source_concept_count'],
                                    concept_pair_count = d[u'concept_pair_count'],
                                    target_concept_count = d[u'target_concept_count'],
                                    counts = d[u'counts'], confidence = np.array(d[u'confidence_interval']).T,
                                    bin_width = d[u'bin_width'], n = d[u'n']) 
                        for d in x[u'deltas']],
            u'distribution': np.array(x[u'distribution']) if x[u'distribution'] is not None else None,
            u'significance': x[u'significance'] if x[u'significance'] is not None else None
        }
            
    target_comparison = dict()
    for x in result[u'target_comparison']:
        target_comparison[x[u'bin_width']] = {
            u'cad_similarities': x[u'cad_similarities'],
            u'deltas': [DeltaCounts(dataset_id = d[u'dataset_id'], source_concept_id = d[u'source_concept_id'], 
                                    target_concept_id = d[u'target_concept_id'],
                                    source_concept_name = d[u'source_concept_name'],                            
                                    target_concept_name = d[u'target_concept_name'], 
                                    source_concept_count = d[u'source_concept_count'],
                                    concept_pair_count = d[u'concept_pair_count'],
                                    target_concept_count = d[u'target_concept_count'],
                                    counts = d[u'counts'], confidence = np.array(d[u'confidence_interval']).T,
                                    bin_width = d[u'bin_width'], n = d[u'n']) 
                        for d in x[u'deltas']],
            u'distribution': np.array(x[u'distribution']) if x[u'distribution'] is not None else None,
            u'significance': x[u'significance'] if x[u'significance'] is not None else None
        }    
        
    combined_comparison = dict()
    for x in result[u'combined_comparison']:
        combined_comparison[x[u'bin_width']] = {
            u'distribution': np.array(x[u'distribution']) if x[u'distribution'] is not None else None,
            u'significance': x[u'significance'] if x[u'significance'] is not None else None
        }    
            
    return queried_pair, source_comparison, target_comparison, combined_comparison



# ######################################################################
# Translator API
# ######################################################################
def translator_query(node_1_curie, node_2_curie=None, node_2_type=None, max_results=500,
                     confidence_interval=None, dataset_id=3, local_oxo=True, method='obsExpRatio',
                     min_cooccurrence=None, ontology_targets=None, biolink_only=True, threshold=None):
    """NCATS Translator Reasoner API. See documentation: https://github.com/NCATS-Tangerine/NCATS-ReasonerStdAPI

    Parameters
    ----------
    node_1_curie - CURIE of node 1, e.g., "DOID:9053"
    node_2_curie - [Optional] CURIE of node 2, e.g., "DOID:9053". One of node_2_curie or node_2_type is required.
    node_2_type - [Optional] node 2 semantic type, e.g., "procedure". One of node_2_curie or node_2_type is required.
    max_results - Maximum number of results. Default: 500
    confidence_interval - [Optional] Confidence interval for associations
    dataset_id - COHD dataset ID. See datasets function. Default: 3
    local_oxo - True to use COHD's local implementation of OxO (faster, but not up to date). Default: True
    method - Association metric. One of: 'obsExpRatio' (default), 'relativeFrequency', or 'chiSquare'
    min_cooccurrence - [Optional] Criteria that the results have a minimum co-occurrence count
    threshold - [Optional] Criteria threshold to apply to the association metric. chiSquare: p-value < threshold.
                obsExpRatio: abs(ln_ratio) >= threshold. relativeFrequency: relative_frequency >= threshold.
    ontology_targets - [Optional] Desired ontologies for results to be mapped to

    Returns
    -------
    Translator Reasoner Standard API Message JSON
    """
    url = f'{server}/translator/query'

    # Node 1
    node_1 = {
        "id": "n00",
        "curie": node_1_curie
    }

    # Node 2
    node_2 = {
        "id": "n01",
    }
    if node_2_curie is not None:
        node_2["curie"] = node_2_curie
    if node_2_type is not None:
        node_2["type"] = node_2_type

    # Query options
    query_options = {
                        "method": method,
                        "dataset_id": dataset_id,
                        "local_oxo": local_oxo,
                        "biolink_only": biolink_only,
                        "ontology_targets": None
                    }
    if confidence_interval is not None:
        query_options["confidence_interval"] = confidence_interval
    if min_cooccurrence is not None:
        query_options["min_cooccurrence"] = min_cooccurrence
    if threshold is not None:
        query_options["threshold"] = threshold
    if ontology_targets is not None:
        query_options["ontology_targets"] = ontology_targets

    query = {
              "max_results": max_results,
              "message": {
                "query_graph": {
                  "nodes": [node_1, node_2],
                  "edges": [
                    {
                      "id": "e00",
                      "type": "association",
                      "source_id": "n00",
                      "target_id": "n01"
                    }
                  ]
                }
              },
              "query_options": query_options
            }

    response = requests.post(url, json=query)
    if response.status_code == requests.status_codes.codes.OK:
        return response.json()
    else:
        response.raise_for_status()

