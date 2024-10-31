from collections import namedtuple, Counter
import json
from datetime import datetime
import os
from os import path
import logging

import numpy as np
from scipy.stats import poisson, chisquare


N_PATIENTS = {
    1: 1790431,
    3: 1731858
}
N_CONCEPT_PAIRS = {
    1: 15927195,
    3: 197683321
}
THRESHOLD_COUNT = 10
CONFIDENCE = 0.99  # Confidence interval level
LN_RATIO_THRESHOLD = 1.0
JSON_INFINITY_REPLACEMENT = 999  # Proper JSON does not allow infinity. Replace infinity with arbitrarily large value
MIN_P = 1e-12  # ARAX displays p-value of 0 as None. Replace with a minimum p-value
EARLY_STOPPING = 50  # Number of edges to create per file before stopping. Set to 0 or False to disable early stopping
INFORES_ID = 'infores:cohd'
KNOWLEDGE_LEVEL = 'statistical_association'
AGENT_TYPE = 'data_analysis_pipeline'

# Input data dir and files
DIR_DATA = '20241030'  
files_count_data = [
    'counts_ds1.tsv',
    'counts_cd.tsv',
    'counts_dc.tsv',
    'counts_dd.tsv',
    'counts_dp.tsv',
    'counts_pd.tsv'
]

# Create output dir with today's date
dir_output = datetime.now().strftime('kgx_%Y%m%d')
if not path.exists(dir_output):
    os.mkdir(dir_output)

logging.basicConfig(filename=path.join(dir_output, 'kgx.log'), level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def poisson_ci(freq, confidence=0.99):
    """ Assuming two Poisson processes (1 for the event rate and 1 for randomization), calculate the confidence interval
    for the true rate

    Parameters
    ----------
    freq: float - co-occurrence frequency
    confidence: float - desired confidence. range: [0, 1]

    Returns
    -------
    (lower bound, upper bound)
    """
    # # Adjust the interval for each individual poisson to achieve overall confidence interval
    # return poisson.interval(confidence, freq)

    # COHD defaults to confidence values of 0.99 and 0.999 (double poisson), so cache these values to save compute time
    use_cache = (confidence == 0.99 or confidence == 0.999)
    if use_cache:
        cache = _poisson_ci_cache[confidence]
        if freq in cache:
            return cache[freq]

    # Same result as using poisson.interval, but much faster calculation
    alpha = 1 - confidence
    ci = poisson.ppf([alpha / 2, 1 - alpha / 2], freq)
    ci[0] = max(ci[0], 1)  # min possible count is 1
    ci = tuple(ci)

    if use_cache:
        # Only cache results for 99% and 99.9% CI
        cache[freq] = ci
    return ci


# Pre-cache values for poisson_ci. Confidence values of 0.99 and 0.999 are commonly used. Caching up to a freq of 10000
# covers 99% of co-occurrence counts in COHD and takes up < 1MB RAM for both confidence levels.
# Note: also evaluated a hybrid implementation using both lists and dicts, but had exact same performance
_poisson_ci_cache = {
    0.99: dict(),
    0.999: dict()
}
for i in range(10000):
    poisson_ci(i, confidence=0.99)
    poisson_ci(i, confidence=0.999)


def double_poisson_ci(freq, confidence=0.99):
    """ Assuming two Poisson processes (1 for the event rate and 1 for randomization), calculate the confidence interval
    for the true rate

    Parameters
    ----------
    freq: float - co-occurrence frequency
    confidence: float - desired confidence. range: [0, 1]

    Returns
    -------
    (lower bound, upper bound)
    """
    # # Adjust the interval for each individual poisson to achieve overall confidence interval
    # confidence_adjusted = 1 - (1 - confidence) ** 0.5
    # return (poisson.interval(confidence_adjusted, poisson.interval(confidence_adjusted, freq)[0])[0],
    #         poisson.interval(confidence_adjusted, poisson.interval(confidence_adjusted, freq)[1])[1])

    # More efficient calculation using a single call to poisson.interval with similar results as above
    # Adjust the interval for each individual poisson to achieve overall confidence interval
    confidence_adjusted = 1 - ((1 - confidence) ** 1.5)
    return poisson_ci(freq, confidence_adjusted)


def ln_ratio_ci(freq, ln_ratio, confidence=0.99, replace_inf=None):
    """ Estimates the confidence interval of the log ratio using the double poisson method

    Parameters
    ----------
    freq: float - co-occurrence count
    ln_ratio: float - log ratio
    confidence: float - desired confidence. range: [0, 1]
    replace_inf: (Optional) If specified, replaces +Inf or -Inf with +replace_inf or -replace_inf (useful because JSON
                 doesn't allow Infinity)

    Returns
    -------
    (lower bound, upper bound)
    """
    # Convert ln_ratio back to ratio and calculate confidence intervals for the ratios
    ci = tuple(np.log(np.array(double_poisson_ci(freq, confidence)) * np.exp(ln_ratio) / freq))
    if replace_inf:
        ci = max(ci[0], -replace_inf), min(ci[1], replace_inf)
    return ci


def rel_freq_ci(pair_count, base_count, confidence=0.99, replace_inf=None):
    """ Estimates the confidence interval of the relative frequency using the double poisson method

    Parameters
    ----------
    pair_count: int - co-occurrence count
    base_count: int - base concept count
    confidence: float - desired confidence. range: [0, 1]
    replace_inf: (Optional) If specified, replaces +Inf or -Inf with +replace_inf or -replace_inf (useful because JSON
                 doesn't allow Infinity)

    Returns
    -------
    (lower bound, upper bound)
    """
    pair_count_ci = poisson_ci(pair_count, confidence)
    base_count_ci = poisson_ci(base_count, confidence)
    ci = pair_count_ci[0] / base_count_ci[1], pair_count_ci[1] / base_count_ci[0]
    if replace_inf:
        ci = ci[0], min(ci[1], replace_inf)
    return ci


def log_odds(c1, c2, cp, n, replace_inf=np.inf):
    """ Calculates the log-odds and 95% CI 

    Params
    ------
    c1: count for concept 1
    c2: count for concept 2
    cp: concept-pair count
    n: total population size
    replace_inf: (Optional) If specified, replaces +Inf or -Inf with +replace_inf or -replace_inf (useful because JSON
                 doesn't allow Infinity)

    Returns
    -------
    (log-odds, [95% CI lower bound, 95% CI upper bound])
    """
    a = cp
    b = c1 - cp
    c = c2 - cp
    d = n - c1 - c2 + cp
    # Check b/c <= 0 since Poisson perturbation can cause b or c to be negative
    if b <= 0 or c <= 0:
        if a == 0:
            return 0, [0, 0]
        else:
            return replace_inf, [replace_inf, replace_inf]
    else:
        log_odds = np.log((a*d)/(b*c))
        ci = 1.96 * np.sqrt(1/a + 1/b + 1/c + 1/d)
        # Strict JSON doesn't allow Inf values, replace as necessary
        ci = [clip(log_odds - ci, replace_inf), clip(log_odds + ci, replace_inf)]        
        return clip(log_odds, replace_inf), ci
    

def chi_square(cpc, c1, c2, pts, n_concept_pairs, min_p=MIN_P):
    """ Calculate p-value and Bonferonni-adjusted p-value using Chi-square

    Params
    ------
    cpc: concept-pair count
    c1: count for concept 1
    c2: count for concept 2
    pts: total population size
    n_concept_pairs: number of pairs of concepts in dataset
    min_p: minimum p-value to return 
    """
    neg = pts - c1 - c2 + cpc
    # Create the observed and expected RxC tables and perform chi-square
    o = [neg, c1 - cpc, c2 - cpc, cpc]
    e = [(pts - c1) * (pts - c2) / pts, c1 * (pts - c2) / pts, c2 * (pts - c1) / pts, c1 * c2 / pts]
    cs = chisquare(o, e, 2)
    p = max(cs.pvalue, min_p)
    p_bonferonni = max(min(cs.pvalue * n_concept_pairs, 1.0), min_p)  # Bonferonni adjustment
    return p, p_bonferonni

    
def clip(x, clip):
    """ Clip values to [-clip, clip] 
    
    Params
    ------
    x: value to clip
    clip: value to clip to 
    
    Returns
    -------
    clipped value 
    """
    # return min(max(x, -clip), clip)
    return -clip if x < -clip else clip if x > clip else x    

# Read OMOP concep definitions
omop_concepts = dict()
with open(path.join(DIR_DATA, 'concepts.tsv'), 'r') as f_concepts:
    # skip header line
    f_concepts.readline()
    while line := f_concepts.readline():
        line_split = line.strip().split('\t')
        if len(line_split) == 2:
            # For some reason, many Read (vocabulary) concepts don't have concept_name. Give it a fake name
            line_split.append("Error: Read vocabulary concept missing concept_name")
        omop_id, domain_id, concept_name = line_split
        omop_concepts[int(omop_id)] = {
            'id': 'OMOP:' + omop_id,
            'name': concept_name,
            'domain': domain_id
        }

# Read Biolink mappings into a dict and create KG nodes
mappings = dict()
nodes = dict()
with open(path.join(DIR_DATA, 'mappings.tsv'), 'r') as f_mappings:
    # skip header line
    f_mappings.readline()
    
    while line := f_mappings.readline():
        row = line.strip().split('\t')
        omop_id, biolink_id, biolink_label, categories = row[:4]
        omop_id = int(omop_id)

        omop_concept = omop_concepts.get(omop_id)
        if omop_concept is not None:            
            mappings[omop_id] = biolink_id        

            # Create KG Node         
            attributes = [
                {
                    "attribute_source": INFORES_ID,
                    "attribute_type_id": "EDAM:data_0954",
                    "attributes": [
                        {
                            "attribute_source": "infores:omop-ohdsi",
                            "attribute_type_id": "EDAM:data_1087",
                            "original_attribute_name": "concept_id",
                            "value": omop_concept["id"],
                            "value_type_id": "EDAM:data_1087",
                            "value_url": f"https://athena.ohdsi.org/search-terms/terms/{omop_id}"
                        },
                        {
                            "attribute_source": "infores:omop-ohdsi",
                            "attribute_type_id": "EDAM:data_2339",
                            "original_attribute_name": "concept_name",
                            "value": omop_concept["name"],
                            "value_type_id": "EDAM:data_2339"
                        },
                        {
                            "attribute_source": "infores:omop-ohdsi",
                            "attribute_type_id": "EDAM:data_0967",
                            "original_attribute_name": "domain",
                            "value": omop_concept["domain"],
                            "value_type_id": "EDAM:data_0967"
                        }
                    ],
                    "original_attribute_name": "Database cross-mapping",
                    "value": "(OMOP:2313993)-[OMOP Map]-(CPT:93976)",
                    "value_type_id": "EDAM:data_0954"
                }
            ]
            nodes[biolink_id] = {
                "id": biolink_id,
                "name": biolink_label,
                "categories": json.loads(categories),
                "attributes": [json.dumps(a) for a in attributes]
            }
        else:
            # No definition for concept
            logging.warning(f"No OMOP concept definition found for {line}")
            continue
        

t1 = datetime.now()

node_counter = Counter()
count_lines = 0
count_edges_total = 0
log_odds_values = list()
log_odds_max_counter = 0

with open(path.join(dir_output, 'cohd_edges.jsonl'), 'w') as f_edges:
    for file_count_data in files_count_data:
        logging.info(f'Processing file {file_count_data}')
        count_edges_file = 0

        with open(path.join(DIR_DATA, file_count_data), 'r') as f_counts:
            # skip header line
            f_counts.readline()
            
            while line := f_counts.readline():
                count_lines += 1                
                if (count_lines % 1000000) == 0:
                    logging.info(f'{count_lines} lines processed')

                omop_id_1, omop_id_2, count_1, count_2, count_pair, dataset_id = [int(x) for x in line.strip().split('\t')]
                count_1 = count_1
                count_2 = count_2
                count_pair = count_pair
                biolink_id_1 = mappings[omop_id_1]
                biolink_id_2 = mappings[omop_id_2]
                n_patients = N_PATIENTS[dataset_id]
                n_concept_pairs = N_CONCEPT_PAIRS[dataset_id]
                
                if count_1 <= THRESHOLD_COUNT or count_2 <= THRESHOLD_COUNT or count_pair <= THRESHOLD_COUNT:
                    continue

                # calculate ln_ratio
                count_expected = count_1 * count_2 / (n_patients)
                lnr = np.log(count_pair * n_patients / (count_1 * count_2))
                lnr_ci = ln_ratio_ci(count_pair, lnr, CONFIDENCE, JSON_INFINITY_REPLACEMENT)
                
                if lnr_ci[0] > LN_RATIO_THRESHOLD or lnr_ci[1] < -LN_RATIO_THRESHOLD:
                    count_edges_file += 1

                    # calculate relative frequency
                    rf1 = count_pair / count_1
                    rf1_ci = rel_freq_ci(count_pair, count_1, CONFIDENCE, JSON_INFINITY_REPLACEMENT)
                    rf2 = count_pair / count_2
                    rf2_ci = rel_freq_ci(count_pair, count_2, CONFIDENCE, JSON_INFINITY_REPLACEMENT)

                    # calculate chi-square
                    p, p_bonferonni = chi_square(count_pair, count_1, count_2, n_patients, n_concept_pairs)
                    
                    # calculate log-odds
                    lo, lo_ci = log_odds(count_1, count_2, count_pair, n_patients, JSON_INFINITY_REPLACEMENT)
                    log_odds_values.append(lo)
                    
                    # Checking log-odds for max values
                    if abs(lo) > JSON_INFINITY_REPLACEMENT or abs(lo_ci[0]) > JSON_INFINITY_REPLACEMENT or abs(lo_ci[1]) > JSON_INFINITY_REPLACEMENT:
                        logging.warning(f'log odds greater than {JSON_INFINITY_REPLACEMENT}')
                    if lo == JSON_INFINITY_REPLACEMENT:
                        log_odds_max_counter += 1

                    # Convention: subj <-> concept 1; obj <-> concept 2
                    curie_subj = biolink_id_1
                    curie_obj = biolink_id_2
                    count_subj_value = count_1
                    count_obj_value = count_2
                    count_study_value = f"{curie_subj}: {count_subj_value}; {curie_obj}: {count_obj_value}; pair: {count_pair}"
                    chi_study_value = f"p-value: {p:.2e}; Bonferonni p-value: {p_bonferonni:.2e}"
                    oefr_study_value = f"{lnr:.3f} [{lnr_ci[0]:.3f}, {lnr_ci[1]:.3f}]"
                    rel_freq_subj_value = rf1
                    rel_freq_subj_ci_value = rf1_ci
                    rel_freq_obj_value = rf2
                    rel_freq_obj_ci_value = rf2_ci
                    rel_freq_study_value = f"Relative to {curie_subj}: {rel_freq_subj_value:.3f} [{rel_freq_subj_ci_value[0]:.3f}, {rel_freq_subj_ci_value[1]:.3f}]; " \
                                        f"Relative to {curie_obj}: {rel_freq_obj_value:.3f} [{rel_freq_obj_ci_value[0]:.3f}, {rel_freq_obj_ci_value[1]:.3f}]"
                    log_odds_study_value = f"{lo:.3f} [{lo_ci[0]:.3f}, {lo_ci[1]:.3f}]"

                    # Build attributes
                    attributes = [
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association"
                        },
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:agent_type",
                            "value": "data_analysis_pipeline"
                        },
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:has_supporting_study_result",
                            "description": "A study result describing the initial count of concepts",
                            "value": count_study_value,
                            "value_type_id": "biolink:ConceptCountAnalysisResult",
                            'value_url': 'https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP',
                            "attributes": [
                                {
                                    'attribute_type_id': 'biolink:concept_pair_count',
                                    'original_attribute_name': 'concept_pair_count',
                                    'value': count_pair,
                                    'value_type_id': 'EDAM:data_0006',  # Data
                                    'attribute_source': INFORES_ID,
                                    'description': 'Observed concept count between the pair of subject and object nodes'
                                },
                                {
                                    'attribute_type_id': 'biolink:concept_count_subject',
                                    'original_attribute_name': 'concept_count_subject',
                                    'value': count_subj_value,
                                    'value_type_id': 'EDAM:data_0006',  # Data
                                    'attribute_source': INFORES_ID,
                                    'description': f'Observed concept count of the subject node ({curie_subj})'
                                },
                                {
                                    'attribute_type_id': 'biolink:concept_count_object',
                                    'original_attribute_name': 'concept_count_object',
                                    'value': count_obj_value,
                                    'value_type_id': 'EDAM:data_0006',  # Data
                                    'attribute_source': INFORES_ID,
                                    'description': f'Observed concept count of the object node ({curie_obj})'
                                },
                                {
                                    'attribute_type_id': 'biolink:dataset_count',
                                    'original_attribute_name': 'patient_count',
                                    'value': n_patients,
                                    'value_type_id': 'EDAM:data_0006',  # Data
                                    'attribute_source': INFORES_ID,
                                    'description': 'Number of patients in the COHD dataset'
                                },
                                {
                                    'attribute_type_id': 'biolink:supporting_data_set', 
                                    'original_attribute_name': 'dataset_id',
                                    'value': f"COHD:dataset_{dataset_id}",
                                    'value_type_id': 'EDAM:data_1048',  # Database ID
                                    'attribute_source': INFORES_ID,
                                    'description': f'Dataset ID within COHD'
                                },
                                # Knowledge Level
                                {
                                    'attribute_type_id': 'biolink:knowledge_level',  
                                    'value': KNOWLEDGE_LEVEL,
                                    'attribute_source': INFORES_ID
                                },
                                # Agent Type
                                {
                                    'attribute_type_id': 'biolink:agent_type',  
                                    'value': AGENT_TYPE,
                                    'attribute_source': INFORES_ID
                                }
                            ]
                        },
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:has_supporting_study_result",
                            "description": "A study result describing a chi-squared analysis on a single pair of concepts",
                            "value": chi_study_value,
                            "value_type_id": "biolink:ChiSquaredAnalysisResult",
                            'value_url': 'https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP',
                            "attributes": [
                                {
                                    'attribute_type_id': 'biolink:unadjusted_p_value',
                                    'original_attribute_name': 'p-value',
                                    'value': p,
                                    'value_type_id': 'EDAM:data_1669',  # P-value
                                    'attribute_source': INFORES_ID,
                                    'value_url': 'http://edamontology.org/data_1669',
                                    'description': 'Chi-square p-value, unadjusted.'
                                },
                                {
                                    'attribute_type_id': 'biolink:bonferonni_adjusted_p_value',
                                    'original_attribute_name': 'p-value adjusted',
                                    'value': p_bonferonni,
                                    'value_type_id': 'EDAM:data_1669',  # P-value
                                    'attribute_source': INFORES_ID,
                                    'value_url': 'http://edamontology.org/data_1669',
                                    'description': 'Chi-square p-value, Bonferonni adjusted by number of pairs of concepts.'
                                },
                                {
                                    'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                                    'original_attribute_name': 'dataset_id',
                                    'value': f"COHD:dataset_{dataset_id}",
                                    'value_type_id': 'EDAM:data_1048',  # Database ID
                                    'attribute_source': INFORES_ID,
                                    'description': f'Dataset ID within COHD'
                                },
                                # Knowledge Level
                                {
                                    'attribute_type_id': 'biolink:knowledge_level',  
                                    'value': KNOWLEDGE_LEVEL,
                                    'attribute_source': INFORES_ID
                                },
                                # Agent Type
                                {
                                    'attribute_type_id': 'biolink:agent_type',  
                                    'value': AGENT_TYPE,
                                    'attribute_source': INFORES_ID
                                }
                            ]
                        },
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:has_supporting_study_result",
                            "description": "A study result describing a relative frequency anaylsis on a single pair of concepts",
                            "value": rel_freq_study_value,
                            "value_type_id": "biolink:RelativeFrequencyAnalysisResult",
                            'value_url': 'https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP',
                            "attributes": [
                                {
                                    'attribute_type_id': 'biolink:relative_frequency_subject',
                                    'original_attribute_name': 'relative_frequency_subject',
                                    'value': rel_freq_subj_value,
                                    'value_type_id': 'EDAM:data_1772',  # Score
                                    'attribute_source': INFORES_ID,
                                    'description': f'Relative frequency, relative to the subject node ({curie_subj}).'
                                },
                                {
                                    'attribute_type_id': 'biolink:relative_frequency_subject_confidence_interval',
                                    'original_attribute_name': 'relative_freq_subject_confidence_interval',
                                    'value': rel_freq_subj_ci_value,
                                    'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                                    'attribute_source': INFORES_ID,
                                    'description': f'Relative frequency (subject) {CONFIDENCE*100}% confidence interval'
                                },
                                {
                                    'attribute_type_id': 'biolink:relative_frequency_object',
                                    'original_attribute_name': 'relative_frequency_object',
                                    'value': rel_freq_obj_value,
                                    'value_type_id': 'EDAM:data_1772',  # Score
                                    'attribute_source': INFORES_ID,
                                    'description': f'Relative frequency, relative to the object node ({curie_obj}).'
                                },
                                {
                                    'attribute_type_id': 'biolink:relative_frequency_object_confidence_interval',
                                    'original_attribute_name': 'relative_freq_object_confidence_interval',
                                    'value': rel_freq_obj_ci_value,
                                    'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                                    'attribute_source': INFORES_ID,
                                    'description': f'Relative frequency (object) {CONFIDENCE*100}% confidence interval'
                                },
                                {
                                    'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                                    'original_attribute_name': 'dataset_id',
                                    'value': f"COHD:dataset_{dataset_id}",
                                    'value_type_id': 'EDAM:data_1048',  # Database ID
                                    'attribute_source': INFORES_ID,
                                    'description': f'Dataset ID within COHD'
                                },
                                # Knowledge Level
                                {
                                    'attribute_type_id': 'biolink:knowledge_level',  
                                    'value': KNOWLEDGE_LEVEL,
                                    'attribute_source': INFORES_ID
                                },
                                # Agent Type
                                {
                                    'attribute_type_id': 'biolink:agent_type',  
                                    'value': AGENT_TYPE,
                                    'attribute_source': INFORES_ID
                                }
                            ]
                        },
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:has_supporting_study_result",
                            "description": "A study result describing an observed-expected frequency anaylsis on a single pair of concepts",
                            "value": oefr_study_value,
                            "value_type_id": "biolink:ObservedExpectedFrequencyAnalysisResult",
                            'value_url': 'https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP',
                            "attributes": [
                                {
                                    'attribute_type_id': 'biolink:expected_count',
                                    'original_attribute_name': 'expected_count',
                                    'value': count_expected,
                                    'value_type_id': 'EDAM:operation_3438',
                                    'attribute_source': INFORES_ID,
                                    'description': 'Calculated expected count of concept pair.'
                                },
                                {
                                    'attribute_type_id': 'biolink:ln_ratio',
                                    'original_attribute_name': 'ln_ratio',
                                    'value': lnr,
                                    'value_type_id': 'EDAM:data_1772',  # Score
                                    'attribute_source': INFORES_ID,
                                    'description': 'Observed-expected frequency ratio.'
                                },
                                {
                                    'attribute_type_id': 'biolink:ln_ratio_confidence_interval',
                                    'original_attribute_name': 'ln_ratio_confidence_interval',
                                    'value': lnr_ci,
                                    'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                                    'attribute_source': INFORES_ID,
                                    'description': f'Observed-expected frequency ratio {CONFIDENCE*100}% confidence interval'
                                },
                                {
                                    'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                                    'original_attribute_name': 'dataset_id',
                                    'value': f"COHD:dataset_{dataset_id}",
                                    'value_type_id': 'EDAM:data_1048',  # Database ID
                                    'attribute_source': INFORES_ID,
                                    'description': f'Dataset ID within COHD'
                                },
                                # Knowledge Level
                                {
                                    'attribute_type_id': 'biolink:knowledge_level',  
                                    'value': KNOWLEDGE_LEVEL,
                                    'attribute_source': INFORES_ID
                                },
                                # Agent Type
                                {
                                    'attribute_type_id': 'biolink:agent_type',  
                                    'value': AGENT_TYPE,
                                    'attribute_source': INFORES_ID
                                }
                            ]
                        },
                        {
                            "attribute_source": INFORES_ID,
                            "attribute_type_id": "biolink:has_supporting_study_result",
                            "description": "A study result describing a log-odds anaylsis on a single pair of concepts",
                            "value": log_odds_study_value,
                            "value_type_id": "biolink:LogOddsAnalysisResult",
                            'value_url': 'https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP',
                            "attributes": [
                                {
                                    'attribute_type_id': 'biolink:log_odds_ratio',
                                    'original_attribute_name': 'log_odds',
                                    'value': lo,
                                    'value_type_id': 'EDAM:data_1772',  # Score
                                    'attribute_source': INFORES_ID,
                                    'description': 'Natural logarithm of the odds-ratio'
                                },
                                {
                                    'attribute_type_id': 'biolink:log_odds_ratio_95_ci',
                                    'original_attribute_name': 'log_odds_ci',
                                    'value': lo_ci,
                                    'value_type_id': 'EDAM:data_0951',  # Statistical estimate score
                                    'attribute_source': INFORES_ID,
                                    'description': f'Log-odds 95% confidence interval'
                                },
                                {
                                    'attribute_type_id': 'biolink:total_sample_size',
                                    'original_attribute_name': 'concept_pair_count',
                                    'value': count_pair,
                                    'value_type_id': 'EDAM:data_0006',  # Data
                                    'attribute_source': INFORES_ID,
                                    'description': 'Observed concept count between the pair of subject and object nodes'
                                },
                                {
                                    'attribute_type_id': 'biolink:supporting_data_set',  # Database ID
                                    'original_attribute_name': 'dataset_id',
                                    'value': f"COHD:dataset_{dataset_id}",
                                    'value_type_id': 'EDAM:data_1048',  # Database ID
                                    'attribute_source': INFORES_ID,
                                    'description': f'Dataset ID within COHD'
                                },
                                # Knowledge Level
                                {
                                    'attribute_type_id': 'biolink:knowledge_level',  
                                    'value': KNOWLEDGE_LEVEL,
                                    'attribute_source': INFORES_ID
                                },
                                # Agent Type
                                {
                                    'attribute_type_id': 'biolink:agent_type',  
                                    'value': AGENT_TYPE,
                                    'attribute_source': INFORES_ID
                                }
                            ]
                        }
                    ]
                    # Convert attributes to JSON strings
                    attributes = [json.dumps(a) for a in attributes]
                    
                    predicate = 'biolink:positively_correlated_with' if lo_ci[0] > 0 else 'biolink:negatively_correlated_with'    
                    edge = {
                        'subject': biolink_id_1,
                        'object': biolink_id_2,
                        'predicate': predicate,
                        'attributes': attributes,
                        'sources': [
                            {
                                "resource_id": "infores:columbia-cdw-ehr-data",
                                "resource_role": "supporting_data_source"
                            },
                            {
                                "resource_id": INFORES_ID,
                                "resource_role": "primary_knowledge_source",
                                "upstream_resource_ids": [
                                    "infores:columbia-cdw-ehr-data"
                                ]
                            }
                        ]
                    }
                    f_edges.write(json.dumps(edge) + '\n')

                    # Keep track of which nodes seen
                    node_counter[biolink_id_1] += 1
                    node_counter[biolink_id_2] += 1    

                    if EARLY_STOPPING and (count_edges_file >= EARLY_STOPPING):
                        logging.info('Early stopping')
                        break
            
            logging.info(f'{count_edges_file} edges created from file')
            count_edges_total += count_edges_file

            
with open(path.join(dir_output, 'cohd_nodes.jsonl'), 'w') as f_nodes:
    for node_id in node_counter:
        f_nodes.write(json.dumps(nodes[node_id]) + '\n')
        
logging.info(f'{(datetime.now() - t1).seconds} seconds')
logging.info(f'{count_edges_total} edges created total')
