# This code pasted together from prototyping notebook. Untested.
from getpass import getpass
from datetime import datetime
from collections import namedtuple
import sys
import logging

import numpy as np
import pandas as pd
from scipy.stats import poisson, chisquare
from sqlalchemy import create_engine
import mysql.connector


logFormatter = logging.Formatter("%(asctime)s %(levelname)s %(module)s:%(lineno)d: %(message)s")
rootLogger = logging.getLogger()
rootLogger.handlers.clear()
fileHandler = logging.handlers.RotatingFileHandler(f"prototype.log", maxBytes=10e6)
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.INFO)
rootLogger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.INFO)

pwd = getpass()
engine = create_engine(f'mysql+mysqlconnector://admin:{pwd}@tr-kp-clinical-db.ncats.io/cohd')
conn = engine.connect()

connection2 = mysql.connector.connect(host='tr-kp-clinical-db.ncats.io',
                                      database='cohd',
                                      user='admin',
                                      password=pwd)

sql = '''
SELECT COUNT(*)
FROM concept_counts
WHERE dataset_id = %(dataset_id)s;
'''
for dataset_id in range(1, 5):
    cur = conn.exec_driver_sql(sql, {'dataset_id': dataset_id})
    n_rows = cur.fetchone()[0]
    print(f'{dataset_id}: {n_rows} rows')

CohdCalculations = namedtuple('CohdCalculations', ['p_value', 'pair_count_ci', 'ln_ratio', 'ln_ratio_ci',
                                                   'log_odds', 'log_odds_ci'])


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
    alpha = 1 - confidence
    ci = poisson.ppf([alpha / 2, 1 - alpha / 2], freq)
    ci[0] = max(ci[0], 1)  # min possible count is 1
    ci = int(ci[0]), int(ci[1])
    return ci


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


def rel_freq_cis(pair_count, count_1, count_2, confidence=0.99):
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
    count_1_ci = poisson_ci(count_1, confidence)
    count_2_ci = poisson_ci(count_2, confidence)
    ci_1 = pair_count_ci[0] / count_1_ci[1], pair_count_ci[1] / count_1_ci[0]
    ci_2 = pair_count_ci[0] / count_2_ci[1], pair_count_ci[1] / count_2_ci[0]
    return ci_1, ci_2


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
    #     print('--------------')
    #     print(c1, c2, cp, n)
    #     print(a, b, c, d)
    # Check b/c <= 0 since Poisson perturbation can cause b or c to be negative
    if b <= 0 or c <= 0:
        if a == 0:
            return 0, [0, 0]
        else:
            return replace_inf, [replace_inf, replace_inf]
    else:
        log_odds = np.log((a * d) / (b * c))
        ci = 1.96 * np.sqrt(1 / a + 1 / b + 1 / c + 1 / d)
        # Strict JSON doesn't allow Inf values, replace as necessary
        ci = [log_odds - ci, log_odds + ci]
        return log_odds, ci


def chi(c1, c2, cpc, pts):
    """ Calculates chi-square p-value

    Params
    ------
    c1: count for concept 1
    c2: count for concept 2
    cpc: concept-pair count
    pts: total population size

    Returns
    -------
    chi-square -value (unadjusted)
    """
    neg = pts - c1 - c2 + cpc
    # Create the observed and expected RxC tables and perform chi-square
    o = [neg, c1 - cpc, c2 - cpc, cpc]
    e = [(pts - c1) * (pts - c2) / pts, c1 * (pts - c2) / pts, c2 * (pts - c1) / pts, c1 * c2 / pts]
    cs = chisquare(o, e, 2)
    return cs.pvalue


def calculations(count_1, count_2, pair_count, patient_count):
    count_1 = float(count_1)
    count_2 = float(count_2)
    pair_count = float(pair_count)
    patient_count = float(patient_count)
    p = chi(count_1, count_2, pair_count, patient_count)
    pair_count_ci = poisson_ci(pair_count, confidence=0.99)
    ln_ratio = np.log(pair_count * patient_count / (count_1 * count_2))
    lr_ci = ln_ratio_ci(pair_count, ln_ratio, confidence=0.99)
    # rf1_ci, rf2_ci = rel_freq_cis(pair_count, count_1, count_2, confidence=0.99)
    lo, lo_ci = log_odds(count_1, count_2, pair_count, patient_count, )
    return CohdCalculations(p, pair_count_ci, ln_ratio, lr_ci, lo, lo_ci)

### Update concept_counts table ###

sql_fetch = '''
SELECT concept_id, concept_count
FROM concept_counts
WHERE dataset_id = %(dataset_id)s
'''

sql_update = '''
UPDATE concept_counts
SET ci_lo = %s, ci_hi = %s
WHERE dataset_id = %s AND concept_id = %s;
'''
cur_update = connection2.cursor(prepared=True)

CONFIDENCE = 0.99
dataset_ids = [1, 2, 3, 4]
for dataset_id in dataset_ids:
    t1 = datetime.now()
    cur_fetch = conn.exec_driver_sql(sql_fetch, {'dataset_id': dataset_id})
    count = 0
    params_list = list()
    for row in cur_fetch:
        concept_id, concept_count = row
        ci_lo, ci_hi = poisson_ci(concept_count, CONFIDENCE)
        params_list.append((ci_lo, ci_hi, dataset_id, concept_id))

        count += 1
        if count % 5000 == 0:
            print(count)

    cur_update.executemany(sql_update, params_list)
    connection2.commit()

    delta = datetime.now() - t1
    print(f'{delta.total_seconds()} seconds')

### Update concept_pair_counts table ###

connection = mysql.connector.connect(host='tr-kp-clinical-db.ncats.io',
                                     database='cohd',
                                     user='admin',
                                     password=pwd)
cursor = connection.cursor()

sql_patients = '''
SELECT count
FROM patient_count
WHERE dataset_id = %s;
'''

sql_counts = '''
SELECT concept_id, concept_count
FROM concept_counts
WHERE dataset_id = %s
ORDER BY concept_id ASC;
'''

# splitting up the query into 2 queries looking for concept_id_1 and concept_id_2 separately is MUCH faster
sql_pair_counts_1 = '''
SELECT cc.concept_id, cc.concept_count, cpc.concept_count AS pair_count
FROM concept_pair_counts cpc
JOIN concept_counts cc ON cpc.dataset_id = cc.dataset_id AND cpc.concept_id_2 = cc.concept_id
WHERE cpc.dataset_id = %s AND concept_id_1 = %s AND cpc.p_value IS NULL
'''

sql_pair_counts_2 = '''
SELECT cc.concept_id, cc.concept_count, cpc.concept_count AS pair_count
FROM concept_pair_counts cpc
JOIN concept_counts cc ON cpc.dataset_id = cc.dataset_id AND cpc.concept_id_1 = cc.concept_id
WHERE cpc.dataset_id = %s AND concept_id_2 = %s AND cpc.p_value IS NULL
'''

sql_update = '''
UPDATE concept_pair_counts
SET p_value = %s, pair_count_ci_lo = %s, pair_count_ci_hi = %s, 
    ln_ratio = %s, ln_ratio_ci_lo = %s, ln_ratio_ci_hi = %s, 
    log_odds = %s, log_odds_ci_lo = %s, log_odds_ci_hi = %s
WHERE dataset_id = %s AND concept_id_1 = %s AND concept_id_2 = %s;
'''
cur_update = connection.cursor(prepared=True)

n_rows_dataset = [15927195, 32788901, 197683321, 56848043]
datasets = [2, 4]
for dataset_id in datasets:
    n_rows = n_rows_dataset[dataset_id - 1]
    logging.info(f'######## dataset_id {dataset_id}  - {n_rows} rows ########')

    # Get patient count
    cursor.execute(sql_patients, (dataset_id,))
    patient_count = float(cursor.fetchall()[0][0])

    cursor.execute(sql_counts, (dataset_id,))
    n_a = 0
    n_b = 0
    t1 = datetime.now()
    for row_counts in cursor.fetchall():
        concept_id_a, count_a = row_counts
        logging.debug(f'concept_id_a: {concept_id_a}')

        cursor.execute(sql_pair_counts_1, (dataset_id, concept_id_a,))
        sql_params = list()
        for row_pair_counts in cursor.fetchall():
            concept_id_b, count_b, pair_count = row_pair_counts
            cc = calculations(count_a, count_b, pair_count, patient_count)
            sql_params.append((cc.p_value, cc.pair_count_ci[0], cc.pair_count_ci[1],
                               cc.ln_ratio, cc.ln_ratio_ci[0], cc.ln_ratio_ci[1],
                               cc.log_odds, cc.log_odds_ci[0], cc.log_odds_ci[1],
                               dataset_id, concept_id_a, concept_id_b))
            n_b += 1
            logging.debug(f'concept_id_b: {concept_id_b}')
            logging.debug(cc)
        cur_update.executemany(sql_update, sql_params)

        cursor.execute(sql_pair_counts_2, (dataset_id, concept_id_a,))
        sql_params = list()
        for row_pair_counts in cursor.fetchall():
            concept_id_b, count_b, pair_count = row_pair_counts
            cc = calculations(count_b, count_a, pair_count, patient_count)
            sql_params.append((cc.p_value, cc.pair_count_ci[0], cc.pair_count_ci[1],
                               cc.ln_ratio, cc.ln_ratio_ci[0], cc.ln_ratio_ci[1],
                               cc.log_odds, cc.log_odds_ci[0], cc.log_odds_ci[1],
                               dataset_id, concept_id_b, concept_id_a))
            n_b += 1
            logging.debug(f'concept_id_b: {concept_id_b}')
            logging.debug(cc)
        cur_update.executemany(sql_update, sql_params)

        connection.commit()

        n_a += 1
        if n_a % 100 == 1 and n_b > 0:
            duration = (datetime.now() - t1).total_seconds() / 60 / 60
            est_speed = n_b / duration
            est_remain = (n_rows - n_b) / est_speed
            logging.info(
                f'{n_b} ({n_b / n_rows * 100}%) - {duration:0.1f} hours - {est_remain:0.1f} hours ({est_remain / 24:0.2f} days) remaining')

logging.info(f'{n_b} - {(datetime.now() - t1).total_seconds() / 60 / 60} hours')