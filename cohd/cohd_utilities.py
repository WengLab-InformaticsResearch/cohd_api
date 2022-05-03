from typing import NamedTuple, Optional
import numpy as np
from scipy.stats import poisson


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

    # Same result as using poisson.interval, but much faster calculation
    alpha = 1 - confidence
    ci = poisson.ppf([alpha / 2, 1 - alpha / 2], freq)
    ci[0] = max(ci[0], 1)  # min possible count is 1
    return tuple(ci)


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


def ci_significance(ci1, ci2=None):
    """ Checks for significance between either 1) a single confidence interval and 0 or 2) two confidence intervals

    Parameters
    ----------
    ci1: list - [lower bound, upper bound]
    ci2: list - [lower bound, upper bound]

    Returns
    -------
    true if confidence intervals do not overlap
    """
    # if ci2 not specified, then check ci1 against 0
    if ci2 is None:
        ci2 = [0, 0]
    
    # Check if the confidence intervals overlap
    return (ci1[0] > ci2[1]) or (ci2[0] > ci1[1])


def omop_concept_uri(concept_id):
    """ Returns URI for concept_id using OHDSI WebAPI as a reference

    Parameters
    ----------
    concept_id: String - OMOP concept ID

    Returns
    -------
    http://api.ohdsi.org/WebAPI/vocabulary/concept/{concept_id}
    """
    return 'http://api.ohdsi.org/WebAPI/vocabulary/concept/{concept_id}'.format(concept_id=concept_id)


def omop_concept_curie(concept_id):
    """ Returns CURIE for concept_id using OMOP

    Parameters
    ----------
    concept_id: String - OMOP concept ID

    Returns
    -------
    OMOP:{concept_id}
    """
    return 'OMOP:{concept_id}'.format(concept_id=concept_id)


class DomainClass(NamedTuple):
    """ Structure to hold OMOP domain_id and concept_class_id as a pair
    """
    domain_id: Optional[str]
    concept_class_id: Optional[str]
