import numpy as np
from scipy.stats import poisson


def poisson_ci(freq, alpha=0.99):
    """ Assuming two Poisson processes (1 for the event rate and 1 for randomization), calculate the confidence interval
    for the true rate

    Parameters
    ----------
    freq: float - co-occurrence frequency
    alpha: float - desired confidence. range: [0, 1]

    Returns
    -------
    (lower bound, upper bound)
    """
    # Adjust the interval for each individual poisson to achieve overall confidence interval
    return poisson.interval(alpha, freq)


def double_poisson_ci(freq, alpha=0.99):
    """ Assuming two Poisson processes (1 for the event rate and 1 for randomization), calculate the confidence interval
    for the true rate

    Parameters
    ----------
    freq: float - co-occurrence frequency
    alpha: float - desired confidence. range: [0, 1]

    Returns
    -------
    (lower bound, upper bound)
    """
    # Adjust the interval for each individual poisson to achieve overall confidence interval
    alpha_adjusted = 1 - (1 - alpha) ** 0.5
    return (poisson.interval(alpha_adjusted, poisson.interval(alpha_adjusted, freq)[0])[0],
            poisson.interval(alpha_adjusted, poisson.interval(alpha_adjusted, freq)[1])[1])


def ln_ratio_ci(freq, ln_ratio, alpha=0.99):
    """ Estimates the confidence interval of the log ratio using the double poisson method

    Parameters
    ----------
    freq: float - co-occurrence count
    ln_ratio: float - log ratio
    alpha: float - desired confidence. range: [0, 1]

    Returns
    -------
    (lower bound, upper bound)
    """
    # Convert ln_ratio back to ratio and calculate confidence intervals for the ratios
    return tuple(np.log(np.array(double_poisson_ci(freq, alpha)) * np.exp(ln_ratio) / freq))


def rel_freq_ci(pair_count, base_count, alpha=0.99):
    """ Estimates the confidence interval of the relative frequency using the double poisson method

    Parameters
    ----------
    pair_count: int - co-occurrence count
    base_count: int - base concept count
    alpha: float - desired confidence. range: [0, 1]

    Returns
    -------
    (lower bound, upper bound)
    """
    pair_count_ci = double_poisson_ci(pair_count, alpha)
    base_count_ci = double_poisson_ci(base_count, alpha)
    return pair_count_ci[0] / base_count_ci[1], pair_count_ci[1] / base_count_ci[0]


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
    return u'http://api.ohdsi.org/WebAPI/vocabulary/concept/{concept_id}'.format(concept_id=concept_id)


def omop_concept_curie(concept_id):
    """ Returns CURIE for concept_id using OMOP

    Parameters
    ----------
    concept_id: String - OMOP concept ID

    Returns
    -------
    OMOP:{concept_id}
    """
    return u'OMOP:{concept_id}'.format(concept_id=concept_id)
