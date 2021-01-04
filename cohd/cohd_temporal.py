from collections import defaultdict
import random

import numpy as np
from scipy.stats import poisson

from .query_cohd_mysql import *


DATASET_ID_DEFAULT_TEMPORAL = 4

# Value usd in COHD to indicate that the count was suppressed
SUPPRESSION_MARKER = 1

# Value used in place of suppressed numbers as an estimate
SUPPRESSION_ESTIMATE = 5


class AgeCounts:
    def __init__(self, dataset_id, concept_id, concept_name, concept_count, counts, bin_width):
        self.dataset_id = dataset_id
        self.concept_id = concept_id
        self.concept_name = concept_name
        self.bin_width = bin_width
        self.bins = len(counts)
        self.counts = np.array(counts, dtype=np.uint32)
        self.concept_count = concept_count

    def convert_bin_scheme(self, new_bin_width, new_bins=None, suppression_estimate=SUPPRESSION_ESTIMATE):
        # Make sure the new bin width is a multiple of the current bin width
        assert (new_bin_width % self.bin_width == 0)

        bin_ratio = int(new_bin_width / self.bin_width)

        if new_bins is None:
            new_bins = int(np.ceil(float(self.bins) / bin_ratio))

        new_counts = np.zeros(new_bins, dtype=np.uint32)

        # Replace suppressed counts with estimated counts
        estimated_counts = self.counts.copy()
        estimated_counts[estimated_counts == SUPPRESSION_MARKER] = suppression_estimate

        for i in range(new_bins - 1):
            new_counts[i] = np.sum(estimated_counts[(i * bin_ratio):((i + 1) * bin_ratio)])
        new_counts[new_bins - 1] = np.sum(estimated_counts[((new_bins - 1) * bin_ratio):])

        return AgeCounts(self.dataset_id, self.concept_id, self.concept_name, self.concept_count,
                         new_counts, new_bin_width)

    def convert_to_dict_results(self):
        """ Creates a dict results representation of this object for JSON returns

        Returns
        -------
        dict representation
        """
        return {
            'dataset_id': self.dataset_id,
            'concept_id': self.concept_id,
            'concept_name': self.concept_name,
            'concept_count': self.concept_count,
            'bin_width': self.bin_width,
            'counts': [int(x) for x in self.counts],
            'confidence_interval': [(int(x[0]), int(x[1])) for x in self.confidence_intervals()]
        }

    def confidence_intervals(self, alpha=0.99):
        """ Returns confidence intevals of counts

        Parameters
        ----------
        alpha

        Returns
        -------
        List of tuples with confidence intervals
        """
        return [poisson.interval(alpha, x) for x in self.counts]


class DeltaCounts:
    def __init__(self, dataset_id, source_concept_id, target_concept_id, source_concept_name, target_concept_name,
                 source_concept_count, target_concept_count, concept_pair_count, counts, bin_width, n):
        self.dataset_id = dataset_id
        self.source_concept_id = source_concept_id
        self.target_concept_id = target_concept_id
        self.source_concept_name = source_concept_name
        self.target_concept_name = target_concept_name
        self.source_concept_count = source_concept_count
        self.target_concept_count = target_concept_count
        self.concept_pair_count = concept_pair_count
        self.counts = np.array(counts)
        self.bin_width = bin_width
        self.n = n
        self.bins = self.n * 2 + 1  # number of bins

    def convert_bin_scheme(self, new_bin_width, new_n=None):
        # Make sure the new bin width is a multiple of the current bin width
        assert (new_bin_width % self.bin_width == 0)

        bin_ratio = int(new_bin_width / self.bin_width)

        if new_n is None:
            new_n = int(np.ceil(float(self.n) / bin_ratio))

        if new_bin_width == self.bin_width and self.n == new_n:
            # No change in structure, just change from list to ndarray
            new_counts = self.counts.copy()
        else:
            new_bins = new_n * 2 + 1
            new_counts = np.zeros(new_bins, dtype=np.uint32)

            # Make a new copy so we don't mess up the original
            cnts = np.array(self.counts)

            # No grouping for 0-day co-occurrence
            new_counts[new_n] = cnts[self.n]

            # If the binning stretches "beyond" the original counts array, pad the original counts array
            reach = bin_ratio * new_n
            if reach > self.n:
                pad = np.zeros(reach - self.n, dtype=np.uint32)
                cnts = np.concatenate((pad, cnts, pad))
            center = int(np.floor(len(cnts) / 2))

            # Fill in the positive deltas
            upper = center + reach + 1
            new_counts[(new_n + 1):new_bins] = cnts[(center + 1):upper].reshape(bin_ratio, new_n, order='F').sum(
                axis=0)

            # Fill in the negative deltas
            lower = center - reach
            new_counts[0:new_n] = cnts[lower:center].reshape(bin_ratio, new_n, order='F').sum(axis=0)

            # Add the leftover bins
            if reach < self.n:
                new_counts[new_bins - 1] += cnts[upper:]
                new_counts[0] += cnts[:lower]

        return DeltaCounts(self.dataset_id, self.source_concept_id, self.target_concept_id, self.source_concept_name,
                           self.target_concept_name, self.source_concept_count, self.target_concept_count,
                           self.concept_pair_count, new_counts, new_bin_width, new_n)

    def reverse(self):
        """ Creates a new DeltaCounts object with the source and target concepts reversed

        Returns
        -------
        DeltaCounts
        """
        return DeltaCounts(self.dataset_id, self.target_concept_id, self.source_concept_id, self.target_concept_name,
                           self.source_concept_name, self.target_concept_count, self.source_concept_count,
                           self.concept_pair_count, np.flip(self.counts.copy()), self.bin_width, self.n)

    def convert_to_dict_results(self):
        """ Creates a dict results representation of this object for JSON returns

        Returns
        -------
        dict representation
        """
        # Make sure all counts are represented as ints (as opposed to numpy types) for JSON serialization
        return {
            'dataset_id': self.dataset_id,
            'source_concept_id': self.source_concept_id,
            'source_concept_name': self.source_concept_name,
            'source_concept_count': int(self.source_concept_count),
            'target_concept_id': self.target_concept_id,
            'target_concept_name': self.target_concept_name,
            'target_concept_count': int(self.target_concept_count),
            'concept_pair_count': int(self.concept_pair_count),
            'bin_width': int(self.bin_width),
            'n': int(self.n),
            'counts': [int(x) for x in self.counts],
            'confidence_interval': [(int(x[0]), int(x[1])) for x in self.confidence_intervals()]
        }

    def confidence_intervals(self, alpha=0.99):
        """ Returns confidence intevals of counts

        Parameters
        ----------
        alpha

        Returns
        -------
        List of tuples with confidence intervals
        """
        return [poisson.interval(alpha, x) for x in self.counts]


def _estimate_suppressed_percent(counts, total_count, suppression_estimate=SUPPRESSION_ESTIMATE):
    """ Estimates the percentage of the total count that was suppressed

    Parameters
    ----------
    counts: counts that can be converted np.ndarray
    suppression_estimate: estimated count for suppressed counts

    Returns
    -------
    Percent in decimals (e.g., 0.05 for 5%)
    """
    np_counts = np.array(counts)
    suppressed_bin_mask = np_counts == SUPPRESSION_MARKER
    suppressed_count = np.sum(suppressed_bin_mask) * suppression_estimate
    np_counts[suppressed_bin_mask] = suppression_estimate
    return suppressed_count / total_count


def jaccard_similarity(d1, d2):
    """ Calculate Jaccard similarity between two distributions

    Parameters
    ----------
    d1: np.ndarray distribution 1
    d2: np.ndarray distribution 2

    Returns
    -------
    Jaccard similarity index
    """
    return np.sum(np.minimum(d1, d2)) / np.sum(np.maximum(d1, d2))


def query_concept_age_counts(dataset_id, concept_id):
    cur = sql_connection().cursor()

    # Get the concept-age counts binning scheme
    sql = '''SELECT *
        FROM cohd.concept_age_schemes
        WHERE dataset_id = %s AND concept_id = %s;'''
    params = [dataset_id, concept_id]
    cur.execute(sql, params)
    binning_scheme = cur.fetchall()

    if len(binning_scheme) != 1:
        # No binning scheme found, meaning no concept-age distributions found
        cads = []
    else:
        binning_scheme = binning_scheme[0]

        # Retrieve the concept-age distributions along with some basic info about the concept
        sql = '''SELECT count
            FROM cohd.concept_age_counts             
            WHERE dataset_id = %s AND concept_id = %s
            ORDER BY bin ASC;'''
        params = [dataset_id, concept_id]
        cur.execute(sql, params)

        # Create a list for the concept-age distribution
        counts = [x['count'] for x in cur.fetchall()]

        # Retrieve the concept name as well
        concept_def = omop_concept_definition(concept_id)

        # Get the single concept count
        concept_counts = query_count([concept_id], dataset_id=dataset_id)

        if len(counts) > 0 and concept_id in concept_counts:
            concept_count = concept_counts[concept_id]['concept_count']
            concept_name = ''
            if concept_def is not None:
                concept_name = concept_def['concept_name']

            cad = AgeCounts(dataset_id, concept_id, concept_name, concept_count, counts, binning_scheme['bin_width'])
            cads = [cad]
        else:
            cads = []

    return cads


def query_delta_counts(dataset_id, concept_pairs):
    """ Retrieves deltas for each pair of concepts

    Parameters
    ----------
    dataset_id (int) - data set ID
    concept_pairs list(tuples: (int, int)) - list of OMOMP concept ID pairs (source_concept, target_concept)

    Returns
    -------
    list of DeltaCounts objects the same lenth as concept_pairs. List entry will be None if delta counts not found
    """
    def _add_delta_count():
        """ Inner function to help create a DeltaCounter while reading the SQL results

        Returns
        -------
        Nothing
        """
        # Only create DeltaCounts if we can find the single concept counts, concept pair count, and binning scheme
        if (current_concept_id_1 > 0) and (current_concept_id_2 > 0) and \
                current_concept_id_1 in concept_counts and current_concept_id_2 in concept_counts and \
                current_pair in concept_pairs_counts and current_pair in binning_scheme_rows:

            concept_1_name = ''
            if current_concept_id_1 in concept_defs:
                concept_1_name = concept_defs[current_concept_id_1]['concept_name']

            concept_2_name = ''
            if current_concept_id_2 in concept_defs:
                concept_2_name = concept_defs[current_concept_id_2]['concept_name']

            source_concept_count = concept_counts[current_concept_id_1]['concept_count']
            target_concept_count = concept_counts[current_concept_id_2]['concept_count']
            concept_pair_count = concept_pairs_counts[current_pair]['results'][0]['concept_count']
            binning_scheme = binning_scheme_rows[current_pair]

            dc = DeltaCounts(dataset_id, current_concept_id_1, current_concept_id_2, concept_1_name, concept_2_name,
                             source_concept_count, target_concept_count, concept_pair_count, current_counts,
                             binning_scheme['bin_width'], binning_scheme['n'])
            deltas_dict[current_pair] = dc

    cur = sql_connection().cursor()

    # Database always stores the deltas with the smaller concept ID as concept_id_1
    database_pairs = list()
    for pair in concept_pairs:
        if not (isinstance(pair, tuple) and len(pair) == 2 and isinstance(pair[0], int) and isinstance(pair[1], int)):
            # Pair is not in the right format. Skip it
            continue

        # Keep track of pairs of concepts in the order stored in the database
        source_concept_id, target_concept_id = pair
        reverse = target_concept_id < source_concept_id
        if reverse:
            database_pair = (target_concept_id, source_concept_id)
        else:
            database_pair = pair
        database_pairs.append(database_pair)

    if len(database_pairs) == 0:
        # No pairs in the expected format. Return a list of None
        return [None] * len(concept_pairs)

    # Get the concept-pair delta binning scheme
    sql = '''SELECT *
                FROM cohd.delta_schemes
                WHERE dataset_id = %s AND ({concept_pairs_filter});'''
    concept_pairs_filter = ' OR '.join(['(concept_id_1 = %s AND concept_id_2 = %s)' for _ in range(len(database_pairs))])
    sql = sql.format(concept_pairs_filter=concept_pairs_filter)
    params = [dataset_id]
    for pair in database_pairs:
        params += [pair[0], pair[1]]

    cur.execute(sql, params)

    binning_scheme_rows = dict()
    for r in cur.fetchall():
        binning_scheme_rows[(r['concept_id_1'], r['concept_id_2'])] = r

    # If no binning schemes were found, that means no deltas will be found for the requested pair(s). Return empty
    if len(binning_scheme_rows) == 0:
        return [None for _ in concept_pairs]

    # Get rid of any database pairs that were not found in the binning schemes since they shouldn't be found in deltas
    # Also keep track of unique concepts to retrieve their concept definitions
    unique_concept_ids = set()
    for i, database_pair in reversed(list(enumerate(database_pairs))):
        if database_pair not in binning_scheme_rows:
            # No binning scheme found, meaning no concept-age distributions will (should) be found
            del database_pairs[i]
        else:
            # Keep track of unique concepts to retrieve their definitions later
            unique_concept_ids.add(database_pair[0])
            unique_concept_ids.add(database_pair[1])

    # Retrieve the concept definitions
    concept_defs = omop_concept_definitions(unique_concept_ids)

    # Retrieve the single concept counts
    concept_counts = query_count(unique_concept_ids, dataset_id=dataset_id)

    # Retrieve the concept pair counts
    concept_pairs_counts = dict()
    for database_pair in database_pairs:
        concept_pairs_counts[database_pair] = query_concept_pair_count(database_pair[0], database_pair[1], dataset_id)

    # Retrieve the concept-age distributions along with some basic info about the concept
    sql = '''SELECT concept_id_1, concept_id_2, count
                FROM cohd.delta_counts             
                WHERE dataset_id = %s AND ({concept_pairs_filter})
                ORDER BY concept_id_1 ASC, concept_id_2 ASC, bin ASC;'''
    concept_pairs_filter = ' OR '.join(['(concept_id_1 = %s AND concept_id_2 = %s)' for _ in range(len(database_pairs))])
    sql = sql.format(concept_pairs_filter=concept_pairs_filter)
    params = [dataset_id]
    for pair in database_pairs:
        params += [pair[0], pair[1]]
    cur.execute(sql, params)

    # Go through the returned rows and create DeltaCounts
    deltas_dict = dict()
    current_concept_id_1 = -1
    current_concept_id_2 = -1
    current_counts = list()

    for r in cur.fetchall():
        concept_id_1 = r['concept_id_1']
        concept_id_2 = r['concept_id_2']
        if (current_concept_id_1 != concept_id_1) or (current_concept_id_2 != concept_id_2):
            # New pair encountered, create the DeltaCount for the current pair
            # Only create it if we can find the single concept counts, the concept pair count, and the binning scheme
            if (current_concept_id_1 > 0) and (current_concept_id_2 > 0) and \
                    current_concept_id_1 in concept_counts and current_concept_id_2 in concept_counts and \
                    current_pair in concept_pairs_counts and current_pair in binning_scheme_rows:
                _add_delta_count()

            # Start tracking the new concept pair
            current_concept_id_1 = concept_id_1
            current_concept_id_2 = concept_id_2
            current_pair = (current_concept_id_1, current_concept_id_2)
            current_counts = list()

        current_counts.append(r['count'])

    # Finished reading table. Still need to add the last DeltaCounts
    _add_delta_count()

    # Now match up the deltas to the requested concept pairings
    deltas = list()
    for pair in concept_pairs:
        if not (isinstance(pair, tuple) and len(pair) == 2 and isinstance(pair[0], int) and isinstance(pair[1], int)):
            # Pair is not in the right format. Put in None to signify no DeltaCount could be retrieved
            deltas.append(None)
        else:
            if pair[0] < pair[1]:
                if pair in deltas_dict:
                    deltas.append(deltas_dict[pair])
                else:
                    deltas.append(None)
            else:
                reversed_pair = (pair[1], pair[0])
                if reversed_pair in deltas_dict:
                    deltas.append(deltas_dict[reversed_pair].reverse())
                else:
                    deltas.append(None)
    return deltas


def concepts_cooccur(concept_id_1, concept_id_2, dataset_id=DATASET_ID_DEFAULT_TEMPORAL, threshold=0.01,
                     concept_pair_count=None):
    """ Determines if a pair of concepts frequently co-occur on the same day.

    If the 0-day co-occurrence count is more than <threshold>% of the total co-occurrence, then the concept pair are
    considered to frequently co-occur.

    Parameters
    ----------
    concept_id_1 (int) - concept ID
    concept_id_2 (int) - concept ID
    dataset_id (int) - dataset ID (default: 4)
    threshold (float) - threshold percent (default: 0.01 (1%))

    Returns
    -------
    Boolean. True: frequent co-occurrence
    """
    # Database stores concept pairs with the smaller concept ID as concept_id_1
    if concept_id_2 < concept_id_1:
        temp = concept_id_1
        concept_id_1 = concept_id_2
        concept_id_2 = temp

    cur = sql_connection().cursor()

    if concept_pair_count is None:
        # Get the concept_pair_count
        json_result = query_concept_pair_count(concept_id_1, concept_id_2, dataset_id)
        if json_result is not None and 'results' in json_result and len(json_result['results']) == 1:
            concept_pair_count = json_result['results'][0]['concept_count']
        else:
            # Could not find a concept pair count. Assume unrelated
            return False

    # Get the 0-day co-occurrence
    sql = '''SELECT dc.count AS cooccurrence_count
        FROM delta_counts dc        
        WHERE dc.bin = 0 
            AND dc.concept_id_1 = %(concept_id_1)s  
            AND dc.concept_id_2 = %(concept_id_2)s 
            AND dc.dataset_id = %(dataset_id)s;'''
    params = {
        'concept_id_1': concept_id_1,
        'concept_id_2': concept_id_2,
        'dataset_id': dataset_id
    }
    cur.execute(sql, params)

    related = False
    r = cur.fetchone()
    if r is not None:
        cooccurrence_count = r['cooccurrence_count']
        if cooccurrence_count == SUPPRESSION_MARKER:
            # Be aggressive with detecting co-occurrence relatedness. Assume the upper end
            cooccurrence_count = 9

        if cooccurrence_count > (concept_pair_count * threshold):
            related = True

    return related


def query_similar_age_distributions(concept_id, dataset_id=DATASET_ID_DEFAULT_TEMPORAL, exclude_related=True,
                                    restrict_type=True, threshold=0.7, limit=20):
    """ Finds concepts with similar age distributions to the given concept_id

    Parameters
    ----------
    concept_id (int) - concept ID to find similar concepts for
    dataset_id (int) - data set ID
    exclude_related (bool) - True: exclude concepts that are considered to be related to the search concept
    restrict_type (bool) - True: restrict similar concepts to the same domain as the search concept
    threshold (float) - threshold for similarity score to be included
    limit (int) - maximum number of similar concepts to return

    Returns
    -------
    (coi_cacs, cacs_binned, similarities_binned)
    coi_cacs: dict[bin_width] -> concept age counts of the concept of interest
    cacs_binned: defaultdict[bin_width] -> list of concept age counts of similar concepts
    similarities_binned: defaultdict[bin_width] -> list of similarity scores of similar concepts
    """
    def _process_comparison_concept():
        """ This inner function helps add similar concepts to the lists

        Returns
        -------
        Nothing
        """
        if current_concept_id > 0 and len(current_counts) > 0 and current_concept_id != concept_id:
            # Check for suppressed bins. If the estimated suppressed count > 5% of the estimated total count,
            # then don't include this concept for analysis
            estimated_counts = np.array(current_counts)
            suppressed_bin_mask = estimated_counts == SUPPRESSION_MARKER
            suppressed_count = np.sum(suppressed_bin_mask) * SUPPRESSION_ESTIMATE
            estimated_counts[suppressed_bin_mask] = SUPPRESSION_ESTIMATE

            if suppressed_count <= (current_concept_count * 0.05):
                # Create an AgeCount with the estimated counts
                estimated_cac = AgeCounts(dataset_id, current_concept_id, current_concept_name,
                                          current_concept_count, estimated_counts, current_bin_width)

                # Convert the current CAC's bin_width to the larger bin_width
                if estimated_cac.bin_width < coi_cac.bin_width:
                    estimated_cac = estimated_cac.convert_bin_scheme(coi_cac.bin_width)

                # Calculate current concept's age distribution
                distribution = estimated_cac.counts / float(current_concept_count)

                # Calculate Jaccard similarity
                j = jaccard_similarity(distribution, coi_cads[estimated_cac.bin_width])
                if j >= threshold:
                    # Save the CAC of the current concept with the original (suppressed) counts
                    cac = AgeCounts(dataset_id, current_concept_id, current_concept_name,
                                    current_concept_count, current_counts, current_bin_width)
                    if cac.bin_width < coi_cac.bin_width:
                        cac = cac.convert_bin_scheme(coi_cac.bin_width)
                    cacs_binned[estimated_cac.bin_width].append(cac)
                    similarities_binned[estimated_cac.bin_width].append(j)

    # Get the AgeCounts for the concept of interest (COI)
    coi_cac = query_concept_age_counts(dataset_id, concept_id)
    if len(coi_cac) != 1:
        # Didn't find AgeCounts for this concept
        return dict(), dict(), dict()
    coi_cac = coi_cac[0]

    # Convert the COI's CAD to larger bins for comparison against other concepts with larger binning schemes
    # Also, replace the suppressed counts with approximated values, and convert the counts to a distribution
    coi_cads = dict()
    coi_cacs = dict()
    bin_widths = [1, 2, 4, 8, 16, 32]
    for bin_width in bin_widths:
        if bin_width >= coi_cac.bin_width:
            # Concept-Age Counts
            cac = coi_cac.convert_bin_scheme(bin_width)
            coi_cacs[bin_width] = cac

            # Concept-Age Distributions
            downconverted = cac.convert_bin_scheme(bin_width).counts
            downconverted[downconverted == SUPPRESSION_MARKER] = SUPPRESSION_ESTIMATE
            coi_cads[bin_width] = downconverted / float(coi_cac.concept_count)

    # Get the domain_id and concept_class_id if we're restricting by type
    if restrict_type:
        concept_def = omop_concept_definition(concept_id)
        domain = concept_def['domain_id']

        if domain == 'Drug' and concept_def['concept_class_id'] == 'Ingredient':
            concept_class = 'Ingredient'
        else:
            concept_class = None
    else:
        domain = None
        concept_class = None

    cur = sql_connection().cursor()
    sql = '''SELECT
                cac.concept_id, cac.count,                
                cas.bin_width,
                cc.concept_count,
                c.concept_name
            FROM cohd.concept_age_counts cac
            JOIN cohd.concept_age_schemes cas ON cac.concept_id = cas.concept_id AND cac.dataset_id = cas.dataset_id 
            JOIN cohd.concept_counts cc ON cac.concept_id = cc.concept_id AND cac.dataset_id = cc.dataset_id
            JOIN cohd.concept c ON cac.concept_id = c.concept_id
            WHERE cac.dataset_id = %(dataset_id)s                
                {domain_filter}
                {class_filter}
            ORDER BY cac.concept_id ASC, cac.bin ASC;'''
    params = {
        'dataset_id': dataset_id,
        'concept_id': concept_id,
    }

    # Filter concepts by domain
    if domain is not None and domain != ['']:
        # Restrict the concepts by domain
        domain_filter = 'AND c.domain_id = %(domain_id)s'
        params['domain_id'] = domain
    else:
        # Unrestricted domain
        domain_filter = ''

    # Filter concepts by concept_class
    if concept_class is not None and concept_class != ['']:
        # Restrict the concepts by domain
        class_filter = 'AND c.concept_class_id = %(concept_class_id)s'
        params['concept_class_id'] = concept_class
    else:
        # Unrestricted domain
        class_filter = ''

    sql = sql.format(domain_filter=domain_filter, class_filter=class_filter)
    cur.execute(sql, params)

    cacs_binned = defaultdict(list)
    current_concept_id = -1
    current_counts = list()
    similarities_binned = defaultdict(list)

    for r in cur.fetchall():
        if r['concept_id'] != current_concept_id:
            # This row starts a new concept. Add the current concept to the lists
            _process_comparison_concept()

            # Start tracking a new concept
            current_concept_id = r['concept_id']
            current_counts = list()
            current_concept_name = r['concept_name']
            current_concept_count = r['concept_count']
            current_bin_width = r['bin_width']

        # Build a list of counts for this comparison concept
        current_counts.append(r['count'])

    # Finished reading the table, still need to process the current concept
    _process_comparison_concept()

    # Sort the concepts in descending order of similarity for results at each bin width and keep a limited number
    for bin_width in bin_widths:
        if bin_width not in similarities_binned:
            continue

        # Check lists are same size
        sms = similarities_binned[bin_width]
        cacs = cacs_binned[bin_width]
        assert len(sms) == len(cacs)

        # Get sort order by descending similarity metric
        sort_ix = np.flipud(np.argsort(sms))

        # Sort the similarity and cac lists
        similarities_binned[bin_width] = [sms[x] for x in sort_ix]
        cacs_binned[bin_width] = [cacs[x] for x in sort_ix]

    if exclude_related:
        # Go through the lists again and remove any concepts that are related
        for bin_width in bin_widths:
            if bin_width not in cacs_binned:
                continue

            unrelated_cacs = list()
            unrelated_similarities = list()
            cac_list = cacs_binned[bin_width]
            similarity_list = similarities_binned[bin_width]

            for i, cac in enumerate(cac_list):
                # First check the ln_ratio of the concepts
                concept_pair_count = None
                assoc_results = query_association('obsExpRatio', concept_id, cac.concept_id, dataset_id)
                if assoc_results is not None and 'results' in assoc_results and len(assoc_results['results']) == 1:
                    assoc_result = assoc_results['results'][0]
                    ln_ratio = assoc_result['ln_ratio']
                    if ln_ratio > 2.0:
                        # This pair is related. Onto the next one
                        continue

                    # Grab the concept pair count for use in the co-occurrence check
                    concept_pair_count = assoc_result['observed_count']

                    # Next, check the co-occurrence
                    related = concepts_cooccur(concept_id, cac.concept_id, dataset_id,
                                               concept_pair_count=concept_pair_count, threshold=0.05)
                    if not related:
                        unrelated_cacs.append(cac)
                        unrelated_similarities.append(similarity_list[i])

                        # Keep a limited number of results per bin
                        if len(unrelated_cacs) >= limit:
                            break

            cacs_binned[bin_width] = unrelated_cacs
            similarities_binned[bin_width] = unrelated_similarities
    else:
        # Keep a limited number of results per bin
        for bin_width in bin_widths:
            if bin_width not in similarities_binned:
                continue

            l = min(limit, len(similarities_binned[bin_width]))
            similarities_binned[bin_width] = similarities_binned[bin_width][:l]
            cacs_binned[bin_width] = cacs_binned[bin_width][:l]

    return coi_cacs, cacs_binned, similarities_binned


def bootstrap_delta_distribution(deltas, mode='relative_source', iterations=10000):
    """ Estimates the distributions of the deltas by bootstrap
    Deltas will be sampled with replacement. Poisson randomization will be called on each count.

    Parameters
    ----------
    deltas: List of DeltaCounts
    mode: 'counts', 'relative_source', or 'relative_target'
    iterations: number of iterations to simulate

    Returns
    -------
    numpy ndarray shape (5, bins). Rows correspond to percentiles: 2.5, 25, 50, 75, 97.5. Columns correspond to bins.
    """
    if deltas is None or len(deltas) == 0:
        return None

    mode = mode.strip().lower()

    n_deltas = len(deltas)
    simulated_frequencies = list()
    for i in range(iterations):
        randi = random.randint(0, n_deltas - 1)
        sim = np.random.poisson(deltas[randi].counts)
        if mode == 'relative_source':
            sim = sim / float(deltas[randi].source_concept_count)
        elif mode == 'relative_target':
            sim = sim / float(deltas[randi].target_concept_count)
        simulated_frequencies.append(sim)

    simulated_frequencies = np.array(simulated_frequencies)
    return np.percentile(simulated_frequencies, q=[2.5, 25, 50, 75, 97.5], axis=0)


def query_source_to_target(dataset_id, source_concept_id, target_concept_id, exclude_related=False):
    """ Analyzes the temporal relationship between source_concept_id and target_concept_id

    Parameters
    ----------
    dataset_id (int) - dataset ID
    source_concept_id (int) - OMOP concept ID of the source concept (effector)
    target_concept_id (int) - OMOP concept ID of the target concept (effected)
    exclude_related (bool) - True to exclude concepts "related" to the concepts of interest from the comparison concepts

    Returns
    -------
    Analysis results, including comparison delta distributions
    """
    # Get the deltas between the source and target concepts
    delta_primary = query_delta_counts(dataset_id, [(source_concept_id, target_concept_id)])
    if delta_primary[0] is None:
        # No delta found for this concept pair
        return dict()
    delta_primary = delta_primary[0]

    # Convert the delta to larger bin widths for comparison
    delta_primary_downconverted = dict()
    settings = [(1, 13), (2, 6), (4, 3), (8, 2), (16, 1)]
    for bin_width, n in settings:
        if bin_width == delta_primary.bin_width:
            delta_primary_downconverted[bin_width] = delta_primary
        elif bin_width > delta_primary.bin_width:
            delta_primary_downconverted[bin_width] = delta_primary.convert_bin_scheme(bin_width, n)

    # Create a results structure with the concept-age counts and the deltas, grouped by the delta's bin width
    source_results_binned = dict()
    target_results_binned = dict()
    combined_results_binned = dict()
    for bin_width in [1, 2, 4, 8, 16]:
        if bin_width < delta_primary.bin_width:
            continue

        source_results_binned[bin_width] = {
            'bin_width': bin_width,
            'deltas': list(),
            'cad_similarities': list(),
            'distribution': None,
            'significance': None
        }

        target_results_binned[bin_width] = {
            'bin_width': bin_width,
            'deltas': list(),
            'cad_similarities': list(),
            'distribution': None,
            'significance': None
        }

        combined_results_binned[bin_width] = {
            'bin_width': bin_width,
            'distribution': None,
            'significance': None
        }

    # Get concepts similar to the source concept for comparison
    _, concepts_similar_to_source, similarity_to_source = query_similar_age_distributions(
        source_concept_id, dataset_id, exclude_related=exclude_related, restrict_type=True, threshold=0.7, limit=100)

    # Get concepts similar to the target concept for comparison
    _, concepts_similar_to_target, similarity_to_target = query_similar_age_distributions(
        target_concept_id, dataset_id, exclude_related=exclude_related, restrict_type=True, threshold=0.7, limit=100)

    # Build a list of concept pairs between [concepts similar to source] -> target_concept_id
    similar_source_pairs = []
    similarity_source_list = []
    for bin_width, cacs in list(concepts_similar_to_source.items()):
        similar_source_pairs += [(x.concept_id, target_concept_id) for x in cacs]
        similarity_source_list += similarity_to_source[bin_width]

    # Get delta comparisons with similar source concepts
    deltas_source = query_delta_counts(dataset_id, similar_source_pairs)

    # Group the comparison deltas by bin_width
    for sim, delta in zip(similarity_source_list, deltas_source):
        if delta is None:
            continue

        # Add the delta unaltered if its bin_width is at least as large as the delta_primary's bin_width
        if delta.bin_width >= delta_primary.bin_width:
            source_results_binned[delta.bin_width]['cad_similarities'].append(sim)
            source_results_binned[delta.bin_width]['deltas'].append(delta)

        # Also convert the delta to larger bin_widths to add them to the comparison in the larger bin_width groups
        for bin_width, n in settings:
            if bin_width > delta.bin_width and bin_width >= delta_primary.bin_width:
                delta_downconverted = delta.convert_bin_scheme(bin_width, n)
                source_results_binned[bin_width]['cad_similarities'].append(sim)
                source_results_binned[bin_width]['deltas'].append(delta_downconverted)

    # Build a list of concept pairs between source_concept_id -> [concepts similar to target]
    similar_target_pairs = []
    similarity_target_list = []
    for bin_width, cacs in list(concepts_similar_to_target.items()):
        similar_target_pairs += [(source_concept_id, x.concept_id) for x in cacs]
        similarity_target_list += similarity_to_target[bin_width]

    # Get delta comparisons with similar target concepts
    deltas_target = query_delta_counts(dataset_id, similar_target_pairs)

    # Group the comparison deltas by bin_width
    for sim, delta in zip(similarity_target_list, deltas_target):
        # Check that the delta bin_width isn't a higher resolution than the bin width captured by the delta_primary
        if delta is None:
            continue

        # Add the delta unaltered if its bin_width is at least as large as the delta_primary's bin_width
        if delta.bin_width >= delta_primary.bin_width:
            target_results_binned[delta.bin_width]['cad_similarities'].append(sim)
            target_results_binned[delta.bin_width]['deltas'].append(delta)

        # Also convert the delta to larger bin_widths to add them to the comparison in the larger bin_width groups
        for bin_width, n in settings:
            if bin_width > delta.bin_width and bin_width >= delta_primary.bin_width:
                delta_downconverted = delta.convert_bin_scheme(bin_width, n)
                target_results_binned[bin_width]['cad_similarities'].append(sim)
                target_results_binned[bin_width]['deltas'].append(delta_downconverted)

    # Run simulations on the source comparisons to generate a distribution
    for bin_width, srb in list(source_results_binned.items()):
        srb_deltas = srb['deltas']
        if len(srb_deltas) < 5:
            # Don't estimate the distribution for bins with too few comparison deltas
            continue

        # Estimate distributions of the comparison concepts
        dist = bootstrap_delta_distribution(srb_deltas, mode='relative_source', iterations=1000)
        srb['distribution'] = dist.tolist()

        # Compare the distributions to the confidence interval of the primary delta
        dp = delta_primary_downconverted[bin_width]
        cis = np.array(dp.confidence_intervals()).T / float(dp.source_concept_count)
        # note: need to convert numpy.bool to normal bool for json
        sig = [bool((cis[0, i] > dist[4, i]) or (cis[1, i] < dist[0, i])) for i in range(cis.shape[1])]
        srb['significance'] = sig

    # Run simulations on the target comparisons to generate a distribution
    for bin_width, trb in list(target_results_binned.items()):
        trb_deltas = trb['deltas']
        if len(trb_deltas) < 5:
            # Don't estimate the distribution for bins with too few comparison deltas
            continue

        # Estimate distributions of the comparison concepts
        dist = bootstrap_delta_distribution(trb_deltas, mode='relative_source', iterations=1000)
        trb['distribution'] = dist.tolist()

        # Compare the distributions to the confidence interval of the primary delta
        dp = delta_primary_downconverted[bin_width]
        cis = np.array(dp.confidence_intervals()).T / float(dp.source_concept_count)
        # note: need to convert numpy.bool to normal bool for json
        sig = [bool((cis[0, i] > dist[4, i]) or (cis[1, i] < dist[0, i])) for i in range(cis.shape[1])]
        trb['significance'] = sig

    # Run simulations on the combined source and target comparisons to generate a distribution
    for bin_width in [1, 2, 4, 8, 16]:
        combined_deltas = []
        if bin_width in source_results_binned:
            combined_deltas += source_results_binned[bin_width]['deltas']
        if bin_width in target_results_binned:
            combined_deltas += target_results_binned[bin_width]['deltas']

        if len(combined_deltas) < 5:
            # Don't estimate the distribution for bins with too few comparison deltas
            continue

        crb = combined_results_binned[bin_width]

        # Estimate distributions of the comparison concepts
        dist = bootstrap_delta_distribution(combined_deltas, mode='relative_source', iterations=1000)
        crb['distribution'] = dist.tolist()

        # Compare the distributions to the confidence interval of the primary delta
        dp = delta_primary_downconverted[bin_width]
        cis = np.array(dp.confidence_intervals()).T / float(dp.source_concept_count)
        # note: need to convert numpy.bool to normal bool for json
        sig = [bool((cis[0, i] > dist[4, i]) or (cis[1, i] < dist[0, i])) for i in range(cis.shape[1])]
        crb['significance'] = sig

    # Create the result structure
    queried_pair = [{'bin_width': bw,
                     'delta': d.convert_to_dict_results()}
                    for bw, d in list(delta_primary_downconverted.items())]
    source_comparison = [{'bin_width': bw,
                          'deltas': [d.convert_to_dict_results() for d in x['deltas']],
                          'cad_similarities': [float(s) for s in x['cad_similarities']],
                          'distribution': x['distribution'],
                          'significance': x['significance']}
                         for bw, x in list(source_results_binned.items())]
    target_comparison = [{'bin_width': bw,
                          'deltas': [d.convert_to_dict_results() for d in x['deltas']],
                          'cad_similarities': [float(s) for s in x['cad_similarities']],
                          'distribution': x['distribution'],
                          'significance': x['significance']}
                         for bw, x in list(target_results_binned.items())]
    combined_comparison = [{'bin_width': bw,
                            'distribution': x['distribution'],
                            'significance': x['significance']}
                           for bw, x in list(combined_results_binned.items())]

    # Create the json_return structure
    json_return = [{
        'queried_pair': queried_pair,
        'source_comparison': source_comparison,
        'target_comparison': target_comparison,
        'combined_comparison': combined_comparison
    }]

    return json_return


def query_cohd_temporal(service, method, args):
    # This function only handles temporal service.
    assert service == 'temporal'

    # Connect to MYSQL database
    conn = sql_connection()
    cur = conn.cursor()

    json_return = []

    query = args.get('q')

    print("Service: {s}; Method: {m}, Query: {q}".format(s=service, m=method, q=query))

    # Retrieves concept-age distributions
    # e.g. /api/temporal/conceptAgeCounts?dataset_id=4&concept_id=313217
    if method == 'conceptAgeCounts':
        dataset_id = get_arg_dataset_id(args, DATASET_ID_DEFAULT_TEMPORAL)

        # Get concept_id parameters
        concept_id = get_arg_concept_id(args)
        if concept_id is None:
            return 'concept_id parameter is missing', 400

        cads = query_concept_age_counts(dataset_id, concept_id)
        json_return = [cad.convert_to_dict_results() for cad in cads]

    # Finds concepts with a similar concept-age distribution to the concept of interest
    # e.g. /api/temporal/conceptAgeCounts?dataset_id=4&concept_id=313217
    elif method == 'findSimilarAgeDistributions':
        # Get concept_id parameters
        concept_id = get_arg_concept_id(args)
        if concept_id is None:
            return 'concept_id parameter is missing', 400

        # Get optional params
        params = {}
        dataset_id = get_arg_dataset_id(args, DATASET_ID_DEFAULT_TEMPORAL)
        restrict_type = get_arg_boolean(args, 'restrict_type')
        if restrict_type is not None:
            params['restrict_type'] = restrict_type
        exclude_related = get_arg_boolean(args, 'exclude_related')
        if exclude_related is not None:
            params['exclude_related'] = exclude_related
        threshold = get_arg_float(args, 'threshold')
        if threshold is not None:
            params['threshold'] = threshold
        limit = get_arg_int(args, 'limit')
        if limit is not None:
            params['limit'] = limit

        coi_cacs, cacs, similarities = query_similar_age_distributions(concept_id, dataset_id, **params)
        assert len(cacs) == len(similarities)

        # Convert the cacs into result sets for each bin width
        json_return = []
        for bin_width in [1, 2, 4, 8, 16, 32]:
            if bin_width not in cacs:
                continue

            # Insert the concept of interest as the first concept in the array
            coi_cac = coi_cacs[bin_width]
            cac_result = coi_cac.convert_to_dict_results()
            cac_result['similarity'] = 1.0
            cac_results = [cac_result]

            sm = similarities[bin_width]
            for i, cac in enumerate(cacs[bin_width]):
                cac_result = cac.convert_to_dict_results()
                cac_result['similarity'] = float(sm[i])
                cac_results.append(cac_result)

            # Create a result set for the results in this bin width
            result_set = {
                'bin_width': bin_width,
                'concept_age_counts': cac_results
            }
            json_return.append(result_set)

    # Retrieves concept pair delta distributions
    # e.g. /api/temporal/conceptPairDeltaCounts?dataset_id=4&source_concept_id=312327&target_concept_id=313217
    elif method == 'conceptPairDeltaCounts':
        dataset_id = get_arg_dataset_id(args, DATASET_ID_DEFAULT_TEMPORAL)

        # Get concept_id parameters
        source_concept_id = get_arg_concept_id(args, 'source_concept_id')
        target_concept_id = get_arg_concept_id(args, 'target_concept_id')
        if source_concept_id is None:
            return 'source_concept_id parameter is missing', 400
        if target_concept_id is None:
            return 'target_concept_id parameter is missing', 400

        deltas = query_delta_counts(dataset_id, [(source_concept_id, target_concept_id)])
        json_return = [delta.convert_to_dict_results() for delta in deltas if delta is not None]

    # Returns ratio of observed to expected frequency between pairs of concepts
    # e.g. /api/temporal/sourceToTarget?dataset_id=4&source_concept_id=312327&target_concept_id=313217
    elif method == 'sourceToTarget':
        dataset_id = get_arg_dataset_id(args, DATASET_ID_DEFAULT_TEMPORAL)

        # Get concept_id parameters
        source_concept_id = get_arg_concept_id(args, 'source_concept_id')
        target_concept_id = get_arg_concept_id(args, 'target_concept_id')
        if source_concept_id is None:
            return 'source_concept_id parameter is missing', 400
        if target_concept_id is None:
            return 'target_concept_id parameter is missing', 400

        json_return = query_source_to_target(dataset_id, source_concept_id, target_concept_id)


    # print cur._executed
    # print(json_return)

    cur.close()
    conn.close()

    json_return = {"results": json_return}
    json_return = jsonify(json_return)

    return json_return