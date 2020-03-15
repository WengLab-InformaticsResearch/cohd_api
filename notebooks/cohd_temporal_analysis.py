import numpy as np
import matplotlib.pyplot as plt


class AgeCounts:
    def __init__(self, dataset_id, concept_id, concept_name, concept_count, counts, confidence, bin_width):
        self.dataset_id = dataset_id
        self.concept_id = concept_id
        self.concept_name = concept_name
        self.bin_width = bin_width
        self.bins = len(counts)
        self.counts = np.array(counts)
        self.concept_count = concept_count
        self.confidence = np.array(confidence)
    
    
    def x(self, incremental=False):
        if incremental:
            # Return [0, 1, 2, 3, ...] (good for x-values of bar plot)
            return np.arange(self.bins)
        else:
            # Return e.g. [0, 5, 10, 15, ...] indicating the start of each bin
            return np.arange(self.bins) * self.bin_width
    
    
    def x_labels(self):
        return [str(x) for x in self.x(incremental=True)*self.bin_width]
    
    
    def convert_bin_scheme(self, new_bin_width, new_bins=None):
        # Make sure the new bin width is a multiple of the current bin width
        assert (new_bin_width % self.bin_width == 0)
        
        bin_ratio = int(new_bin_width / self.bin_width)
                
        if new_bins is None:
            new_bins = int(np.ceil(float(self.bins) / bin_ratio))
                
        new_counts = np.zeros(new_bins)
        
        for i in range(new_bins - 1):
            new_counts[i] = np.sum(self.counts[(i*bin_ratio):((i+1)*bin_ratio)])
        new_counts[new_bins-1] = np.sum(self.counts[((new_bins-1)*bin_ratio):])      
        
        # Recalculate confidence intervals
        new_ci = np.array(poisson.interval(0.99, new_counts))        
        
        return AgeCounts(self.dataset_id, self.concept_id, self.concept_name, self.concept_count,
                        new_counts, new_ci, new_bin_width)
    
    def errors(self):
        # Gets lower and upper error bars
        return np.abs(self.confidence - self.counts)
    

class DeltaCounts:
    def __init__(self, dataset_id, source_concept_id, target_concept_id, source_concept_name, target_concept_name, 
                 source_concept_count, target_concept_count, concept_pair_count, counts, confidence, bin_width, n):
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
        self.confidence = np.array(confidence)

    
    def convert_bin_scheme(self, new_bin_width, new_n=None):
        # Make sure the new bin width is a multiple of the current bin width
        assert (new_bin_width % self.bin_width == 0)
        
        bin_ratio = int(new_bin_width / self.bin_width)
                
        if new_n is None:
            new_n = int(np.ceil(float(self.n) / bin_ratio))                            
        
        if new_bin_width == self.bin_width and self.n == new_n:
            # No change in structure, just change from list to ndarray
            new_counts = np.array(self.counts)
        else:
            new_bins = new_n * 2 + 1
            new_counts = np.zeros(new_bins)

            # Make a new copy so we don't mess up the original
            cnts = np.array(self.counts)
            
            # No grouping for 0-day co-occurrence
            new_counts[new_n] = cnts[self.n]
            
            # If the binning stretches "beyond" the original counts array, pad the original counts array
            reach = bin_ratio * new_n
            if reach > self.n:
                pad = np.zeros(reach - self.n)
                cnts = np.concatenate((pad, cnts, pad))
            center = int(np.floor(len(cnts) / 2))
            
            # Fill in the positive deltas            
            upper = center + reach + 1
            new_counts[(new_n + 1):new_bins] = cnts[(center + 1):upper].reshape(bin_ratio, new_n, order='F').sum(axis=0)
                        
            # Fill in the negative deltas
            lower = center - reach
            new_counts[0:new_n] = cnts[lower:center].reshape(bin_ratio, new_n, order='F').sum(axis=0)
            
            # Add the leftover bins
            if reach < self.n:
                new_counts[new_bins - 1] += cnts[upper:]
                new_counts[0] += cnts[:lower]    
                
        # Recalculate confidence intervals
        new_ci = np.array(poisson.interval(0.99, new_counts))
        
        return DeltaCounts(self.dataset_id, self.source_concept_id, self.target_concept_id, self.source_concept_name,
                           self.target_concept_name, self.source_concept_count, self.target_concept_count, 
                           self.concept_pair_count, new_counts, new_ci, new_bin_width, new_n)
    
    
    def bin_labels(self):
        labels = [''] * self.bins
        labels[self.n] = '0'
        
        if self.n > 0:
            labels[slice(self.n - 1, self.n + 2, 2)] = ['-1', '1']
            max_bin_label = f'{2 ** ((self.n-1) * self.bin_width)}+'
            labels[self.bins - 1] = max_bin_label
            labels[0] = '-' + max_bin_label                        
        
        for i in range(2, self.n):
            lower = 2 ** ((i - 1) * self.bin_width)
            upper = 2 ** (i * self.bin_width) - 1
            labels[self.n + i] = f'{lower} — {upper}'
            labels[self.n - i] = f'-{upper} — -{lower}'
            
        return labels
    
    
    def bin_labels_mixed(self):
        """ Compact labels with mixed units (easier to interpret) """
        labels = ['0d']
        
        for i in range(0, self.n):
            days = 2 ** (i * self.bin_width)
            if days >= 365:
                years = days / 365.25
                label = f'{years:0.1f}y'
            else:
                label = f'{days}d'
            
            labels.append(label)
            labels.insert(0, '-' + label)
            
        return labels
    
    
    def x(self):
        """ X-ticks ranging from -self.n:self.n (useful as x-axis values for plotting) """
        return np.arange(-self.n, self.n+1)
    
    
    def get(self, ix):
        """ Gets the counts using a relative index, where 0 is the 0-day bin, 1 is the 1-day bin, 
        -1 is the -1-day bin, etc. 
        
        Params
        ------
        indices: relative index or list of indices """        
        try:
            # Assume ix is an iterable and retrieve all requested indices
            if isinstance(ix, list):
                ix = np.array(ix)
            if not isinstance(ix, np.ndarray): 
                raise TypeError('ix expected to be list or numpy.ndarray')
                
            return self.counts[self.n + ix]
        except TypeError:
            # ix is not iterable, assume it's an int
            return self.counts[self.n + ix]
        
    def errors(self):
        # Gets lower and upper error bars
        return np.abs(self.confidence - self.counts)
    
    def reverse(self):
        # Creates a new DeltaCounts object with the source and targets reversed
        return DeltaCounts(self.dataset_id, self.target_concept_id, self.source_concept_id, self.target_concept_name, 
                           self.source_concept_name, self.target_concept_count, self.source_concept_count, 
                           self.concept_pair_count, np.flip(self.counts), np.flip(self.confidence), self.bin_width, self.n)
        
        
def plot_delta(delta, mode='count', alpha=1.0, show_error_bars=False, show_plot=True):
    """ Plots the delta counts from concept_source --> concept_target
    
    mode: 'count' (default), 'density', 'relative_source', or 'relative_target' """
        
    x = delta.x() 
    counts = delta.counts      
    errors = delta.errors()
    
    mode = mode.lower()
    if mode == 'density':
        # normalize the co-occurrence counts to a sum of 1.0 (i.e., probability distribution)
        counts = counts / delta.concept_pair_count
        errors = errors / delta.concept_pair_count
    elif mode == 'relative_source':
        # normalize by the total counts of the source concept
        counts = counts / delta.source_concept_count     
        errors = errors / delta.source_concept_count
    elif mode == 'relative_target':
        # normalize by the total counts of the target concept
        counts = counts / delta.target_concept_count
        errors = errors / delta.target_concept_count
        
    # Additional plot params
    params = dict()
    if show_error_bars:
        params['yerr'] = errors
        params['capsize'] = 4.0
                
    plt.bar(x, counts, alpha=alpha, **params)
    plt.xticks(x, delta.bin_labels_mixed(), rotation='vertical')
    plt.title(f'{delta.source_concept_name} -> {delta.target_concept_name}')
    plt.xlabel('Delta (days)')    
    plt.ylabel(mode)
    
    if show_plot:
        # Optionally not showing the plot now gives caller ability to edit plot properties
        plt.show()
    
    
def plot_age_counts(cads, normalize=True, alpha=None, show_error_bars=False, show_plot=True):    
    if not alpha:
        alpha = 1 if len(cads) == 1 else 0.5     
    
    labels = list()
    for cad in cads:
        h = cad.counts
        e = cad.errors()
        if normalize:
            h = h / (cad.concept_count * cad.bin_width)
            e = e / (cad.concept_count * cad.bin_width)
            
        # Additional plot params
        params = dict()
        if show_error_bars:
            params['yerr'] = e
            params['capsize'] = 2.0    
            
        plt.bar(x=cad.x(), height=h, width=cad.bin_width - 0.2, alpha=alpha, align='edge', **params)
        labels.append(cad.concept_name)
    
    plt.legend(labels, fontsize=12)
    plt.xlabel('Age (years)', fontsize=12)
    if normalize:
        plt.ylabel('Percent', fontsize=12)
    else:
        plt.ylabel('Count', fontsize=12)
    
    if show_plot:
        # Optionally not showing the plot now gives caller ability to edit plot properties
        plt.show()