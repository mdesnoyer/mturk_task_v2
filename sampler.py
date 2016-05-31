"""
Exports classes that are useful for sampling from the set of possible images.
"""

from collections import defaultdict as ddict
import numpy as np
import sys
from conf import *
import statemon

mon = statemon.state
statemon.define("n_num_unsampled", int)

class OrderedSampler():
    def __init__(self, items, limit=SAMPLING_LIMIT, inc=1):
        """
        Creates an ordered sampler, which samples in a semi-random way such
        that the sampling order is fixed with respect to the number of times
        an item has already been sampled but random with respect to the items
        themselves.

        For instance, if an item has been sampled m times, it will not be
        sampled again until all other items have been sampled m or more times.

        The advantage is that this can be initialized with known counts.

        :param items: A dictionary indicating, for every item, the number of
        times it has already been sampled.
        :param limit: Items that have been sampled `limit` or more times are
        no longer considered. If All items have been sampled `limit` or more
        times, OrderedSampler sets the `lim_reached` boolean. Once this has
        been set, subsequent calls to `sample` will sample uniformly.
        """
        self._N = len(items)
        self._bins = ddict(lambda: [])
        self._lim = limit
        self._inc = inc
        self.samps = 0
        self.lim_reached = False
        for k, v in items.iteritems():
            self._bins[min(v, self._lim)].append(k)
        for v in self._bins.values():
            np.random.shuffle(v)

    def min_sample(self):
        """
        Finds the minimum sample number.

        :return: The number of times the least sampled item has been sampled,
        as an int.
        """
        return min(self._bins)

    def max_sample(self):
        """
        Finds the maximum sample number.

        :return: The number of times the most sampled item has been sampled,
        as an int.
        """
        return max(self._bins)

    def _update(self, sampled):
        """
        Updates the sampling count given a list of the items sampled on the
        previous sampling run.

        :param sampled: A list of tuples: [(item, bin), ...] where `bin` was
        the bin from which `item` was sampled.
        :return: None
        """
        for item, cbin in sampled:
            if cbin == self._lim:
                continue  # don't bother updating.
            ritem = self._bins[cbin].pop(0)
            assert ritem == item, '%i != %i ahh' % (item, ritem)
            if not len(self._bins[cbin]):
                self._bins.pop(cbin, None)
        for item, cbin in sampled:
            if cbin == self._lim:
                continue  # don't bother updating.
            self._bins[min(cbin+self._inc, self._lim)].insert(
                np.random.randint(len(self._bins[cbin])+1), item)
        try:
            mon.n_num_unsampled = len(self._bins[0])
        except:
            pass

    def sample(self, N):
        """
        Takes a sample of size `N`.

        :param N: The number of samples to obtain.
        :return: A sample, as a list, of size `N`.
        """
        assert N <= self._N, 'Sample is larger than population!'
        cur_samp = []
        sampled = []
        bins = self._bins.keys()
        bins.sort()
        if bins[0] == self._lim:
            self.lim_reached = True
        if bins[0] == self._lim:
            # then just return a random sample
            return np.random.choice(self._bins[bins[0]], N)
        for cbin in bins:
            if len(self._bins[cbin]) > (N - len(cur_samp)):
                chosen = self._bins[cbin][:(N-len(cur_samp))]
            else:
                chosen = self._bins[cbin]
            for item in chosen:
                cur_samp.append(item)
                sampled.append((item, cbin))
            if len(cur_samp) == N:
                break
            if len(cur_samp) > N:
                raise Exception('Sample too full, d\'oh!')
        self._update(sampled)
        np.random.shuffle(cur_samp)
        self.samps += len(cur_samp)
        return cur_samp
