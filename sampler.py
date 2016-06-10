"""
Exports classes that are useful for sampling from the set of possible images.
"""

from collections import defaultdict as ddict
from conf import *
import statemon

mon = statemon.state
statemon.define("n_num_unsampled", int)
statemon.define("n_samples_remaining", int)

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
        no longer sampled in order. If All items have been sampled `limit` or
        more times, OrderedSampler sets the `lim_reached` boolean. Once this
        has been set, subsequent calls to `sample` will sample uniformly.

        NOTE:
            This has been updated, to account for the fact that
            append() and pop() are O(1) methods.

            This results in a speedup of approximately 180x for very
            large samples.
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
        self._cur_bin = None
        self._cur_bin_key = None

    def _get_n_samples_remaining(self):
        """
        Returns the number of samples that remain to be taken.

        :return: An integer, the number of samples that remain to be taken.
        """
        tot = 0
        for bin in self._bins:
            if bin < self._lim:
                tot += (self._lim - bin) * len(self._bins[bin])
        return tot

    def _samp(self, n):
        """
        Hidden sampling method. Guaranteed to return at most n items.
        If it does not, you may call it multiple times.
        """
        if self._cur_bin is None:
            if not self._bins.keys():
                return None
            self._cur_bin_key = min(self._bins.keys())
            if self._cur_bin_key >= self._lim:
                self.lim_reached = True
            self._cur_bin = self._bins[self._cur_bin_key]
            np.random.shuffle(self._cur_bin)
        if self._cur_bin_key >= self._lim:
            return np.random.choice(self._cur_bin, n)
        c_samp = []
        to_update = []
        while len(c_samp) < n:
            if not self._cur_bin:
                self._cur_bin = None
                self._cur_bin_key = None
                break
            obt_val = self._cur_bin.pop()
            c_samp.append(obt_val)
            to_update.append([self._cur_bin_key, obt_val])
        self._update(to_update)
        if self._cur_bin is not None:
            try:
                mon.n_num_unsampled = len(self._bins[0])
            except:
                pass
            try:
                mon.n_samples_remaining = self._get_n_samples_remaining()
            except:
                pass
        return c_samp

    def _update(self, to_update):
        """
        Updates the sampling count given a list of the items sampled on the
        previous sampling run.

        :param sampled: A list of tuples: [(bin, item), ...] where `bin` was
        the bin from which `item` was sampled.
        :return: None
        """
        for key, val in to_update:
            self._bins[min(key+self._inc, self._lim)].append(val)
        minbin = min(self._bins.keys())
        while not self._bins[minbin]:
            self._bins.pop(minbin)
            minbin = min(self._bins.keys())

    def sample(self, N):
        """
        Takes a sample of size `N`.

        :param N: The number of samples to obtain.
        :return: A sample, as a list, of size `N`.
        """
        assert N <= self._N, 'Sample is larger than population!'
        cur_samp = []
        while len(cur_samp) < N:
            cur_samp.extend(self._samp(N - len(cur_samp)))
        np.random.shuffle(cur_samp)
        self.samps += len(cur_samp)
        return cur_samp
