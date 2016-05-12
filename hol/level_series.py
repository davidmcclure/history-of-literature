

from distance import jaccard
from collections import OrderedDict, Counter

from hol import cached
from hol.utils import sort_dict


class LevelSeries:


    def __init__(self, levels, year1=None, year2=None, depth=1000):

        """
        Cache topn lists for a range of years.

        Args:
            levels (iter)
            year1 (int)
            year2 (int)
            depth (int)
        """

        self.levels = levels
        self.depth = depth

        self.topns = OrderedDict()

        for level in self.levels:

            mdw = cached.mdw(
                level1=level,
                level2=level,
                year1=year1,
                year2=year2,
            )

            topn = list(mdw.items())[:depth]

            ranks = OrderedDict()

            for i, (token, _) in enumerate(topn):
                ranks[token] = depth-i

            self.topns[level] = ranks


    def tokens(self, min_count=0):

        """
        Get a set of all unique tokens that appear in any year.

        Returns: set
        """

        counts = Counter()

        for level, topn in self.topns.items():
            for token in topn.keys():
                counts[token] += 1

        return [
            t for t, c in counts.items()
            if c >= min_count
        ]


    def jaccard_distances_from_1(self):

        """
        For each level 2 -> N, compute the Jaccard distance between the first
        level and the Nth level.

        Returns: dict
        """

        topn_1 = list(self.topns[1].keys())

        ds = {}
        for level, topn in self.topns.items():

            # Get topn for level N.
            topn_n = list(self.topns[level].keys())

            # Jaccard distance from 1.
            ds[level] = jaccard(topn_1, topn_n)

        return ds


    def rank_series(self, token):

        """
        Get the rank series for a token.

        Returns: OrderedDict {level: rank, ...}
        """

        series = OrderedDict()

        for level, topn in self.topns.items():

            rank = topn.get(token)

            if rank:
                series[level] = rank

        return series


    def sort_rank_series(self, _lambda, min_count=10):

        """
        Compute ranks series for all tokens, sort on a callback.

        Args:
            _lambda (function)

        Returns: OrderedDict {token: score, ...}
        """

        result = OrderedDict()

        for t in self.tokens(min_count):

            s = self.rank_series(t)

            result[t] = _lambda(s)

        return sort_dict(result)
