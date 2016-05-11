

import numpy as np

from collections import OrderedDict, Counter
from scipy.signal import savgol_filter

from hol import config
from hol.models import AnchoredCount
from hol import cached


class TopnSeries:


    def __init__(self, years, depth=1000, level1=None, level2=None):

        """
        Cache topn lists for a range of years.

        Args:
            years (iter)
            depth (int)
            level1 (int)
            level2 (int)
        """

        self.years = years
        self.depth = depth

        self.topns = OrderedDict()

        for year in self.years:

            mdw = cached.mdw(
                year1=year,
                year2=year,
                level1=level1,
                level2=level2,
            )

            topn = list(mdw.items())[:depth]

            ranks = OrderedDict()

            for i, (token, _) in enumerate(topn):
                ranks[token] = depth-i

            self.topns[year] = ranks


    def tokens(self, min_count=0):

        """
        Get a set of all unique tokens that appear in any year.

        Returns: set
        """

        counts = Counter()

        for year, series in self.topns.items():
            for token in series.keys():
                counts[token] += 1

        return [
            t for t, c in counts.items()
            if c >= min_count
        ]


    def rank_series(self, token):

        """
        Get the rank series for a token.

        Returns: OrderedDict {year: rank, ...}
        """

        series = OrderedDict()

        for year, topn in self.topns.items():

            rank = topn.get(token)

            if rank:
                series[year] = rank

        return series


    def rank_series_smooth(self, token, width=21, order=2):

        """
        Smooth the rank series for a token.

        Args:
            token (str)
            width (int)
            order (int)

        Returns: OrderedDict {year: rank, ...}
        """

        series = self.rank_series(token)

        ranks = list(series.values())

        smooth = savgol_filter(ranks, width, order)

        return OrderedDict(zip(series.keys(), smooth))


    def sort_smoothed_rank_series(self, _lambda, *args, **kwargs):

        """
        Compute smoothed series for all tokens, sort on a callback.

        Args:
            _lambda (function)

        Returns: OrderedDict {token: (series, score), ...}
        """

        series = []
        for t in self.tokens():

            try:
                s = self.rank_series_smooth(t, *args, **kwargs)
                series.append((t, s, _lambda(s)))

            # Ignore series with N < savgol width.
            except TypeError:
                pass

        # Sort descending.
        tsv = sorted(series, key=lambda x: x[2], reverse=True)

        result = OrderedDict()

        # Index by token.
        for (t, s, v) in tsv:
            result[t] = (s, v)

        return result


    def pdf(self, token, *args, **kwargs):

        """
        Estimate a density function from a token's rank series.

        Args:
            token (str)

        Returns: OrderedDict {year: density, ...}
        """

        series = self.rank_series(token)

        return cached.pdf(series, self.years, *args, **kwargs)


    def sort_pdfs(self, _lambda, min_count=20, *args, **kwargs):

        """
        Compute PDFs for all tokens, sort on a callback.

        Args:
            _lambda (function)
            min_count (int)

        Returns: OrderedDict {token: (series, score), ...}
        """

        series = []
        for t in self.tokens(min_count):

            # Get PDF samples.
            s = self.pdf(t, *args, **kwargs)

            # Apply the sorting function.
            series.append((t, s, _lambda(s)))

        # Sort descending.
        tsv = sorted(series, key=lambda x: x[2], reverse=True)

        result = OrderedDict()

        # Index by token.
        for (t, s, v) in tsv:
            result[t] = (s, v)

        return result
