

import numpy as np

from collections import OrderedDict
from scipy.signal import savgol_filter

from hol.models import Score


class TopnSeries:


    def __init__(self, years, n=1000):

        """
        Cache topn lists for a range of years.

        Args:
            years (iter)
        """

        self.topns = OrderedDict()

        for year in years:

            topn = Score.topn_by_year(year, n)

            ranks = OrderedDict()

            for i, (token, _) in enumerate(topn.items()):
                ranks[token] = n-i

            self.topns[year] = ranks


    def tokens(self):

        """
        Get a set of all unique tokens that appear in any year.

        Returns: set
        """

        tokens = set()

        for year, series in self.topns.items():
            tokens.update(series.keys())

        return tokens


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


    def rank_series_smooth(self, token, width=11, order=2):

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


    def query(self, score, *args, **kwargs):

        """
        Compute series for all tokens, sort on a callback.

        Args:
            score (function)

        Returns: OrderedDict {token: series, ...}
        """

        series = []
        for t in self.tokens():

            try:
                s = self.rank_series_smooth(t, *args, **kwargs)
                series.append((t, s, score(s)))

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
