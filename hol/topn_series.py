

import numpy as np

from collections import OrderedDict

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
            series[year] = topn.get(token, 0)

        return series


    def rank_series_smooth(self, token, width=10):

        """
        Smooth the rank series for a token.

        Args:
            token (str)
            years (iter)
            width (int)

        Returns: OrderedDict {year: wpm, ...}
        """

        series = self.rank_series(token)

        ranks = series.values()

        smooth = np.convolve(
            list(ranks),
            np.ones(width) / width,
        )

        return OrderedDict(zip(series.keys(), smooth))


    def sort_series(self, years, rank, width=10):

        """
        Compute series for all tokens, sort on a callback.

        Args:
            years (iter)
            rank (function)
            width (int)

        Returns: OrderedDict {token: series, ...}
        """

        series = []
        for t in self.tokens():
            s = self.rank_series_smooth(t, width)
            series.append((t, s, rank(s)))

        # Sort descending.
        tsv = sorted(series, key=lambda x: x[2], reverse=True)

        result = OrderedDict()

        # Index by token.
        for (t, s, v) in tsv:
            result[t] = (s, v)

        return result
