

import numpy as np

from collections import OrderedDict, Counter
from scipy.spatial import distance
from scipy.signal import savgol_filter

from hol import config, cached
from hol.models import AnchoredCount
from hol.utils import sort_dict


class YearSeries:


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

            # self.topns[year] = ranks
            self.topns[year] = mdw


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

        result = OrderedDict()

        for t in self.tokens():

            try:
                s = self.rank_series_smooth(t, *args, **kwargs)
                result[t] = _lambda(s)

            # Ignore series with N < savgol width.
            except TypeError:
                pass

        return sort_dict(result)


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

        Returns: OrderedDict {token: score, ...}
        """

        result = OrderedDict()

        for t in self.tokens(min_count):

            s = self.pdf(t, *args, **kwargs)

            result[t] = _lambda(s)

        return sort_dict(result)


    def pdfs_similar_to(self, token, min_count=20, *args, **kwargs):

        """
        Given a seed token, rank tokens by PDF similarity.

        Args:
            token (str)
            min_count (int)

        Returns: OrderedDict {token: score, ...}
        """

        source_pdf = self.pdf(token, *args, **kwargs)

        result = OrderedDict()

        for t in self.tokens(min_count):

            target_pdf = self.pdf(t, *args, **kwargs)

            result[t] = 1 - distance.braycurtis(
                list(source_pdf.values()),
                list(target_pdf.values()),
            )

        return sort_dict(result)
