

import numpy as np

from functools import lru_cache
from collections import defaultdict, Counter
from sqlalchemy.sql import func

from hol import config
from .query_set import QuerySet
from hol.models import Count


class CountQueries(QuerySet):


    @lru_cache()
    def tokens(self):

        """
        Get an ordered list of all tokens.

        Returns: list<str>
        """

        res = (
            self.session
            .query(Count.token)
            .distinct()
            .order_by(Count.token.asc())
        )

        return [r[0] for r in res]


    @lru_cache()
    def total_token_count(self):

        """
        Get the total number of observed tokens.

        Returns: int
        """

        res = (
            self.session
            .query(func.sum(Count.count))
        )

        return res.scalar()


    @lru_cache()
    def baseline_series(self, years):

        """
        Get per-year counts for all tokens.

        Args:
            years (iter)

        Returns: [(year, count), ...]
        """

        res = (
            self.session
            .query(Count.year, func.sum(Count.count))
            .filter(Count.year.in_(years))
            .group_by(Count.year)
            .order_by(Count.year)
        )

        return res.all()


    @lru_cache()
    def token_series(self, token, years):

        """
        Get per-year counts for an individual token.

        Args:
            token (str)
            years (iter)

        Returns: [(year, count), ...]
        """

        res = (
            self.session
            .query(Count.year, func.sum(Count.count))
            .filter(Count.token==token, Count.year.in_(years))
            .group_by(Count.year)
            .order_by(Count.year)
        )

        return res.all()


    @lru_cache()
    def token_wpm_series(self, token, years):

        """
        Get a WMP series for a token.

        Args:
            token (str)
            years (iter)

        Returns: [(year, wpm), ...]
        """

        baseline = dict(self.baseline_series(years))

        ts = self.token_series(token, years)

        series = []
        for year, count in ts:
            wpm = (1e6 * count) / baseline[year]
            series.append((year, wpm))

        return series


    @lru_cache()
    def token_wpm_series_smooth(self, token, years, width=10):

        """
        Smooth the WMP series for a token.

        Args:
            token (str)
            years (iter)
            width (int)

        Returns: [(year, wpm), ...]
        """

        series = self.token_wpm_series(token, years)

        years, wpms = zip(*series)

        smooth = np.convolve(
            wpms,
            np.ones(width) / width,
            mode='same',
        )

        return zip(years, smooth)
