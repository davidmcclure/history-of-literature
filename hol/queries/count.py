

import numpy as np

from functools import lru_cache
from collections import defaultdict, Counter
from sqlalchemy.sql import func

from hol import config
from hol.models import Count


class CountQueries:


    def __init__(self):

        """
        Initialize a session.
        """

        self.session = config.Session()


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
            year (iter)

        Returns: list[tuple[year, count]]
        """

        res = (
            self.session
            .query(Count.year, func.sum(Count.count))
            .filter(Count.year.in_(years))
            .group_by(Count.year)
            .order_by(Count.year)
        )

        return np.array(res.all())


    @lru_cache()
    def token_series(self, token, years):

        """
        Get per-year counts for an individual token.

        Args:
            token (str)
            year (iter)

        Returns: list[tuple[year, count]]
        """

        res = (
            self.session
            .query(Count.year, func.sum(Count.count))
            .filter(Count.token==token, Count.year.in_(years))
            .group_by(Count.year)
            .order_by(Count.year)
        )

        return np.array(res.all())


    @lru_cache()
    def token_wpm_series(self, token, years):

        """
        Get a WMP time series for a token.

        Args:
            token (str)
            year (iter)

        Returns: list[tuple[year, count]]
        """

        baseline = self.baseline_series(years)

        token = self.token_series(token, years)

        series = []
        for bc, tc, year in zip(baseline[:,1], token[:,1], years):
            series.append((year, (1e6 * tc) / bc))

        return series
