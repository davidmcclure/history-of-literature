

import numpy as np

from collections import OrderedDict
from functools import lru_cache
from sqlalchemy.sql import func

from hol.models import AnchoredCount
from hol.utils import sort_dict

from .count import CountQueries
from .query_set import QuerySet


class AnchoredCountQueries(QuerySet):


    @lru_cache()
    def year_count(self, year):

        """
        How many tokens appeared on pages with the anchor token in year X?

        Returns: int
        """

        res = (
            self.session
            .query(func.sum(AnchoredCount.count))
            .filter(AnchoredCount.year==year)
        )

        return res.scalar()


    @lru_cache()
    def token_counts_by_year(self, year):

        """
        Get the counts for all tokens that appeared on pages with the anchor
        token in year X.

        Args:
            year (int)

        Returns: [(token, count), ...]
        """

        res = (
            self.session
            .query(AnchoredCount.token, func.sum(AnchoredCount.count))
            .filter(AnchoredCount.year==year)
            .group_by(AnchoredCount.token)
            .order_by(AnchoredCount.token.asc())
        )

        return res.all()


    @lru_cache()
    def dll_by_year(self, year):

        """
        Get Dunning log-likelihood scores for all tokens in a year.

        Args:
            token (str)
            year (int)

        Returns: float
        """

        cq = CountQueries()

        counts = dict(cq.token_counts_by_year(year))
        anchored_counts = dict(self.token_counts_by_year(year))

        c = self.year_count(year)
        d = cq.year_count(year)

        dll = OrderedDict()
        for token, count in anchored_counts.items():

            a = count
            b = counts[token]

            e1 = c*(a+b) / (c+d)
            e2 = d*(a+b) / (c+d)

            g2 = 2*((a*np.log(a/e1)) + (b*np.log(b/e2)))

            dll[token] = g2

        return sort_dict(dll)
