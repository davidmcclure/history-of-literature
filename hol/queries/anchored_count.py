

import numpy as np

from collections import OrderedDict
from functools import lru_cache
from sqlalchemy.sql import func

from hol.models import AnchoredCount
from hol.utils import sort_dict

from hol.queries.query_set import QuerySet
from hol.queries import CountQueries


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
