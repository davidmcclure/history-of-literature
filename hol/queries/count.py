

from functools import lru_cache
from collections import defaultdict, Counter

from hol import config
from hol.models import Count


class CountQueries:


    def __init__(self):

        """
        Initialize a session.
        """

        self.session = config.Session()


    @lru_cache()
    def years(self):

        """
        Get an ordered list of years.

        Returns: list<int>
        """

        res = (
            self.session
            .query(Count.year)
            .distinct()
            .order_by(Count.year.asc())
        )

        return [r[0] for r in res]
