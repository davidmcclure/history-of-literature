

import re

from itertools import combinations
from collections import Counter


class Page:


    def __init__(self, json):

        """
        Wrap an individual page.

        Args:
            json (dict)
        """

        self.json = json


    @property
    def token_counts(self):

        """
        Generate a set of flat (token, count) tuples.

        Yields: (token, count)
        """

        # Filter out non-letters.
        letters = re.compile('^[a-z]+$')

        for token, pc in self.json['body']['tokenPosCount'].items():

            # Downcase token.
            token = token.lower()

            if letters.match(token):
                yield (token, sum(pc.values()))


    @property
    def edges(self):

        """
        Build a page-level edge list.

        Returns: EdgeList
        """

        edges = Counter()

        for (t1, c1), (t2, c2) in combinations(self.token_counts, 2):
            edges[(t1, t2)] += min(c1, c2)

        return edges
