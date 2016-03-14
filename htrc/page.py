

import re

from itertools import combinations


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
        Generate a set of edge weight contributions.

        Yields: (token1, token2, count)
        """

        edges = {}

        for (t1, c1), (t2, c2) in combinations(self.token_counts, 2):

            pair = (t1, t2)
            count = c1 + c2

            # Bump the count, if the pair has been seen.
            if pair in edges:
                edges[pair] += count

            # Or, initialize the value.
            else:
                edges[pair] = count

        return edges
