

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

        # Just allow tokens with letters.
        letters = re.compile('^[a-zA-Z]+$')

        for token, pc in self.json['body']['tokenPosCount'].items():
            if letters.match(token):
                yield (token, sum(pc.values()))


    @property
    def edges(self):

        """
        Generate a set of edge weight contributions.

        Yields: (token1, token2, count)
        """

        for (t1, c1), (t2, c2) in combinations(self.token_counts, 2):
            yield ((t1, t2), c1+c2)
