

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
        Count the total occurrences of each unique token.

        Returns: Counter
        """

        # Filter out non-letters.
        letters = re.compile('^[a-z]+$')

        counts = Counter()
        for token, pc in self.json['body']['tokenPosCount'].items():

            # Downcase token.
            token = token.lower()

            if letters.match(token):
                counts[token] += sum(pc.values())

        return counts
