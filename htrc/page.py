

import re
import networkx as nx

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
    def token_count(self):

        """
        Get the total number of "body" tokens.

        Returns: int
        """

        return self.json['body']['tokenCount']


    def cleaned_token_counts(self):

        """
        Count the total occurrences of each unique token.

        Args:
            min_freq (float): Ignore words below this frequency.

        Returns: Counter
        """

        # Filter out non-letters.
        letters = re.compile('^[a-z]+$')

        counts = Counter()
        for token, pc in self.json['body']['tokenPosCount'].items():

            token = token.lower()

            # Ignore irregular tokens.
            if not letters.match(token):
                continue

            counts[token] += sum(pc.values())

        return counts
