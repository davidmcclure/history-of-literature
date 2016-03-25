

import re
import networkx as nx

from itertools import combinations
from collections import Counter
from wordfreq import get_frequency_dict
from stop_words import get_stop_words


# Cache word frequencies.
FREQS = get_frequency_dict('en')


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


    def total_counts(self, min_freq=1e-05):

        """
        Count the total occurrences of each unique token.

        Args:
            min_freq (float): Ignore words below this frequency.

        Returns: Counter
        """

        # Filter out non-letters.
        letters = re.compile('^[a-z]+$')

        # Get a stopword list.
        stop_words = get_stop_words('en')

        counts = Counter()
        for token, pc in self.json['body']['tokenPosCount'].items():

            token = token.lower()

            # Ignore irregular tokens.
            if not letters.match(token):
                continue

            # Ignore stopwords.
            if token in stop_words:
                continue

            # Ignore infrequent words.
            if FREQS.get(token, 0) < min_freq:
                continue

            counts[token] += sum(pc.values())

        return counts
