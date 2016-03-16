

import re
import networkx as nx

from itertools import combinations
from collections import Counter
from wordfreq import word_frequency

from .term_graph import TermGraph


class Page:


    def __init__(self, json):

        """
        Wrap an individual page.

        Args:
            json (dict)
        """

        self.json = json


    def token_counts(self, min_freq=1e-05):

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

            # Get modern frequency.
            freq = word_frequency(token, 'en')

            # Ignore irregular / infrequent words.
            if letters.match(token) and freq > min_freq:
                counts[token] += sum(pc.values())

        return counts


    def graph(self, *args, **kwargs):

        """
        Assemble the page-level co-occurrence graph.

        Returns: TermGraph
        """

        graph = TermGraph()

        counts = self.token_counts(*args, **kwargs)

        for (t1, c1), (t2, c2) in combinations(counts.items(), 2):
            graph.add_edge(t1, t2, weight=min(c1, c2))

        return graph
