

import re
import networkx as nx

from itertools import combinations
from collections import Counter
from wordfreq import word_frequency


class Page:


    def __init__(self, json):

        """
        Wrap an individual page.

        Args:
            json (dict)
        """

        self.json = json


    def token_counts(self):

        """
        Count the total occurrences of each unique token.

        Returns: Counter
        """

        # Filter out non-letters.
        letters = re.compile('^[a-z]+$')

        counts = Counter()
        for token, pc in self.json['body']['tokenPosCount'].items():

            token = token.lower()

            # Get modern frequency.
            freq = word_frequency(token, 'en')

            # Ignore words that appear < 10 per 1M.
            if letters.match(token) and freq > 1e-05:
                counts[token] += sum(pc.values())

        return counts


    def graph(self):

        """
        Assemble the page-level co-occurrence graph.

        Returns: nx.Graph
        """

        graph = nx.Graph()

        counts = self.token_counts()

        for (t1, c1), (t2, c2) in combinations(counts.items(), 2):
            graph.add_edge(t1, t2, weight=min(c1, c2))

        return graph
