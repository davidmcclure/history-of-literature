

import os
import matplotlib.pyplot as plt
import numpy as np

from functools import lru_cache
from scipy import stats

from htrc.token_graph import TokenGraph
from htrc.utils import sort_dict_by_key


class YearGraphs:


    def __init__(self, path, source):

        """
        Canonicalize the root path.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)

        self.source = source


    @lru_cache()
    def years(self):

        """
        Generate years in the corpus

        Yields: int
        """

        years = []

        for entry in os.scandir(self.path):
            year = os.path.basename(entry.path)
            years.append(int(year))

        return sorted(years)


    @lru_cache()
    def graph_by_year(self, year):

        """
        Hydrate the graph for a year.

        Args:
            year (int)

        Returns: TokenGraph
        """

        path = os.path.join(self.path, str(year))

        return TokenGraph.from_shelf(path)


    @lru_cache()
    def all_tokens(self):

        """
        Get a set of all tokens in the graphs.

        Returns: set
        """

        tokens = set()

        for entry in os.scandir(self.path):

            graph = TokenGraph.from_shelf(entry.path)
            tokens.update(graph.nodes())

        return tokens


    @lru_cache()
    def baseline_time_series(self):

        """
        Get the total per-year time series for all token weights.

        Returns: list of (year, value)
        """

        data = []

        for year in self.years():

            graph = self.graph_by_year(year)

            total = 0
            for t1, t2, d in graph.edges(data=True):
                total += d['weight']

            data.append((year, total))

        return np.array(data)


    @lru_cache()
    def token_time_series(self, token):

        """
        Get the total per-year time series for an individual token.

        Returns: list of (year, value)
        """

        data = []

        for year in self.years():

            graph = self.graph_by_year(year)

            value = 0
            if graph.has_edge(self.source, token):
                value = graph[self.source][token]['weight']

            data.append((year, value))

        return np.array(data)


    @lru_cache()
    def pearson_correlation(self, token):

        """
        Compute the Pearson correlation coefficient between the time series
        data for an individual token and the all-tokens baseline.

        Args:
            token (str)

        Returns: float
        """

        ts1 = self.baseline_time_series()
        ts2 = self.token_time_series(token)

        return stats.pearsonr(ts1[:,1], ts2[:,1])


    def plot_baseline_time_series(self):

        """
        Plot the baseline time series.
        """

        data = self.baseline_time_series()

        plt.plot(*zip(*data))
        plt.show()


    def plot_token_time_series(self, token):

        """
        Plot the time series for a token.
        """

        data = self.token_time_series(token)

        plt.plot(*zip(*data))
        plt.show()


    def plot_scaled_correlation(self, token):

        """
        Plot a scaled token series against the baseline.
        """

        baseline = self.baseline_time_series()
        token = self.token_time_series(token)

        factor = (
            np.sum(baseline[:,1]) /
            np.sum(token[:,1])
        )

        bx = baseline[:,0]
        by = baseline[:,1]

        tx = token[:,0]
        ty = token[:,1] * factor

        plt.plot(bx, by, tx, ty)
        plt.show()


    def plot_token_baseline_ratio(self, token):

        """
        Plot the ratio of a token series against the baseline.
        """

        baseline = self.baseline_time_series()
        token = self.token_time_series(token)

        factor = (
            np.sum(baseline[:,1]) /
            np.sum(token[:,1])
        )

        ratios = []
        for t, b in zip(token[:,1], baseline[:,1]):
            value = ((t*factor) / b) if b > 0 else 0
            ratios.append(value)

        plt.plot(self.years(), ratios)
        plt.show()
