

import numpy as np
import matplotlib.pyplot as plt

from multiprocessing import Pool
from collections import Counter, defaultdict

from htrc import config
from htrc.corpus import Corpus
from htrc.volume import Volume



class CountData:


    def __init__(self, counts=None):

        """
        Initialize the Redis connection.

        Args:
            counts (Counter)
        """

        if not counts:
            counts = defaultdict(Counter)

        self.counts = counts


    def index(self, num_procs=8):

        """
        Index per-year counts for all tokens.

        Args:
            num_procs (int)
        """

        corpus = Corpus.from_env()

        with Pool(num_procs) as pool:

            # Spool a job for each volume.
            jobs = pool.imap_unordered(
                get_vol_counts,
                corpus.paths(),
            )

            for i, (year, counts) in enumerate(jobs):

                # Update the counter.
                for token, count in counts.items():
                    self.counts[year][token] += count

                print(i)


    def years(self):

        """
        Get a sorted list of all years represented in the index.

        Returns: list
        """

        return sorted(self.counts.keys())


    def baseline_count_for_year(self, year):

        """
        Get the total number of tokens for a year.

        Args:
            year (int)
        """

        return sum(self.counts[year].values())


    def token_time_series(self, token):

        """
        Get a list of per-year counts for a token.

        Args:
            token (str)

        Returns: list
        """

        counts = []

        for year in self.years():
            count = self.counts[year][token]
            counts.append((year, count))

        return counts


    def baseline_time_series(self):

        """
        Get a list of the total wordcounts per year.

        Returns: list
        """

        counts = []

        for year in self.years():
            count = self.baseline_count_for_year(year)
            counts.append((year, count))

        return counts


    def plot_token_time_series(self, token, y1=1700, y2=1920):

        """
        Plot a token time series.

        Args:
            token (str)
            y1 (int)
            y2 (int)
        """

        data = self.token_time_series(token)

        plt.xlim(y1, y2)
        plt.plot(*zip(*data))
        plt.show()


    def plot_baseline_time_series(self, y1=1700, y2=1920):

        """
        Plot the baseline time series.

        Args:
            y1 (int)
            y2 (int)
        """

        data = self.baseline_time_series()

        plt.xlim(y1, y2)
        plt.plot(*zip(*data))
        plt.show()



def get_vol_counts(path):

    """
    Extract filtered token counts from a volume.

    Args:
        path (str): A HTRC volume path.

    Returns: dict
    """

    vol = Volume(path)

    return (vol.year, vol.total_counts())
