

import numpy as np

from scipy import stats
from collections import defaultdict, Counter

from htrc import config
from htrc.models import Count
from htrc.utils import window


class CountQueries:


    def __init__(self):

        """
        Hydrate the count map.
        """

        self.session = config.Session()

        self.data = defaultdict(Counter)

        for c in self.session.query(Count).yield_per(1000):
            self.data[c.year][c.token] = c.count


    def years(self):

        """
        Get an ordered list of years.

        Returns: list<int>
        """

        return sorted(self.data.keys())


    def tokens(self):

        """
        Get an ordered list of all tokens.

        Returns: list<int>
        """

        tokens = set()

        for year, counts in self.data.items():
            tokens.update(counts.keys())

        return sorted(tokens)


    def year_count(self, year):

        """
        Get the total token count for a year.

        Args:
            year (int)

        Returns: int
        """

        return sum(self.data[year].values())


    def token_year_count(self, token, year):

        """
        How many times did token X appear in year Y?

        Args:
            token (str)
            year (int)

        Returns: int
        """

        return self.data[year][token]


    def token_year_wpm(self, token, year):

        """
        How many times did token X appear per million words in year Y?

        Args:
            token (str)
            year (int)

        Returns: float
        """

        year_count = self.year_count(year)

        if year_count > 0:

            # Normalize per-M ratio.
            token_count = self.token_year_count(token, year)
            return (1e6 * token_count) / year_count

        else: return 0


    def token_year_wpm_series(self, token, years):

        """
        Get a WPM time series for a word.

        Args:
            token (str)
            years (iter)

        Returns: list
        """

        series = []
        for year in years:
            series.append(self.token_year_wpm(token, year))

        return series


    def token_year_wpm_series_smooth(self, token, years, width=5):

        """
        Get a WPM time series for a word.

        Args:
            token (str)
            years (iter)
            width (int)

        Returns: list
        """

        series = self.token_year_wpm_series(token, years)

        return np.convolve(
            series,
            np.ones(width) / width,
            mode='same',
        )


    def rolling_correlation(self,
        token1,
        token2,
        years,
        window_width=30,
        smooth_width=5,
    ):

        """
        Slide a window across a year range, compute the dyanamic correlation
        between two tokens.

        Args:
            token1 (str)
            token2 (str)
            years (iter)
            window_width (int)
            smooth_width (int)

        Returns: tuple (xs, ys)
        """

        t1_series = self.token_year_wpm_series_smooth(
            token1,
            years,
            smooth_width,
        )

        t2_series = self.token_year_wpm_series_smooth(
            token2,
            years,
            smooth_width,
        )

        windows = zip(
            window(t1_series, window_width),
            window(t2_series, window_width),
            window(years, window_width),
        )

        xs = []
        ys = []
        for t1, t2, y in windows:
            ys.append(stats.pearsonr(t1, t2)[0])
            xs.append((y[0]+y[-1])/2)

        return (xs, ys)
