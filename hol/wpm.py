

import numpy as np

from scipy.signal import savgol_filter
from sklearn.neighbors import KernelDensity

from collections import OrderedDict, Counter


class WPM:


    def tokens(self, min_count=0):

        """
        Get a set of all tokens.

        Returns: set
        """

        counts = Counter()

        for year, wpm in self.wpms.items():
            for token in wpm.keys():
                counts[token] += 1

        return [
            t for t, c in counts.items()
            if c >= min_count
        ]


    def series(self, token):

        """
        Pull a raw WPM series for a word.

        Args:
            token (str)

        Returns: OrderedDict{year: wpm}
        """

        series = OrderedDict()

        for year, wpm in self.wpms.items():

            val = wpm.get(token)

            if val:
                series[year] = val

        return series


    def series_smooth(self, token, width=21, order=2):

        """
        Smooth the series for a word.

        Args:
            token (str)
            width (int)
            order (int)

        Returns: OrderedDict{year: wpm}
        """

        series = self.series(token)

        smooth = savgol_filter(list(series.values()), width, order)

        return OrderedDict(zip(series.keys(), smooth))


    def pdf(self, token, years, bandwidth=5):

        """
        Estimate a density function from a token's rank series.

        Args:
            token (str)
            years (range)

        Returns: OrderedDict {year: density}
        """

        series = self.series(token)

        data = []
        for year, wpm in series.items():
            data += [year] * round(wpm)

        data = np.array(data)[:, np.newaxis]

        pdf = KernelDensity(bandwidth=bandwidth).fit(data)

        samples = OrderedDict()

        for year in years:
            samples[year] = np.exp(pdf.score(year))

        return samples
