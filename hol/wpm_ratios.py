

import numpy as np

from collections import OrderedDict
from scipy.signal import savgol_filter
from scipy.stats import linregress
from statsmodels.nonparametric.kde import KDEUnivariate

from hol.count_wpm import CountWPM
from hol.anchored_count_wpm import AnchoredCountWPM
from hol.utils import sort_dict


class WPMRatios:


    def __init__(self, year1, year2):

        """
        Compute ratios between the baseline and filtered counts.

        Args:
            year1 (int)
            year2 (int)
        """

        wpm0 = CountWPM(year1, year2)
        wpm1 = AnchoredCountWPM(year1, year2)

        self.ratios = {}

        for token in wpm1.tokens():

            s0 = wpm0.series(token)
            s1 = wpm1.series(token)

            # Prune out years missing in the anchored series.
            s0_trimmed = OrderedDict([
                (year, s0[year])
                for year in s1.keys()
            ])

            vals0 = list(s0_trimmed.values())
            vals1 = list(s1.values())

            # Get ratio between anchored series and baseline.
            r = np.array(vals1) / np.array(vals0)

            self.ratios[token] = OrderedDict(zip(s1.keys(), r))


    def series_smooth(self, token, width=41, order=2):

        """
        Smooth the ratio series for a word.

        Args:
            token (str)
            width (int)
            order (int)

        Returns: OrderedDict{year: wpm}
        """

        series = self.ratios[token]

        smooth = savgol_filter(list(series.values()), width, order)

        return OrderedDict(zip(series.keys(), smooth))


    def query_series(self, _lambda, min_count=2):

        """
        Pass all ratio series through a scoring callback, sort descending.

        Args:
            _lambda (func)
            min_count (int)

        Returns: OrderedDict{token: score}
        """

        result = OrderedDict()

        for token, series in self.ratios.items():

            if len(series) < min_count:
                continue

            score = _lambda(series)

            if score is not None:
                result[token] = score

        return sort_dict(result)


    def pdf(self, token, bw=5):

        """
        Estimate a density function from a token's ratio series.

        Args:
            token (str)

        Returns: OrderedDict {year: density}
        """

        series = self.ratios[token]

        weights = np.array(list(series.values()))

        density = KDEUnivariate(list(series.keys()))
        density.fit(fft=False, weights=weights, bw=bw)

        samples = OrderedDict()

        for year in series.keys():
            samples[year] = density.evaluate(year)[0]

        return samples
