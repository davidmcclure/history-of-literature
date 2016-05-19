

import numpy as np

from collections import OrderedDict
from scipy.signal import savgol_filter
from statsmodels.nonparametric.kde import KDEUnivariate
from sklearn.covariance import EllipticEnvelope
from scipy import stats

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

            series = OrderedDict(zip(s1.keys(), r))

            if len(series) > 1:
                self.ratios[token] = series


    def clean_series(self, token, discard=5):

        """
        Remove outliers from the ratio series for a token.

        Args:
            discard (int): Drop the most outlying X% of the data.

        Returns: OrderedDict{year: wpm}
        """

        series = self.ratios[token]

        X = np.array(list(series.values()))[:, np.newaxis]

        env = EllipticEnvelope()
        env.fit(X)

        # Score each data point.
        y_pred = env.decision_function(X).ravel()

        # Get the discard threshold.
        threshold = stats.scoreatpercentile(y_pred, discard)

        return OrderedDict([
            (year, ratio)
            for (year, ratio), pred in zip(series.items(), y_pred)
            if pred > threshold
        ])


    def smooth_series(self, token, width=41, order=2, *args, **kwargs):

        """
        Smooth the ratio series for a word.

        Args:
            token (str)
            width (int)
            order (int)

        Returns: OrderedDict{year: wpm}
        """

        series = self.clean_series(token, *args, **kwargs)

        smooth = savgol_filter(list(series.values()), width, order)

        return OrderedDict(zip(series.keys(), smooth))


    def query_series(self, _lambda, *args, **kwargs):

        """
        Pass all ratio series through a scoring callback, sort descending.

        Args:
            _lambda (func)

        Returns: OrderedDict{token: score}
        """

        result = OrderedDict()

        for token, series in self.ratios.items():

            score = _lambda(series)

            if score is not None:
                result[token] = score

        return sort_dict(result)


    def pdf(self, token, years, bw=5, *args, **kwargs):

        """
        Estimate a density function from a token's ratio series.

        Args:
            token (str)
            years (iter)
            bw (int)

        Returns: OrderedDict {year: density}
        """

        series = self.clean_series(token, *args, **kwargs)

        # Use the ratio values as weights.
        weights = np.array(list(series.values()))

        # Fit the density estimate.
        density = KDEUnivariate(list(series.keys()))
        density.fit(fft=False, weights=weights, bw=bw)

        samples = OrderedDict()

        for year in years:
            samples[year] = density.evaluate(year)[0]

        return samples
