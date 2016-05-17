

import numpy as np

from collections import OrderedDict
from scipy.signal import savgol_filter
from scipy.stats import linregress

from hol.count_wpm import CountWPM
from hol.anchored_count_wpm import AnchoredCountWPM


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

            s0_trimmed = OrderedDict([
                (year, s0[year])
                for year in s1.keys()
            ])

            vals0 = list(s0_trimmed.values())
            vals1 = list(s1.values())

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


    def lin_reg(self, token):

        """
        For a token, regress the ratio onto year.

        Args:
            token (str)

        Returns: LinearRegression
        """

        series = self.ratios[token]

        x = list(series.keys())
        y = list(series.values())

        return linregress(x, y)
