

from scipy.signal import savgol_filter

from collections import OrderedDict


class WPM:


    def tokens(self):

        """
        Get a set of all tokens.

        Returns: set
        """

        tokens = set()

        for year, wpm in self.wpms.items():
            tokens.update(wpm.keys())

        return tokens


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
