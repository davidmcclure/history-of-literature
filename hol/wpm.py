

from scipy.signal import savgol_filter

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
