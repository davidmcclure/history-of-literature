

from collections import OrderedDict


class WPM:


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
