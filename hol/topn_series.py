

from collections import OrderedDict

from hol.models import Score


class TopnSeries:


    def __init__(self, years, n=500):

        """
        Cache topn lists for a range of years.

        Args:
            years (iter)
        """

        self.topns = OrderedDict()

        for year in years:
            self.topns[year] = Score.topn_by_year(year, n)


    def tokens(self):

        """
        Get a set of all unique tokens that appear in any year.

        Returns: set
        """

        tokens = set()

        for year, series in self.topns.items():
            tokens.update(series.keys())

        return tokens
