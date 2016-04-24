

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
