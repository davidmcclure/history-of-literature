

from collections import OrderedDict

from hol.models import Count
from hol.wpm import WPM


class CountWPM(WPM):


    def __init__(self, year1, year2):

        """
        Cache token -> WPM maps for all pages.

        Args:
            year1 (int)
            year2 (int)
        """

        self.wpms = OrderedDict()

        totals = Count.year_count_series(year1, year2)

        for year in totals.keys():

            counts = Count.token_counts_by_year(year, year)

            wpm = {}
            for token, count in counts.items():
                wpm[token] = (1e6*count) / totals[year]

            self.wpms[year] = wpm
