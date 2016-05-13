

from collections import OrderedDict

from hol.models import AnchoredCount
from hol.wpm import WPM


class AnchoredCountWPM(WPM):


    def __init__(self, year1, year2):

        """
        Cache token -> WPM maps for anchored pages.

        Args:
            year1 (int)
            year2 (int)
        """

        self.wpms = OrderedDict()

        totals = AnchoredCount.year_count_series(year1, year2)

        for year in totals.keys():

            counts = AnchoredCount.token_counts_by_year_and_level(year, year)

            wpm = {}
            for token, count in counts.items():
                wpm[token] = (1e6*count) / totals[year]

            self.wpms[year] = wpm
