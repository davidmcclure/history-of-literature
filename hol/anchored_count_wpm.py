

from collections import OrderedDict

from hol.models import AnchoredCount


class AnchoredCountWPM:


    def __init__(self, years):

        """
        Cache token -> WPM maps for anchored pages.

        Args:
            years (iter)
        """

        self.years = years

        self.wpms = OrderedDict()

        # TODO: pass year1 / year2?
        totals = AnchoredCount.year_count_series(years)

        for year in self.years:

            counts = AnchoredCount.token_counts_by_year_and_level(year, year)

            wpm = {}
            for token, count in counts.items():
                wpm[token] = (1e6*count) / totals[year]

            self.wpms[year] = wpm
