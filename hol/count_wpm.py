

from collections import OrderedDict

from hol.models import Count


class CountWPM:


    def __init__(self, years):

        """
        Cache token -> WPM maps for all pages.

        Args:
            years (iter)
        """

        self.years = years

        self.wpms = OrderedDict()

        # TODO: pass year1 / year2?
        totals = Count.year_count_series(years)

        for year in self.years:

            counts = Count.token_counts_by_year(year, year)

            wpm = {}
            for token, count in counts.items():
                wpm[token] = (1e6*count) / totals[year]

            self.wpms[year] = wpm
