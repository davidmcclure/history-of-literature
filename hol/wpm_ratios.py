

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
        wpm2 = AnchoredCountWPM(year1, year2)
