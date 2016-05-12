

from collections import OrderedDict

from hol import cached


class LevelSeries:


    def __init__(self, levels, year1=None, year2=None, depth=1000):

        """
        Cache topn lists for a range of years.

        Args:
            levels (iter)
            year1 (int)
            year2 (int)
            depth (int)
        """

        self.levels = levels
        self.depth = depth

        self.topns = OrderedDict()

        for level in self.levels:

            mdw = cached.mdw(
                level1=level,
                level2=level,
                year1=year1,
                year2=year2,
            )

            topn = list(mdw.items())[:depth]

            ranks = OrderedDict()

            for i, (token, _) in enumerate(topn):
                ranks[token] = depth-i

            self.topns[level] = ranks
