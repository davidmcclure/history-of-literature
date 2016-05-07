

from collections import defaultdict, Counter

from hol.jobs import BaseJob
from hol.models import AnchoredCount
from hol.volume import Volume


class IndexAnchoredCount(BaseJob):


    def __init__(self, anchor, page_size=1000, *args, **kwargs):

        """
        Set the anchor token.

        Args:
            anchor (str)
            page_size (int)
        """

        super().__init__(*args, **kwargs)

        self.anchor = anchor
        self.page_size = page_size

        self.cache = defaultdict(lambda: defaultdict(Counter))


    def process(self, paths):

        """
        Accumulate counts for a set of paths.

        Args:
            paths (list)

        Returns: defaultdict(Counter)
        """

        result = defaultdict(lambda: defaultdict(Counter))

        for path in paths:

            try:

                vol = Volume.from_path(path)

                if vol.is_english:

                    counts = vol.anchored_token_counts(
                        self.anchor,
                        self.page_size,
                    )

                    for level, counts in counts.items():
                        result[vol.year][level] += counts

            except Exception as e:
                print(e)

        return dict(result)


    def merge(self, result):

        """
        Merge in a batch of counts from a rank.

        Args:
            result (dict)
        """

        for year, level_counts in result.items():
            for level, counts in level_counts.items():
                self.cache[year][level] += counts


    def flush(self):

        """
        Increment database counters.
        """

        AnchoredCount.flush(self.cache)
