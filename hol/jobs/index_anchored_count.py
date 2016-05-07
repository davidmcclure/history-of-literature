

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

        self.data = defaultdict(lambda: defaultdict(Counter))


    def process(self, paths):

        """
        Accumulate counts for a set of paths.

        Args:
            paths (list)

        Returns: defaultdict(Counter)
        """

        for path in paths:

            try:

                vol = Volume.from_path(path)

                if vol.is_english:

                    counts = vol.anchored_token_counts(
                        self.anchor,
                        self.page_size,
                    )

                    for level, counts in counts.items():
                        self.data[vol.year][level] += counts

            except Exception as e:
                print(e)


    def shrinkwrap(self):

        """
        Format the counters for MPI.

        Returns: dict
        """

        return dict(self.data)


    def merge(self, data):

        """
        Merge in a batch of counts from a rank.

        Args:
            data (dict)
        """

        for year, level_counts in data.items():
            for level, counts in level_counts.items():
                self.data[year][level] += counts


    def flush(self):

        """
        Increment database counters.
        """

        AnchoredCount.flush(self.data)
