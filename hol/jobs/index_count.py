

from collections import defaultdict, Counter

from hol import config
from hol.utils import flatten_dict
from hol.jobs import BaseJob
from hol.volume import Volume
from hol.models import Count


class IndexCount(BaseJob):


    def __init__(self, *args, **kwargs):

        """
        Initialize the merged counter.
        """

        super().__init__(*args, **kwargs)

        self.data = defaultdict(Counter)


    def process(self, paths):

        """
        Accumulate counts for a set of paths.

        Args:
            paths (list)

        Returns: defaultdict(Counter)
        """

        for i, path in enumerate(paths):

            try:

                vol = Volume.from_path(path)

                if vol.is_english:
                    self.data[vol.year] += vol.token_counts()

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

        for year, token, count in flatten_dict(data):

            # Apply the token whitelist.
            if token in config.tokens:
                self.data[year][token] += count


    def flush(self):

        """
        Increment database counters.
        """

        Count.flush(self.data)
