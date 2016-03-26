

import os

from hirlite import Rlite
from multiprocessing import Pool
from functools import partial
from collections import Counter

from htrc.corpus import Corpus
from htrc.volume import Volume



class CountData:


    def __init__(self, path):

        """
        Initialize the rlite connection.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)

        self.rlite = Rlite(self.path)


    def incr_token_count_for_year(self, token, year, count):

        """
        Increment the count for a token in a year by a given amount.

        Args:
            token (str)
            year (int)
            count (int)
        """

        self.rlite.command('hincrby', str(year), token, str(count))


    def token_count_for_year(self, token, year):

        """
        Get the total count for a token in a year.

        Args:
            year (int)
            token (str)
        """

        count = self.rlite.command('hmget', str(year), token)

        return int(count[0])


    # TODO|dev
    def index(self):

        """
        Index per-year token counts.
        """

        corpus = Corpus.from_env()

        for i, volume in enumerate(corpus.volumes()):

            for token, count in volume.total_counts().items():

                self.incr_token_count_for_year(
                    token,
                    volume.year,
                    count,
                )

            print(i)
