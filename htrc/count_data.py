

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

        with Pool(8) as pool:

            func = partial(index_volume, self.path)

            worker = pool.imap(func, corpus.paths())

            for i, _ in enumerate(worker):
                print(i)



def index_volume(data_path, vol_path):

    """
    Index counts from a volume

    Args:
        data_path (str)
        vol_path (str)
    """

    rlite = Rlite(data_path)

    volume = Volume(vol_path)

    for token, count in volume.total_counts().items():

        rlite.command(
            'hincrby',
            str(volume.year),
            token,
            str(count),
        )

    print(volume.id)
