

import os

from multiprocessing import Pool

from htrc.corpus import Corpus
from htrc.volume import Volume


class CountData:


    def __init__(self, path):

        """
        Canonicalize the root path.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)


    def index(self, numprocs=8):

        """
        Index per-year counts.

        Args:
            numprocs (int)
        """

        corpus = Corpus.from_env()

        with Pool(numprocs) as pool:

            # Spool a job for each volume.
            jobs = pool.imap(get_vol_counts, corpus.paths())

            for i, (year, counts) in enumerate(jobs):
                print(year, len(counts))


def get_vol_counts(path):

    """
    Extract filtered token counts from a volume.

    Args:
        path (str): A HTRC volume path.

    Returns: dict
    """

    vol = Volume(path)

    return (vol.year, vol.total_counts())
