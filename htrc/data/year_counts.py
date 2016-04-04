

import shelve

from multiprocessing import Pool

from htrc.corpus import Corpus
from htrc.volume import Volume



class YearCounts:


    def __init__(self, path):

        """
        Open the shelf.

        Args:
            path (str)
        """

        self.data = shelve.open(path)


    def index(self, num_procs=8):

        """
        Index total token counts by year.

        Args:
            num_procs (int)
        """

        corpus = Corpus.from_env()

        with Pool(num_procs) as pool:

            # Job for each volume.
            jobs = pool.imap_unordered(
                get_vol_count,
                corpus.paths(),
            )

            # Accumulate the counts.
            for i, (year, count) in enumerate(jobs):

                if year in self.data:
                    self.data[year] += count

                else:
                    self.data[year] = count

                print(i)



def get_vol_count(path):

    """
    Extract a total token count from a volume.

    Args:
        path (str): A volume path.

    Returns: tuple (year<str>, count<int>)
    """

    vol = Volume(path)

    return (str(vol.year), vol.token_count)
