

import shelve

from multiprocessing import Pool

from htrc.corpus import Corpus
from htrc.volume import Volume
from htrc.data.shelf import Shelf



class YearTokenCounts(Shelf):


    def index(self, num_procs=8):

        """
        Index total token counts by year.

        Args:
            num_procs (int)
        """

        # TODO: filter out infrequent words?

        corpus = Corpus.from_env()

        with Pool(num_procs) as pool:

            # Job for each volume.
            jobs = pool.imap_unordered(
                worker,
                corpus.paths(),
            )

            # Accumulate the counts.
            for i, (year, counts) in enumerate(jobs):

                if year in self.data:
                    self.data[year] += counts

                else:
                    self.data[year] = counts

                print(i)



def worker(path):

    """
    Extract a token counts for a volume.

    Args:
        path (str): A volume path.

    Returns:
        tuple (year<str>, counts<Counter>)
    """

    vol = Volume(path)

    return (str(vol.year), vol.cleaned_token_counts())
