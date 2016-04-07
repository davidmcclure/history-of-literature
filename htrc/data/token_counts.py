

import os
import numpy as np
import h5py

from multiprocessing import Pool
from collections import defaultdict, Counter

from htrc import config
from htrc.corpus import Corpus
from htrc.corpus import Volume



class TokenCounts:


    def __init__(self, path):

        """
        Open the HDF file.

        Args:
            path (str)
        """

        self.data = h5py.File(os.path.abspath(path))


    def index(self, num_procs=12, page_size=1000):

        """
        Index token counts by year.

        Args:
            num_procs (int)
            cache_len (int)
        """

        corpus = Corpus.from_env()

        groups = corpus.path_groups(page_size)

        with Pool(num_procs) as pool:
            for i, group in enumerate(groups):

                # Queue volume jobs.
                jobs = pool.imap_unordered(worker, group)

                page = defaultdict(Counter)

                # Accumulate counts.
                for j, (year, counts) in enumerate(jobs):
                    page[year] += counts
                    print((i*page_size) + j)

                # Flush to the disk.
                self.flush_page(page)


    def flush_page(self, page):

        """
        Flush a page to disk.

        Args:
            page (dict)
        """

        for year, counts in page.items():
            for token, count in counts.items():

                # Ignore infrequent tokens.
                if token not in config.tokens:
                    continue

                path = '{0}/{1}'.format(year, token)

                # If the path is set, update the count.
                if path in self.data:
                    self.data[path][0] += count

                # Or, initialize the count.
                else:

                    val = np.array(count)

                    self.data.create_dataset(
                        path, (1,), dtype='i', data=val
                    )



def worker(path):

    """
    Extract a token counts for a volume.

    Args:
        path (str): A volume path.

    Returns:
        tuple (year<int>, counts<Counter>)
    """

    vol = Volume(path)

    return (vol.year, vol.cleaned_token_counts())
