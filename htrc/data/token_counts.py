

import os
import numpy as np
import h5py

from multiprocessing import Pool
from collections import defaultdict, Counter
from datetime import datetime as dt

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

        self.file = h5py.File(os.path.abspath(path))

        self.years = {
            y:i for i, y in enumerate(range(1600, 1920))
        }

        self.tokens = {
            t:i for i, t in enumerate(config.tokens)
        }

        self.counts = self.file.require_dataset(
            'counts',
            (len(self.tokens), len(self.years)),
            dtype='i',
        )


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

        t1 = dt.now()

        for year, counts in page.items():
            for token, count in counts.items():

                # Ignore infrequent tokens.
                if token not in config.tokens:
                    continue

                try:

                    # Get the token row, increment.
                    row = self.counts[self.tokens[token]]
                    row[self.years[year]] += count

                    # Set the updated array.
                    self.counts[self.tokens[token]] = row

                except:
                    pass

        t2 = dt.now()
        print(t2-t1)



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
