

import os

from vedis import Vedis
from collections import defaultdict, Counter
from multiprocessing import Pool

from htrc.corpus import Corpus
from htrc.volume import Volume



class YearTokenCounts:


    def __init__(self, path):

        """
        Initialize the Vedis connection.
        """

        self.db = Vedis(path)


    def index(self, num_procs=8, cache_len=1000):

        """
        Index total token counts by year.

        Args:
            num_procs (int)
        """

        corpus = Corpus.from_env()

        cache = defaultdict(Counter)

        with Pool(num_procs) as pool:

            # Queue volume jobs.
            jobs = pool.imap_unordered(
                worker,
                corpus.paths(),
            )

            for i, (year, counts) in enumerate(jobs):

                cache[year] += counts

                # Flush to Redis.
                if i % cache_len == 0:

                    print(i)

                    self.flush_cache(cache)
                    cache.clear()


    def flush_cache(self, cache):

        """
        Flush a cache to the Redis.

        Args:
            cache (dict)
        """

        for year, counts in cache.items():
            for token, count in counts.items():
                self.db.incr_by((token, year), count)



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
