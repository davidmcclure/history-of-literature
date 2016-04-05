

import os

from pymongo import MongoClient
from collections import defaultdict, Counter
from multiprocessing import Pool

from htrc.corpus import Corpus
from htrc.volume import Volume



class Writer:


    def __init__(self):

        """
        Canonicalize the data path.

        Args:
            path (str)
        """

        client = MongoClient()

        self.db = client.hol.year_token_counts


    def index(self, num_procs=8, cache_len=1000):

        """
        Index total token counts by year.

        Args:
            num_procs (int)
        """

        corpus = Corpus.from_env()

        groups = corpus.path_groups(cache_len)

        with Pool(num_procs) as pool:
            for i, group in enumerate(groups):

                # Queue volume jobs.
                jobs = pool.imap_unordered(worker, group,)
                cache = defaultdict(Counter)

                # Accumulate counts.
                for j, (year, counts) in enumerate(jobs):
                    cache[year] += counts
                    print((i*cache_len) + j)

                # Flush to Vedis.
                self.flush_cache(cache)
                print(cache_len * (i+1))


    def flush_cache(self, cache):

        """
        Flush a cache to Mongo.

        Args:
            cache (dict)
        """

        for year, counts in cache.items():
            for token, count in counts.items():

                self.db.update_one(
                    {
                        '_id': '{0}:{1}'.format(year, token),
                        'year': year,
                        'token': token,
                    },
                    {
                        '$inc': {
                            'count': count
                        }
                    },
                    upsert=True,
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
