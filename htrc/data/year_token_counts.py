

import shelve

from multiprocessing import Pool
from collections import defaultdict, Counter

from htrc import config
from htrc.data.keyset import Keyset
from htrc.corpus import Corpus
from htrc.volume import Volume



class YearTokenCounts(Keyset):


    @classmethod
    def from_env(cls):

        """
        Use the ENV-defined Redis database.

        Returns: cls
        """

        return cls(config['redis']['year_token_count'])


    def index(self, num_procs=8, cache_len=100):

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

            # Accumulate the counts.
            for i, (year, counts) in enumerate(jobs):

                # TODO: frequency filter?
                cache[year] += counts

                # Flush to Redis.
                if i % cache_len == 0:
                    self.flush_cache(cache)
                    cache.clear()

                print(i)


    def flush_cache(self, cache):

        """
        Flush a cache to the Redis.

        Args:
            cache (dict)
        """

        print('INCR {0}'.format(len(cache)))

        pipe = self.redis.pipeline()

        for year, counts in cache.items():
            for token, count in counts.items():
                pipe.hincrby(str(year), token, str(count))

        pipe.execute()



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
