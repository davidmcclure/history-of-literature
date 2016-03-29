

from redis import StrictRedis
from multiprocessing import Pool
from collections import Counter

from htrc.corpus import Corpus
from htrc.volume import Volume



class CountData:


    def __init__(self):

        """
        Initialize the Redis connection.
        """

        self.redis = StrictRedis()


    def token_count_for_year(self, token, year):

        """
        Get the total count for a token in a year.

        Args:
            year (int)
            token (str)
        """

        count = self.redis.hmget(str(year), token)

        return int(count[0])


    def index(self, num_procs=8, cache_len=5000):

        """
        Index per-year counts for all tokens.

        Args:
            num_procs (int)
            cache_len (int)
        """

        corpus = Corpus.from_env()

        cache = Counter()

        with Pool(num_procs) as pool:

            # Spool a job for each volume.
            jobs = pool.imap_unordered(
                get_vol_counts,
                corpus.paths(),
            )

            for i, (year, counts) in enumerate(jobs):

                # Update the cache.
                for token, count in counts.items():
                    cache[(token, year)] += count

                # Flush to Redis.
                if i % cache_len == 0:
                    self.flush_cache(cache)
                    cache.clear()

                print(i)


    def flush_cache(self, cache):

        """
        Flush a (token, year) -> count cache to the database.

        Args:
            cache (Counter)
        """

        print(len(cache))

        pipe = self.redis.pipeline()

        for (token, year), count in cache.items():
            pipe.hincrby(str(year), token, str(count))

        pipe.execute()



def get_vol_counts(path):

    """
    Extract filtered token counts from a volume.

    Args:
        path (str): A HTRC volume path.

    Returns: dict
    """

    vol = Volume(path)

    return (vol.year, vol.total_counts())
