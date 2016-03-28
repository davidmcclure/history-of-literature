

from multiprocessing import Pool
from collections import Counter

from sqlalchemy import Column, Integer, String
from sqlalchemy.schema import Index

from htrc import config
from htrc.corpus import Corpus
from htrc.corpus import Volume
from htrc.models import Base



class Count(Base):


    __tablename__ = 'count'

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False, index=True)

    year = Column(Integer, nullable=False, index=True)

    count = Column(Integer, nullable=False)


    @classmethod
    def index(cls, num_procs=8, cache_len=100):

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
            jobs = pool.imap(get_vol_counts, corpus.paths())

            for i, (year, counts) in enumerate(jobs):

                # Update the cache.
                for token, count in counts.items():
                    cache[(token, year)] += count

                # Flush to disk.
                if i % cache_len == 0:
                    print(len(cache))
                    cls.flush_cache(cache)
                    cache.clear()

                print(i)


    @classmethod
    def flush_cache(cls, cache):

        """
        Flush a (token, year) -> count cache to the database.

        Args:
            cache (Counter)
        """

        session = config.Session()

        for (token, year), count in cache.items():

            # Try to update an existing edge.

            updated = (
                session.query(cls)
                .filter_by(token=token, year=year)
                .update(dict(count = cls.count + count))
            )

            # If no rows updated, initialize the edge.

            if updated == 0:
                row = cls(token=token, year=year, count=count)
                session.add(row)

        session.commit()



# Unique index on the token-year pair.
Index(
    'ix_count_token_year',
    Count.token,
    Count.year,
    unique=True,
)



def get_vol_counts(path):

    """
    Extract filtered token counts from a volume.

    Args:
        path (str): A HTRC volume path.

    Returns: dict
    """

    vol = Volume(path)

    return (vol.year, vol.total_counts())
