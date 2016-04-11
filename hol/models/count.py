

from multiprocessing import Pool
from collections import defaultdict, Counter
from functools import partial

from sqlalchemy.schema import Index
from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.sql import text

from hol import config
from hol.corpus import Corpus
from hol.corpus import Volume
from hol.models import Base



class Count(Base):


    __tablename__ = 'count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


    @staticmethod
    def worker(vol):

        """
        Extract a token counts for a volume.

        Args:
            vol (Volume)

        Returns:
            tuple (year<int>, counts<Counter>)
        """

        counts = vol.cleaned_token_counts()

        return (vol.year, counts)


    @classmethod
    def index(cls, num_procs=12, page_size=1000):

        """
        Index token counts by year.

        Args:
            num_procs (int)
            cache_len (int)
        """

        for i, results in enumerate(map_english_vols(cls.worker)):

            page = defaultdict(Counter)

            for year, counts in results:
                page[year] += counts

            cls.flush_page(page)

            print(page_size*(i+1))


    @classmethod
    def flush_page(cls, page):

        """
        Flush a page to disk.

        Args:
            page (dict)
        """

        session = config.Session()

        # SQLite "upsert."
        query = text("""

            INSERT OR REPLACE INTO count (token, year, count)

            VALUES (
                :token,
                :year,
                :count + COALESCE(
                    (
                        SELECT count FROM count
                        WHERE token = :token AND year = :year
                    ),
                    0
                )
            )

        """)

        for year, counts in page.items():
            for token, count in counts.items():

                # Whitelist tokens.
                if token in config.tokens:

                    session.execute(query, dict(
                        token=token,
                        year=year,
                        count=count,
                    ))

        session.commit()



def map_english_vols(worker, num_procs=12, page_size=100):

    """
    Apply a worker to English volumes in a set of paths.

    Args:
        paths (list)
        worker (func)
    """

    corpus = Corpus.from_env()

    groups = corpus.path_groups(page_size)

    with Pool(num_procs) as pool:
        for i, paths in enumerate(groups):

            jobs = pool.imap_unordered(
                partial(map_vol, worker),
                paths,
            )

            yield JobGroup(jobs)



def map_vol(worker, path):

    """
    Inflate a volume and map a function, if it's English.

    Args:
        worker (func)
        path (str)
    """

    vol = Volume.from_path(path)

    if vol.is_english:
        return worker(vol)



class JobGroup:

    def __init__(self, jobs):
        self.jobs = jobs

    def __iter__(self):
        for result in self.jobs:
            if result: yield result
