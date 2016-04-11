

from multiprocessing import Pool
from collections import defaultdict, Counter

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
    def worker(path):

        """
        Extract a token counts for a volume.

        Args:
            path (str): A volume path.

        Returns:
            tuple (year<int>, counts<Counter>)
        """

        vol = Volume.from_path(path)

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

        corpus = Corpus.from_env()

        groups = corpus.path_groups(page_size)

        with Pool(num_procs) as pool:
            for i, group in enumerate(groups):

                # Queue volume jobs.
                jobs = pool.imap_unordered(cls.worker, group)

                page = defaultdict(Counter)

                # Gather counts for English volumes.
                for j, (year, counts) in enumerate(jobs):
                    page[year] += counts
                    print((i*page_size) + j)

                # Flush to disk.
                cls.flush_page(page)


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
