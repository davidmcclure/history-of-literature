

from sqlalchemy.schema import Index
from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.sql import text, func
from collections import defaultdict, Counter
from multiprocessing import Pool

from htrc import config
from htrc.corpus import Corpus
from htrc.corpus import Volume
from htrc.models import Base



class Count(Base):


    __tablename__ = 'count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year'),
        dict(prefixes=['UNLOGGED']),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


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
                jobs = pool.imap_unordered(worker, group)

                page = defaultdict(Counter)

                # Accumulate counts.
                for j, (year, counts) in enumerate(jobs):
                    page[year] += counts
                    print((i*page_size) + j)

                # Flush to the disk.
                cls.flush_page(page)


    @classmethod
    def flush_page(cls, page):

        """
        Flush a page to disk.

        Args:
            page (dict)
        """

        session = config.Session()

        for year, counts in page.items():
            for token, count in counts.items():

                query = text("""
                    INSERT INTO count (token, year, count)
                    VALUES (:token, :year, :count)
                    ON CONFLICT (token, year)
                    DO UPDATE SET count = count.count + :count
                """)

                session.execute(query, dict(
                    token=token,
                    year=year,
                    count=count,
                ))

        session.commit()


    @classmethod
    def years(cls):

        """
        Get an ordered list of years.

        Returns: list<int>
        """

        res = (
            config.Session()
            .query(cls.year)
            .distinct()
            .order_by(cls.year.asc())
        )

        return [r[0] for r in res]


    @classmethod
    def tokens(cls):

        """
        Get an ordered list of all tokens.

        Returns: list<int>
        """

        res = (
            config.Session()
            .query(cls.token)
            .distinct()
            .order_by(cls.token.asc())
        )

        return [r[0] for r in res]


    @classmethod
    def year_count(cls, year):

        """
        Get the total token count for a year.

        Args:
            year (int)

        Returns: int
        """

        res = (
            config.Session()
            .query(func.sum(cls.count))
            .filter(cls.year==year)
        )

        return res.scalar()


    @classmethod
    def token_year_count(cls, token, year):

        """
        How many times did token X appear in year Y?

        Args:
            token (str)
            year (int)

        Returns: int
        """

        res = (
            config.Session()
            .query(func.sum(cls.count))
            .filter(cls.token==token, cls.year==year)
        )

        return res.scalar()



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
