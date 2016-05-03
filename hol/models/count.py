

import numpy as np

from mpi4py import MPI
from collections import defaultdict, Counter, OrderedDict
from functools import lru_cache

from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.schema import Index
from sqlalchemy.sql import text, func

from hol import config
from hol.utils import flatten_dict
from hol.models import Base
from hol.corpus import Corpus
from hol.volume import Volume



def _map(paths):

    """
    Tabulate token counts for a set of volumes.

    Args:
        paths (list)

    Returns: defaultdict(Counter)
    """

    counts = defaultdict(Counter)

    for path in paths:

        try:

            vol = Volume.from_path(path)

            if vol.is_english:
                counts[vol.year] += vol.token_counts()

        except Exception as e:
            print(e)

    return counts



def _reduce(results):

    """
    Merge together the count segments.

    Args:
        results ([defaultdict(Counter)])

    Returns: defaultdict(Counter)
    """

    merged = defaultdict(Counter)

    for result in results:
        for year, counts in result.items():
            merged[year] += counts

    return merged



class Count(Base):


    __tablename__ = 'count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


    @classmethod
    def index(cls):

        """
        Index token counts by year.
        """

        corpus = Corpus.from_env()

        result = corpus.map_mpi(_map, _reduce)

        cls.flush_page(result)


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

            INSERT OR REPLACE INTO {table!s} (token, year, count)

            VALUES (
                :token,
                :year,
                :count + COALESCE(
                    (
                        SELECT count FROM {table!s}
                        WHERE token = :token AND year = :year
                    ),
                    0
                )
            )

        """.format(table=cls.__tablename__))

        for year, token, count in flatten_dict(page):

            # Whitelist tokens.
            if token in config.tokens:

                session.execute(query, dict(
                    token=token,
                    year=year,
                    count=count,
                ))

        session.commit()


    @classmethod
    def total_token_count(cls):

        """
        Get the total number of observed tokens.

        Returns: int
        """

        with config.get_session() as session:

            res = (
                session
                .query(func.sum(cls.count))
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

        with config.get_session() as session:

            res = (
                session
                .query(func.sum(cls.count))
                .filter(cls.token==token, cls.year==year)
            )

            return res.scalar() or 0


    @classmethod
    def year_count(cls, year):

        """
        How many tokens appeared in year X?

        Args:
            year (int)

        Returns: int
        """

        with config.get_session() as session:

            res = (
                session
                .query(func.sum(cls.count))
                .filter(cls.year==year)
            )

            return res.scalar() or 0


    @classmethod
    def year_count_series(cls, years):

        """
        Get total token counts for a set of years.

        Args:
            years (iter)

        Returns: OrderedDict {year: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.year, func.sum(cls.count))
                .filter(cls.year.in_(years))
                .group_by(cls.year)
                .order_by(cls.year)
            )

            return OrderedDict(res.all())


    @classmethod
    def token_count_series(cls, token, years):

        """
        Get counts for a specific token for a set of years.

        Args:
            token (str)
            years (iter)

        Returns: OrderedDict {year: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.year, func.sum(cls.count))
                .filter(cls.token==token, cls.year.in_(years))
                .group_by(cls.year)
                .order_by(cls.year)
            )

            return OrderedDict(res.all())


    @classmethod
    def token_count_series_smooth(cls, token, years, width=10):

        """
        Smooth the count series for a token.

        Args:
            token (str)
            years (iter)
            width (int)

        Returns: OrderedDict {year: wpm, ...}
        """

        series = cls.token_count_series(token, years)

        wpms = series.values()

        smooth = np.convolve(
            list(wpms),
            np.ones(width) / width,
        )

        return OrderedDict(zip(series.keys(), smooth))


    @classmethod
    def token_wpm_series(cls, token, years):

        """
        Get a WMP series for a token.

        Args:
            token (str)
            years (iter)

        Returns: OrderedDict {year: wpm, ...}
        """

        baseline = cls.year_count_series(years)

        token_counts = cls.token_count_series(token, years)

        series = OrderedDict()
        for year, count in token_counts.items():
            wpm = (1e6 * count) / baseline[year]
            series[year] = wpm

        return series


    @classmethod
    def token_wpm_series_smooth(cls, token, years, width=10):

        """
        Smooth the WMP series for a token.

        Args:
            token (str)
            years (iter)
            width (int)

        Returns: OrderedDict {year: wpm, ...}
        """

        series = cls.token_wpm_series(token, years)

        wpms = series.values()

        smooth = np.convolve(
            list(wpms),
            np.ones(width) / width,
        )

        return OrderedDict(zip(series.keys(), smooth))


    @classmethod
    def token_counts_by_year(cls, year):

        """
        Get the counts for all tokens that appeared in year X.

        Args:
            year (int)

        Returns: OrderedDict {token: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.token, cls.count)
                .filter(cls.year==year)
                .order_by(cls.token.asc())
            )

            return OrderedDict(res.all())
