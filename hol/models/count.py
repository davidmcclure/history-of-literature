

import numpy as np

from collections import defaultdict, Counter, OrderedDict

from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.schema import Index
from sqlalchemy.sql import text, func

from hol import config
from hol.utils import flatten_dict
from hol.models import BaseModel
from hol.corpus import Corpus
from hol.volume import Volume



class Count(BaseModel):


    __tablename__ = 'count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


    @classmethod
    def flush(cls, counts):

        """
        Flush a set of counts to disk.

        Args:
            page (dict): year -> token -> count
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

        for year, token, count in flatten_dict(counts):

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

        # series = OrderedDict()
        # for year, count in token_counts.items():
            # wpm = (1e6 * count) / baseline[year]
            # series[year] = wpm

        series = OrderedDict()
        for year in years:
            b = baseline.get(year, 0)
            t = token_counts.get(year, 0)
            series[year] = (1e6 * t) / b

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
    def token_counts_by_year(cls, year1=None, year2=None):

        """
        Get counts for tokens that appear in a range of years.

        Args:
            year1 (int)
            year2 (int)

        Returns: dict {token: count, ...}
        """

        with config.get_session() as session:

            query = session.query(cls.token, func.sum(cls.count))

            if year1:
                query = query.filter(cls.year >= year1)

            if year2:
                query = query.filter(cls.year <= year2)

            res = query.group_by(cls.token)

            return dict(res.all())


    @classmethod
    def total_count_by_year(cls, year1=None, year2=None):

        """
        Get the total token count for a range of years.

        Args:
            year1 (int)
            year2 (int)

        Returns: int
        """

        with config.get_session() as session:

            query = session.query(func.sum(cls.count))

            if year1:
                query = query.filter(cls.year >= year1)

            if year2:
                query = query.filter(cls.year <= year2)

            return query.scalar() or 0
