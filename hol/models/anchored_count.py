

import numpy as np

from functools import partial
from scipy.stats import chi2_contingency
from collections import OrderedDict

from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.sql import text, func

from hol import config
from hol.models import BaseModel, Count
from hol.utils import flatten_dict, sort_dict
from hol.corpus import Corpus
from hol.volume import Volume



class AnchoredCount(BaseModel):


    __tablename__ = 'anchored_count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year', 'anchor_count'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    anchor_count = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


    @classmethod
    def flush(cls, counts):

        """
        Flush a set of counts to disk.

        Args:
            counts (dict): year -> level -> token -> count
        """

        session = config.Session()

        # SQLite "upsert."
        query = text("""

            INSERT OR REPLACE INTO {table!s} (
                token,
                year,
                anchor_count,
                count
            )

            VALUES (
                :token,
                :year,
                :anchor_count,
                :count + COALESCE(
                    (
                        SELECT count FROM {table!s} WHERE (
                            token = :token AND
                            year = :year AND
                            anchor_count = :anchor_count
                        )
                    ),
                    0
                )
            )

        """.format(table=cls.__tablename__))

        for year, level, token, count in flatten_dict(counts):

            session.execute(query, dict(
                token=token,
                year=year,
                anchor_count=level,
                count=count,
            ))

        session.commit()


    @classmethod
    def token_year_count(cls, token, year):

        """
        How many times did token X appear in year Y on all pages that contain
        the anchor token?

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
    def token_year_level_count(cls, token, year, level):

        """
        How many times did token X appear in year Y on pages where the anchor
        token appeared Z times?

        Args:
            token (str)
            year (int)
            level (int)

        Returns: int
        """

        with config.get_session() as session:

            res = (
                session
                .query(func.sum(cls.count))
                .filter(
                    cls.token==token,
                    cls.year==year,
                    cls.anchor_count==level,
                )
            )

            return res.scalar() or 0


    @classmethod
    def year_count(cls, year):

        """
        How many tokens appeared on pages with the anchor token in year X?

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
    def year_count_series(cls, years, min_count=0):

        """
        Get total token counts for a set of years.

        Args:
            years (iter)
            min_count (int)

        Returns: OrderedDict {year: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.year, func.sum(cls.count))
                .filter(
                    cls.anchor_count > min_count,
                    cls.year.in_(years),
                )
                .group_by(cls.year)
                .order_by(cls.year)
            )

            return OrderedDict(res.all())


    @classmethod
    def token_counts_by_year(cls, year, min_count=0):

        """
        Get the counts for all tokens that appeared on pages with the anchor
        token in year X.

        Args:
            year (int)
            min_count (int)

        Returns: OrderedDict {token: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.token, func.sum(cls.count))
                .filter(
                    cls.anchor_count > min_count,
                    cls.year==year,
                )
                .group_by(cls.token)
                .order_by(cls.token.asc())
            )

            return OrderedDict(res.all())


    @classmethod
    def token_counts_by_years_and_levels(cls, years, levels):

        """
        Get anchored counts for all tokens, filtered by years and levels.

        Args:
            years (iter)
            levels (iter)

        Returns: OrderedDict {token: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.token, func.sum(cls.count))
                .filter(
                    cls.anchor_count.in_(levels),
                    cls.year.in_(years)
                )
                .group_by(cls.token)
                .order_by(cls.token.asc())
            )

            return OrderedDict(res.all())


    @classmethod
    def total_count_by_years_and_levels(cls, years, levels):

        """
        Get the total number of tokens that appears on pages with the anchor
        token, given a rance of years and levels

        Args:
            years (iter)
            levels (iter)

        Returns: OrderedDict {token: count, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(func.sum(cls.count))
                .filter(
                    cls.anchor_count.in_(levels),
                    cls.year.in_(years)
                )
            )

            return res.scalar() or 0


    @classmethod
    def topn(cls, years, levels):

        """
        Given a range of years and anchor levels, get a list of the top N most
        distinctive words around the anchor.

        Args:
            years (iter)
            levels (iter)

        Returns: OrderedDict {token: score, ...}
        """

        # a - number of times each token in anchored_count, filtered by year / level
        # b - number of times each token appears in count, filtered by year
        # c - total count from anchored_count, filtered by year / level
        # d - total count from count, filtered by year

        # AnchoredCount#token_counts()

        with config.get_session() as session:

            a = cls.token_counts_by_years_and_levels(years, levels)

            b = Count.token_counts_by_years(years)

            c = cls.total_count_by_years_and_levels()

            res = (
                session
                .query(func.sum(Count.count))
                .filter(Count.year.in_(years))
            )

            d = res.scalar()

            topn = dict()

            for token in a.keys():

                g, p, dof, exp = chi2_contingency(
                    np.array([[a[token], b[token]], [c, d]]),
                    lambda_='log-likelihood',
                )

                topn[token] = g

            return sort_dict(topn)
