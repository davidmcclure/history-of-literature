

import numpy as np

from mpi4py import MPI
from collections import defaultdict, Counter, OrderedDict
from functools import partial, lru_cache

from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.sql import text, func

from hol import config
from hol.utils import flatten_dict
from hol.models import Base
from hol.corpus import Corpus
from hol.volume import Volume



class AnchoredCount(Base):


    __tablename__ = 'anchored_count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year', 'anchor_count'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    anchor_count = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


    @classmethod
    def index(cls, anchor, size=1000):

        """
        Index anchored token counts.

        Args:
            anchor (str)
        """

        comm = MPI.COMM_WORLD

        size = comm.Get_size()
        rank = comm.Get_rank()

        # Scatter path segments.

        data = None

        if rank == 0:

            corpus = Corpus.from_env()

            data = np.array_split(list(corpus.paths()), size)

        paths = comm.scatter(data, root=0)

        # Tabulate the token counts.

        page = defaultdict(lambda: defaultdict(Counter))

        for path in paths:

            try:

                vol = Volume.from_path(path)

                if vol.is_english:

                    level_counts = vol.anchored_token_counts(anchor, size)

                    for level, counts in level_counts.items():
                        page[vol.year][level] += counts

            except Exception as e:
                print(e)

        # Gather the results, merge, flush to disk.

        pages = comm.gather(dict(page), root=0)

        if rank == 0:

            merged = defaultdict(lambda: defaultdict(Counter))

            for page in pages:
                for year, level_counts in page.items():
                    for level, counts in level_counts.items():
                        merged[year][level] += counts

            cls.flush(merged)


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

            # Whitelist tokens.
            if token in config.tokens:

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
