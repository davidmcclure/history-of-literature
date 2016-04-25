

from functools import partial, lru_cache
from collections import defaultdict, Counter, OrderedDict

from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.sql import text, func

from hol import config
from hol.utils import flatten_dict
from hol.models import Base
from hol.corpus import Corpus


class AnchoredCount(Base):


    __tablename__ = 'anchored_count'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year', 'anchor_count'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    anchor_count = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)


    @staticmethod
    def worker(anchor, vol):

        """
        Extract anchored token counts for a volume.

        Args:
            anchor (str)
            vol (Volume)

        Returns:
            tuple (year<int>, counts<dict>)
        """

        counts = vol.anchored_token_counts(anchor)

        return (vol.year, counts)


    @classmethod
    def index(cls, anchor, num_procs=12, page_size=1000):

        """
        Index token counts by year.

        Args:
            anchor (str)
            num_procs (int)
            cache_len (int)
        """

        corpus = Corpus.from_env()

        # Apply the anchor token.
        worker = partial(cls.worker, anchor)

        mapper = corpus.map(worker, num_procs, page_size)

        for i, results in enumerate(mapper):

            # year -> level -> counts
            page = defaultdict(lambda: defaultdict(Counter))

            for year, level_counts in results:
                for level, counts in level_counts.items():
                    page[year][level] += counts

            cls.flush_page(page)

            print((i+1)*page_size)


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

        for year, level, token, count in flatten_dict(page):

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
