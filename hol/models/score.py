

import numpy as np

from scipy.stats import chi2_contingency, rankdata
from collections import OrderedDict

from sqlalchemy import Column, Integer, String, Float, PrimaryKeyConstraint

from hol import config
from hol.models import Base, Count, AnchoredCount
from hol.utils import scale_01


class Score(Base):


    __tablename__ = 'score'

    __table_args__ = (
        PrimaryKeyConstraint('token', 'year'),
    )

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    score = Column(Float, nullable=False)


    @classmethod
    def index(cls, years):

        """
        Index log-likelihood scores.

        Args:
            years (iter)
        """

        session = config.Session()

        # Clear existing rows.
        session.query(cls).delete()

        # Per-year counts.
        yc0 = AnchoredCount.year_count_series(years)
        yc1 = Count.year_count_series(years)

        for year in years:

            c = yc0.get(year, 0)
            d = yc1.get(year, 0)

            # Per-year, per-token counts.
            tc0 = AnchoredCount.token_counts_by_year(year)
            tc1 = Count.token_counts_by_year(year)

            if not tc0: continue

            rows = []
            for token in tc0.keys():

                a = tc0.get(token, 0)
                b = tc1.get(token, 0)

                # Compute the log-likelihood.
                score, p, dof, exp = chi2_contingency(
                    np.array([[a, b], [c, d]]),
                    lambda_='log-likelihood',
                )

                rows.append((token, year, score))

            # Rank the scores.
            scores = [r[2] for r in rows]
            ranked = rankdata(scores, method='dense')

            # Scale to 0-1.
            scaled = scale_01(ranked)

            # Flush the rows.
            for (token, year, _), score in zip(rows, scaled):

                session.add(cls(
                    token=token,
                    year=year,
                    score=score,
                ))

            print(year)

        session.commit()


    @classmethod
    def tokens(cls):

        """
        Get an ordered list of all tokens.

        Returns: list<str>
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.token)
                .distinct()
                .order_by(cls.token.asc())
            )

            return [r[0] for r in res]


    @classmethod
    def topn_by_year(cls, year, n):

        """
        Get the top N tokens for a year.

        Args:
            year (int)
            n (int)

        Returns: [(token, rank), ...]
        """

        with config.get_session() as session:

            ranks = OrderedDict()

            res = (
                session
                .query(cls.token, cls.score)
                .filter(cls.year==year)
                .order_by(cls.score.desc())
                .limit(n)
            )

            return OrderedDict(res.all())


    @classmethod
    def token_series(cls, token, years):

        """
        Get scores for a set of years.

        Args:
            token (str)
            years (iter)

        Returns: OrderedDict {year: score, ...}
        """

        with config.get_session() as session:

            res = (
                session
                .query(cls.year, cls.score)
                .filter(cls.token==token, cls.year.in_(years))
                .group_by(cls.year)
                .order_by(cls.year)
            )

            return OrderedDict(res.all())


    @classmethod
    def token_series_smooth(cls, token, years, width=10):

        """
        Smooth the series for a token.

        Args:
            token (str)
            years (iter)
            width (int)

        Returns: OrderedDict {year: wpm, ...}
        """

        series = cls.token_series(token, years)

        if series:

            wpms = series.values()

            smooth = np.convolve(
                list(wpms),
                np.ones(width) / width,
                mode='same',
            )

            return OrderedDict(zip(series.keys(), smooth))

        # No data.
        else: return series


    @classmethod
    def ranked_series(cls, years, width=10):

        """
        Compute smoothed series for all tokens, rank by variance.

        Args:
            years (iter)
            width (int)

        Returns: OrderedDict {token: series, ...}
        """

        tsv = []
        for t in Score.tokens():

            # Smoothed log-likelihood series.
            s = cls.token_series_smooth(t, years, width)

            # Variance of the series.
            if s:
                v = np.var(list(s.values()))
                tsv.append((t, s, v))

        # Sort descending.
        tsv = sorted(tsv, key=lambda x: x[2], reverse=True)

        result = OrderedDict()

        # Index by token.
        for (t, s, v) in tsv:
            result[t] = (s, v)

        return result
