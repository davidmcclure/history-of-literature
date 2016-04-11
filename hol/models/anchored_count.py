

from functools import partial

from sqlalchemy import Column, Integer, String, PrimaryKeyConstraint

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

            for year, counts in results:
                print(year, counts)

            # TODO
