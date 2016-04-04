

from sqlalchemy.schema import Index
from sqlalchemy import Column, Integer, String
from multiprocessing import Pool

from htrc.models import Base
from htrc.corpus import Corpus
from htrc.corpus import Volume



class Count(Base):


    __tablename__ = 'count'

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False, index=True)

    year = Column(Integer, nullable=False, index=True)

    count = Column(Integer, nullable=False)


    @classmethod
    def index(self, num_procs=8):

        """
        Index token counts by year.

        Args:
            num_procs (int)
        """

        # TODO: filter out infrequent words?

        corpus = Corpus.from_env()

        with Pool(num_procs) as pool:

            # Job for each volume.
            jobs = pool.imap_unordered(
                worker,
                corpus.paths(),
            )

            # Accumulate the counts.
            for i, (year, counts) in enumerate(jobs):
                print(year, len(counts))



# Unique index on the token-year pair.
Index(
    'ix_count_token_year',
    Count.token,
    Count.year,
    unique=True,
)



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
