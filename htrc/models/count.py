

from sqlalchemy.schema import Index
from sqlalchemy import Column, Integer, String
from multiprocessing import Pool

from htrc.models import Base
from htrc.corpus import Corpus
from htrc.corpus import Volume



class Count(Base):


    __tablename__ = 'count'

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False)

    year = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)



# Unique index token + year.
Index(
    'ix_count_token_year',
    Count.token,
    Count.year,
    unique=True,
)
