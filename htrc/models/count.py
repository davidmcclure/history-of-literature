

from sqlalchemy import Column, Integer, String
from sqlalchemy.schema import Index

from htrc import config
from htrc.models import Base



class Count(Base):


    __tablename__ = 'count'

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False, index=True)

    year = Column(Integer, nullable=False, index=True)

    count = Column(Integer, nullable=False)



# Unique index on the token-year pair.
Index(
    'ix_count_token_year',
    Count.token,
    Count.year,
    unique=True,
)
