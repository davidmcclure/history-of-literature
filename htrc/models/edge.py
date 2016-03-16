

from sqlalchemy import Column, Integer, String
from sqlalchemy.schema import Index

from htrc.models.base import Base


class Edge(Base):


    __tablename__ = 'edge'

    id = Column(Integer, primary_key=True)

    token1 = Column(String, nullable=False, index=True)

    token2 = Column(String, nullable=False, index=True)

    year = Column(Integer, nullable=False)

    weight = Column(Integer, nullable=False)


# Unique index on the token pair.
Index(
    'ix_edge_token1_token2_year',
    Edge.token1,
    Edge.token2,
    Edge.year,
    unique=True,
)
