

from sqlalchemy import Column, Integer, String

from htrc.models.base import Base


class Edge(Base):


    __tablename__ = 'edge'


    id = Column(Integer, primary_key=True)

    token1 = Column(String, nullable=False)

    token2 = Column(String, nullable=False)

    weight = Column(Integer, nullable=False)
