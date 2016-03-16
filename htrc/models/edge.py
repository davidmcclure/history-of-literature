

from sqlalchemy import Column, Integer, String
from sqlalchemy.schema import Index

from htrc.models import Base, Session


class Edge(Base):


    __tablename__ = 'edge'

    id = Column(Integer, primary_key=True)

    token1 = Column(String, nullable=False, index=True)

    token2 = Column(String, nullable=False, index=True)

    year = Column(Integer, nullable=False)

    weight = Column(Integer, nullable=False)


    @classmethod
    def index_volume(cls, volume, min_freq=1e-03):

        """
        Index edges from a volume.

        Args:
            volume (Volume)
            min_freq (float)
        """

        graph = volume.graph(min_freq=min_freq)

        session = Session()

        for t1, t2, data in graph.edges_iter(data=True):

            weight = data.get('weight')

            edge = cls(
                token1=t1,
                token2=t2,
                year=volume.year,
                weight=weight,
            )

            session.add(edge)

        session.commit()


# Unique index on the token pair.
Index(
    'ix_edge_token1_token2_year',
    Edge.token1,
    Edge.token2,
    Edge.year,
    unique=True,
)
