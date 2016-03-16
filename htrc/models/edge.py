

from sqlalchemy import Column, Integer, String, sql
from sqlalchemy.schema import Index

from htrc import config
from htrc.models import Base


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

        session = config.Session()

        for t1, t2, data in graph.edges_iter(data=True):

            weight = data.get('weight')

            # Does the pair / year edge already exist?

            exists = session.query(
                sql.exists()
                .where(cls.token1==t1)
                .where(cls.token2==t2)
                .where(cls.year==volume.year)
            )

            # If an edge exists, update the weight.

            if exists.scalar():

                session.query(cls).filter_by(
                    token1=t1,
                    token2=t2,
                    year=volume.year,
                ).update(dict(
                    weight = cls.weight + weight
                ))

            # Otherwise, insert a new edge.

            else:

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
