

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# TODO: Break this into a separate config manager.


# TODO: Read from config.
engine = create_engine('postgresql://localhost/htrc', echo=True)
Session = sessionmaker(bind=engine)


from .base import Base
from .edge import Edge
