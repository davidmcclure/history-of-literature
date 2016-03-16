

from sqlalchemy import create_engine
from sqlalchemy.org import sessionmaker

from .edge import Edge


# TODO: Read from config.
engine = create_engine('postgresql://localhost/htrc', echo=True)

Session = sessionmaker(bind=engine)
