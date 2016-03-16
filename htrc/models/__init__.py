

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .base import Base
from .edge import Edge


# TODO: Read from config.
engine = create_engine('postgresql://localhost/htrc', echo=True)

Session = sessionmaker(bind=engine)
