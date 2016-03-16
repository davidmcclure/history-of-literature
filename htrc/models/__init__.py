

from sqlalchemy import create_engine

from .edge import Edge


# TODO|dev
engine = create_engine('postgresql://localhost/htrc', echo=True)
