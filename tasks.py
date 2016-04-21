

from invoke import task

from hol.models import Base
from hol import config


@task
def init_db():

    """
    Create database tables.
    """

    engine = config.build_engine()

    # Create all tables.
    Base.metadata.create_all(engine)
