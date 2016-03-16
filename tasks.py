

from invoke import task

from htrc.models import Base
from htrc import config


@task
def reset_db():

    """
    Recreate all database tables.
    """

    engine = config.make_engine()

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
