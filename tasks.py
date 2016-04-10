

from invoke import task

from hol.models import Base
from hol import config


@task
def reset_db():

    """
    Recreate all database tables.
    """

    engine = config.build_engine()

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
