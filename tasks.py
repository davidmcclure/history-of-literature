

from invoke import task

from hol.models import Base
from hol import config


@task
def reset_db():

    """
    Initialize the database.
    """

    engine = config.build_engine()

    # Clear and recreate all tables.
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
