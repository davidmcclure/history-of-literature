

from invoke import task

from hol.models import BaseModel
from hol import config


@task
def init_db():

    """
    Create database tables.
    """

    engine = config.build_engine()

    # Create all tables.
    BaseModel.metadata.create_all(engine)
