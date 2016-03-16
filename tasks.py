

from invoke import task

from htrc.models import Base, engine


@task
def reset_db():

    """
    Recreate all database tables.
    """

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
