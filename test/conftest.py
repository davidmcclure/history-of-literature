

import os
import pytest
import yaml

from hol import config as _config
from hol.models import BaseModel

from test.mock_corpus import MockCorpus


@pytest.fixture(scope='session', autouse=True)
def test_env():

    """
    Register the testing config file.
    """

    _config.paths.append('~/.hol.test.yml')
    _config.read()


@pytest.yield_fixture
def config():

    """
    Reset the configuration object after each test.

    Yields:
        The modify-able config object.
    """

    yield _config
    _config.read()


@pytest.yield_fixture
def mock_corpus(config):

    """
    Provide a MockCorpus instance.

    Yields:
        MockCorpus
    """

    corpus = MockCorpus()

    # Point config -> mock.
    config.config.update({
        'corpus': corpus.path
    })

    yield corpus

    corpus.teardown()


@pytest.fixture()
def db(config):

    """
    Create / reset the testing database.
    """

    engine = config.build_engine()

    # Clear and recreate all tables.
    BaseModel.metadata.drop_all(engine)
    BaseModel.metadata.create_all(engine)


@pytest.yield_fixture()
def mpi(config, mock_corpus):

    """
    Write the current configuration into the /tmp/.hol.yml file.
    """

    config.sync_tmp()

    yield

    os.remove('/tmp/.hol.yml')
