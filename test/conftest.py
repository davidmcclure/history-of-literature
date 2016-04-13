

import pytest

from hol import config as _config
from test.mock_corpus import MockCorpus


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
    config.config.update_w_merge({
        'corpus': corpus.path
    })

    yield corpus
    corpus.teardown()
