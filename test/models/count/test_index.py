

import pytest
import time

from hol.models import Count

from test.helpers import make_vol


pytestmark = pytest.mark.usefixtures('db')


def test_index(mock_corpus, config):

    """
    Count.index() should index per-year token counts.
    """

    for i in range(100):

        vol = make_vol([
            {
                'one': {
                    'POS': 1
                },
                'two': {
                    'POS': 1
                },
                'three': {
                    'POS': 1
                },
            },
        ])

        mock_corpus.add_vol(vol)

    Count.index()

    # TODO
    session = config.Session()
    print(session.query(Count).count())
    assert False
