

import pytest


pytestmark = pytest.mark.usefixtures('db')


def test_index(mock_corpus):

    """
    Count.index() should index per-year token counts.
    """

    pass
