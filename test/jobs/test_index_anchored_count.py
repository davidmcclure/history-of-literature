

import pytest

from subprocess import call

from hol.models import AnchoredCount
from hol.jobs import index_anchored_count

from test.helpers import make_page, make_vol


pytestmark = pytest.mark.usefixtures('db', 'mpi')


def test_index_year_token_counts(mock_corpus):

    """
    AnchoredCount.index() should index per-year counts for tokens that appear
    on the same page with the anchor token, bucketed by anchor token count.
    """

    v1 = make_vol(year=1901, pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'one': {
                'POS': 1
            },
            'two': {
                'POS': 2
            },

        }),
    ])

    v2 = make_vol(year=1902, pages=[
        make_page(counts={

            'anchor': {
                'POS': 2
            },

            'two': {
                'POS': 3
            },
            'three': {
                'POS': 4
            },

        }),
    ])

    v3 = make_vol(year=1903, pages=[
        make_page(counts={

            'anchor': {
                'POS': 3
            },

            'three': {
                'POS': 5
            },
            'four': {
                'POS': 6
            },

        }),
    ])

    mock_corpus.add_vol(v1)
    mock_corpus.add_vol(v2)
    mock_corpus.add_vol(v3)

    call([
        'mpirun',
        'bin/index_anchored_count',
        'anchor',
        '--group_size=2',
    ])

    assert AnchoredCount.token_year_level_count('one',   1901, 1) == 1
    assert AnchoredCount.token_year_level_count('two',   1901, 1) == 2
    assert AnchoredCount.token_year_level_count('two',   1902, 2) == 3
    assert AnchoredCount.token_year_level_count('three', 1902, 2) == 4
    assert AnchoredCount.token_year_level_count('three', 1903, 3) == 5
    assert AnchoredCount.token_year_level_count('four',  1903, 3) == 6


def test_merge_year_level_counts(mock_corpus):

    """
    Token counts for the same year/level should be merged.
    """

    v1 = make_vol(year=1901, pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'one': {
                'POS': 1
            },
            'two': {
                'POS': 2
            },

        }),
    ])

    v2 = make_vol(year=1901, pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'one': {
                'POS': 11
            },
            'two': {
                'POS': 12
            },

        }),
    ])

    mock_corpus.add_vol(v1)
    mock_corpus.add_vol(v2)

    call([
        'mpirun',
        'bin/index_anchored_count',
        'anchor',
    ])

    assert AnchoredCount.token_year_level_count('one', 1901, 1) == 1+11
    assert AnchoredCount.token_year_level_count('two', 1901, 1) == 2+12
