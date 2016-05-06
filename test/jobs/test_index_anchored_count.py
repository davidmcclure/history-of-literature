

import pytest

from subprocess import call

from test.helpers import make_page, make_vol
from hol.models import AnchoredCount


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

    v4 = make_vol(year=1904, pages=[
        make_page(counts={

            'anchor': {
                'POS': 4
            },

            'four': {
                'POS': 7
            },
            'five': {
                'POS': 8
            },

        }),
    ])

    v5 = make_vol(year=1905, pages=[
        make_page(counts={

            'anchor': {
                'POS': 5
            },

            'five': {
                'POS': 9
            },
            'six': {
                'POS': 10
            },

        }),
    ])

    v6 = make_vol(year=1906, pages=[
        make_page(counts={

            'anchor': {
                'POS': 6
            },

            'six': {
                'POS': 11
            },
            'seven': {
                'POS': 12
            },

        }),
    ])

    mock_corpus.add_vol(v1)
    mock_corpus.add_vol(v2)
    mock_corpus.add_vol(v3)
    mock_corpus.add_vol(v4)
    mock_corpus.add_vol(v5)
    mock_corpus.add_vol(v6)

    call([
        'mpirun',
        'bin/index_anchored_count',
        'anchor',
        '--group_size=2',
    ])

    assert AnchoredCount.token_year_level_count('one',      1901, 1) == 1
    assert AnchoredCount.token_year_level_count('two',      1901, 1) == 2
    assert AnchoredCount.token_year_level_count('two',      1902, 2) == 3
    assert AnchoredCount.token_year_level_count('three',    1902, 2) == 4
    assert AnchoredCount.token_year_level_count('three',    1903, 3) == 5
    assert AnchoredCount.token_year_level_count('four',     1903, 3) == 6
    assert AnchoredCount.token_year_level_count('four',     1904, 4) == 7
    assert AnchoredCount.token_year_level_count('five',     1904, 4) == 8
    assert AnchoredCount.token_year_level_count('five',     1905, 5) == 9
    assert AnchoredCount.token_year_level_count('six',      1905, 5) == 10
    assert AnchoredCount.token_year_level_count('six',      1906, 6) == 11
    assert AnchoredCount.token_year_level_count('seven',    1906, 6) == 12


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


def test_ignore_non_english_volumes(mock_corpus):

    """
    Non-English volumes should be skipped.
    """

    v1 = make_vol(year=1900, pages=[
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

    v2 = make_vol(year=1900, language='ger', pages=[
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

    assert AnchoredCount.token_year_level_count('one', 1900, 1) == 1
    assert AnchoredCount.token_year_level_count('two', 1900, 1) == 2
