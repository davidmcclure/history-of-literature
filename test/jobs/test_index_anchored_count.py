

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

            'a': {
                'POS': 1
            },
            'b': {
                'POS': 2
            },

        }),
    ])

    v2 = make_vol(year=1902, pages=[
        make_page(counts={

            'anchor': {
                'POS': 2
            },

            'b': {
                'POS': 3
            },
            'c': {
                'POS': 4
            },

        }),
    ])

    v3 = make_vol(year=1903, pages=[
        make_page(counts={

            'anchor': {
                'POS': 3
            },

            'c': {
                'POS': 5
            },
            'd': {
                'POS': 6
            },

        }),
    ])

    v4 = make_vol(year=1904, pages=[
        make_page(counts={

            'anchor': {
                'POS': 4
            },

            'd': {
                'POS': 7
            },
            'e': {
                'POS': 8
            },

        }),
    ])

    v5 = make_vol(year=1905, pages=[
        make_page(counts={

            'anchor': {
                'POS': 5
            },

            'e': {
                'POS': 9
            },
            'f': {
                'POS': 10
            },

        }),
    ])

    v6 = make_vol(year=1906, pages=[
        make_page(counts={

            'anchor': {
                'POS': 6
            },

            'f': {
                'POS': 11
            },
            'g': {
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

    assert AnchoredCount.token_year_level_count('a', 1901, 1) == 1
    assert AnchoredCount.token_year_level_count('b', 1901, 1) == 2
    assert AnchoredCount.token_year_level_count('b', 1902, 2) == 3
    assert AnchoredCount.token_year_level_count('c', 1902, 2) == 4
    assert AnchoredCount.token_year_level_count('c', 1903, 3) == 5
    assert AnchoredCount.token_year_level_count('d', 1903, 3) == 6
    assert AnchoredCount.token_year_level_count('d', 1904, 4) == 7
    assert AnchoredCount.token_year_level_count('e', 1904, 4) == 8
    assert AnchoredCount.token_year_level_count('e', 1905, 5) == 9
    assert AnchoredCount.token_year_level_count('f', 1905, 5) == 10
    assert AnchoredCount.token_year_level_count('f', 1906, 6) == 11
    assert AnchoredCount.token_year_level_count('g', 1906, 6) == 12


def test_merge_year_level_counts(mock_corpus):

    """
    Token counts for the same year/level should be merged.
    """

    v1 = make_vol(year=1901, pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'a': {
                'POS': 1
            },
            'b': {
                'POS': 2
            },

        }),
    ])

    v2 = make_vol(year=1901, pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'a': {
                'POS': 11
            },
            'b': {
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

    assert AnchoredCount.token_year_level_count('a', 1901, 1) == 1+11
    assert AnchoredCount.token_year_level_count('b', 1901, 1) == 2+12


def test_ignore_non_english_volumes(mock_corpus):

    """
    Non-English volumes should be skipped.
    """

    v1 = make_vol(year=1900, pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'a': {
                'POS': 1
            },
            'b': {
                'POS': 2
            },

        }),
    ])

    v2 = make_vol(year=1900, language='ger', pages=[
        make_page(counts={

            'anchor': {
                'POS': 1
            },

            'a': {
                'POS': 11
            },
            'b': {
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

    assert AnchoredCount.token_year_level_count('a', 1900, 1) == 1
    assert AnchoredCount.token_year_level_count('b', 1900, 1) == 2


def test_combine_counts_for_grouped_pages(mock_corpus):

    """
    The --page_size option should control the page grouping.
    """

    vol = make_vol(year=1900, pages=[

        make_page(token_count=100, counts={
            'anchor': {
                'POS': 1,
            },
            'a': {
                'POS': 1,
            },
        }),

        make_page(token_count=100, counts={
            'anchor': {
                'POS': 2,
            },
            'a': {
                'POS': 2,
            },
        }),

        make_page(token_count=100, counts={
            'anchor': {
                'POS': 3,
            },
            'a': {
                'POS': 3,
            },
        }),

        make_page(token_count=100, counts={
            'anchor': {
                'POS': 4,
            },
            'a': {
                'POS': 4,
            },
        }),

    ])

    mock_corpus.add_vol(vol)

    call([
        'mpirun',
        'bin/index_anchored_count',
        'anchor',
        '--page_size=200',
    ])

    assert AnchoredCount.token_year_level_count('a', 1900, 1+2) == 1+2
    assert AnchoredCount.token_year_level_count('a', 1900, 3+4) == 3+4
