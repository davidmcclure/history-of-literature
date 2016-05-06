

import pytest

from subprocess import call

from test.helpers import make_page, make_vol
from hol.models import Count


pytestmark = pytest.mark.usefixtures('db', 'mpi')


def test_index_year_token_counts(mock_corpus):

    """
    Count.index() should index per-year token counts.
    """

    v1 = make_vol(year=1901, pages=[
        make_page(counts={
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

    call(['mpirun', 'bin/index_count', '--group_size=2'])

    assert Count.token_year_count('one',    1901) == 1
    assert Count.token_year_count('two',    1901) == 2
    assert Count.token_year_count('two',    1902) == 3
    assert Count.token_year_count('three',  1902) == 4
    assert Count.token_year_count('three',  1903) == 5
    assert Count.token_year_count('four',   1903) == 6
    assert Count.token_year_count('four',   1904) == 7
    assert Count.token_year_count('five',   1904) == 8
    assert Count.token_year_count('five',   1905) == 9
    assert Count.token_year_count('six',    1905) == 10
    assert Count.token_year_count('six',    1906) == 11
    assert Count.token_year_count('seven',  1906) == 12


def test_merge_year_counts(mock_corpus):

    """
    Token counts for the same years should be merged.
    """

    v1 = make_vol(year=1901, pages=[
        make_page(counts={
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

    call(['mpirun', 'bin/index_count'])

    assert Count.token_year_count('one', 1901) == 1+11
    assert Count.token_year_count('two', 1901) == 2+12
