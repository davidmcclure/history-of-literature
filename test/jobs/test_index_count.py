

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

    call(['mpirun', 'bin/index_count', '--group_size=2'])

    assert Count.token_year_count('a',    1901) == 1
    assert Count.token_year_count('b',    1901) == 2
    assert Count.token_year_count('b',    1902) == 3
    assert Count.token_year_count('c',  1902) == 4
    assert Count.token_year_count('c',  1903) == 5
    assert Count.token_year_count('d',   1903) == 6
    assert Count.token_year_count('d',   1904) == 7
    assert Count.token_year_count('e',   1904) == 8
    assert Count.token_year_count('e',   1905) == 9
    assert Count.token_year_count('f',    1905) == 10
    assert Count.token_year_count('f',    1906) == 11
    assert Count.token_year_count('g',  1906) == 12


def test_merge_year_counts(mock_corpus):

    """
    Token counts for the same years should be merged.
    """

    v1 = make_vol(year=1901, pages=[
        make_page(counts={
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

    call(['mpirun', 'bin/index_count'])

    assert Count.token_year_count('a', 1901) == 1+11
    assert Count.token_year_count('b', 1901) == 2+12


def test_ignore_non_english_volumes(mock_corpus):

    """
    Non-English volumes should be skipped.
    """

    v1 = make_vol(year=1900, pages=[
        make_page(counts={
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

    call(['mpirun', 'bin/index_count'])

    assert Count.token_year_count('a', 1900) == 1
    assert Count.token_year_count('b', 1900) == 2
