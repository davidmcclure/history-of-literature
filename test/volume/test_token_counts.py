

from hol.volume import Volume
from test.helpers import make_page, make_vol


def test_combine_page_counts():

    """
    Volume#token_counts() should add up page-specific counts.
    """

    v = make_vol(pages=[

        make_page(counts={
            'a': {
                'POS': 1,
            },
            'b': {
                'POS': 2,
            },
            'c': {
                'POS': 3,
            },
        }),

        make_page(counts={
            'b': {
                'POS': 4,
            },
            'c': {
                'POS': 5,
            },
            'd': {
                'POS': 6,
            },
        }),

    ])

    assert v.token_counts() == {
        'a': 1,
        'b': 2+4,
        'c': 3+5,
        'd': 6,
    }
