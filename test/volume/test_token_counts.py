

from hol.volume import Volume
from test.helpers import make_vol


def test_combine_page_counts():

    """
    Volume#token_counts() should add up page-specific counts.
    """

    v = make_vol([

        {
            'aaa': {
                'POS': 1,
            },
            'bbb': {
                'POS': 2,
            },
            'ccc': {
                'POS': 3,
            },
        },

        {
            'bbb': {
                'POS': 4,
            },
            'ccc': {
                'POS': 5,
            },
            'ddd': {
                'POS': 6,
            },
        },

    ])

    assert v.token_counts() == {
        'aaa': 1,
        'bbb': 2+4,
        'ccc': 3+5,
        'ddd': 6,
    }
