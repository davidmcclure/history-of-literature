

from hol.volume import Volume
from test.helpers import make_page, make_vol


def test_anchored_token_counts():

    """
    Volume#anchored_token_counts() should add up token counts for pages where
    an "anchor" token appears, bucketed by the anchor token count.
    """

    v = make_vol(pages=[

        make_page(counts={

            'literature': {
                'POS': 1,
            },

            'aaa': {
                'POS': 1,
            },
            'bbb': {
                'POS': 1,
            },
            'ccc': {
                'POS': 1,
            },

        }),

        make_page(counts={

            'literature': {
                'POS': 2,
            },

            'aaa': {
                'POS': 2,
            },
            'bbb': {
                'POS': 2,
            },
            'ccc': {
                'POS': 2,
            },

        }),

        make_page(counts={

            'literature': {
                'POS': 2,
            },

            'aaa': {
                'POS': 3,
            },
            'bbb': {
                'POS': 3,
            },
            'ccc': {
                'POS': 3,
            },

        }),

        make_page(counts={

            'literature': {
                'POS': 3,
            },

            'aaa': {
                'POS': 4,
            },
            'bbb': {
                'POS': 4,
            },
            'ccc': {
                'POS': 4,
            },

        }),

    ])

    assert v.anchored_token_counts('literature') == {
        1: {
            'aaa': 1,
            'bbb': 1,
            'ccc': 1,
        },
        2: {
            'aaa': 2+3,
            'bbb': 2+3,
            'ccc': 2+3,
        },
        3: {
            'aaa': 4,
            'bbb': 4,
            'ccc': 4,
        }
    }


def test_ignore_pages_without_anchor_token():

    """
    Ignore tokens on pages that don't contain the anchor token.
    """

    v = make_vol(pages=[

        make_page(counts={

            'literature': {
                'POS': 1,
            },

            'aaa': {
                'POS': 1,
            },
            'bbb': {
                'POS': 1,
            },
            'ccc': {
                'POS': 1,
            },

        }),

        # No anchor token.
        make_page(counts={
            'aaa': {
                'POS': 2,
            },
            'bbb': {
                'POS': 2,
            },
            'ccc': {
                'POS': 2,
            },
        }),

    ])

    assert v.anchored_token_counts('literature') == {
        1: {
            'aaa': 1,
            'bbb': 1,
            'ccc': 1,
        },
    }
