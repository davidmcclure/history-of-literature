

from hol.volume import Volume
from test.helpers import make_page, make_vol


def test_anchored_token_counts():

    """
    Volume#anchored_token_counts() should add up token counts for pages where
    an "anchor" token appears, bucketed by the anchor token count.
    """

    v = make_vol(pages=[

        make_page(token_count=100, counts={

            'anchor': {
                'POS': 1,
            },

            'a': {
                'POS': 1,
            },
            'b': {
                'POS': 1,
            },
            'c': {
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
            'b': {
                'POS': 2,
            },
            'c': {
                'POS': 2,
            },

        }),

        make_page(token_count=100, counts={

            'anchor': {
                'POS': 2,
            },

            'a': {
                'POS': 3,
            },
            'b': {
                'POS': 3,
            },
            'c': {
                'POS': 3,
            },

        }),

        make_page(token_count=100, counts={

            'anchor': {
                'POS': 3,
            },

            'a': {
                'POS': 4,
            },
            'b': {
                'POS': 4,
            },
            'c': {
                'POS': 4,
            },

        }),

    ])

    assert v.anchored_token_counts('anchor', 100) == {
        1: {
            'a': 1,
            'b': 1,
            'c': 1,
        },
        2: {
            'a': 2+3,
            'b': 2+3,
            'c': 2+3,
        },
        3: {
            'a': 4,
            'b': 4,
            'c': 4,
        }
    }


def test_ignore_pages_without_anchor_token():

    """
    Ignore tokens on pages that don't contain the anchor token.
    """

    v = make_vol(pages=[

        make_page(token_count=100, counts={

            'anchor': {
                'POS': 1,
            },

            'a': {
                'POS': 1,
            },
            'b': {
                'POS': 1,
            },
            'c': {
                'POS': 1,
            },

        }),

        # No anchor token.
        make_page(token_count=100, counts={
            'a': {
                'POS': 2,
            },
            'b': {
                'POS': 2,
            },
            'c': {
                'POS': 2,
            },
        }),

    ])

    assert v.anchored_token_counts('anchor', 100) == {
        1: {
            'a': 1,
            'b': 1,
            'c': 1,
        },
    }


def test_combine_counts_for_grouped_pages():

    """
    When pages are grouped together in order to hit the requested token count
    average, the word counts should be merged.
    """

    v = make_vol(pages=[

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

    assert v.anchored_token_counts('anchor', 200) == {
        1+2: {
            'a': 1+2,
        },
        3+4: {
            'a': 3+4,
        },
    }
