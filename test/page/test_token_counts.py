

from hol.page import Page
from test.helpers import make_page


def test_add_pos_counts():

    """
    Page#token_counts() should combine POS-specific counts for each token.
    """

    p = make_page({
        'a': {
            'POS1': 1,
            'POS2': 2,
        },
        'b': {
            'POS1': 3,
            'POS2': 4,
        },
        'c': {
            'POS1': 5,
            'POS2': 6,
        },
    })

    assert p.token_counts() == {
        'a': 1+2,
        'b': 3+4,
        'c': 5+6,
    }


def test_combine_casing_variants():

    """
    The same tokens with different casing should be combined.
    """

    p = make_page({
        'token': {
            'POS': 1,
        },
        'Token': {
            'POS': 2,
        },
    })

    assert p.token_counts() == {
        'token': 1+2,
    }


def test_ignore_irregular_tokens():

    """
    Tokens that aren't [a-zA-Z] should be skipped.
    """

    p = make_page({

        'token': {
            'POS': 1,
        },

        # Number
        'token1': {
            'POS': 1,
        },

        # Punctuation
        '...': {
            'POS': 1,
        },

    })

    assert p.token_counts() == {
        'token': 1,
    }


def test_apply_token_whitelist():

    """
    Non-whitelisted tokens should be ignored.
    """

    p = make_page({

        'token': {
            'POS': 1,
        },

        # Not on whitelist
        'zxcvb': {
            'POS': 1,
        },

    })

    assert p.token_counts() == {
        'token': 1,
    }
