

from hol.page import Page


def test_add_pos_counts():

    """
    Page#token_counts() should combine POS-specific counts for each token.
    """

    p = Page({
        'body': {
            'tokenPosCount': {
                'aaa': {
                    'POS1': 1,
                    'POS2': 2,
                },
                'bbb': {
                    'POS1': 3,
                    'POS2': 4,
                },
                'ccc': {
                    'POS1': 5,
                    'POS2': 6,
                },
            }
        }
    })

    assert p.token_counts() == {
        'aaa': 3,
        'bbb': 7,
        'ccc': 11,
    }


def test_combine_casing_variants():

    """
    The same tokens with different casing should be combined.
    """

    p = Page({
        'body': {
            'tokenPosCount': {
                'token': {
                    'POS': 1,
                },
                'Token': {
                    'POS': 2,
                },
            }
        }
    })

    assert p.token_counts() == {
        'token': 3,
    }


def test_ignore_irregular_tokens():

    """
    Tokens that aren't [a-zA-Z] should be skipped.
    """

    p = Page({
        'body': {
            'tokenPosCount': {

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
            }
        }
    })

    assert p.token_counts() == {
        'token': 1,
    }
