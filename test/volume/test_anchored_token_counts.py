

from hol.volume import Volume


def test_combine_page_counts():

    """
    Volume#token_counts() should add up page-specific counts.
    """

    v = Volume({
        'features': {
            'pages': [

                {
                    'body': {
                        'tokenPosCount': {

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

                        }
                    }
                },

                {
                    'body': {
                        'tokenPosCount': {

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

                        }
                    }
                },

                {
                    'body': {
                        'tokenPosCount': {

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

                        }
                    }
                },

                {
                    'body': {
                        'tokenPosCount': {

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

                        }
                    }
                },

            ]
        }
    })

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

