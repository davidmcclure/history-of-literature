

import uuid

from hol.page import Page
from hol.volume import Volume


def make_page(counts):

    """
    Make a page instance with the provided tokenPosCount map.

    Args:
        counts (dict)

    Returns: Page
    """

    return Page({
        'body': {
            'tokenPosCount': counts
        }
    })


def make_vol(counts, id=None, year=1900, language='eng'):

    """
    Make a volume instance with the provided tokenPosCount maps.

    Args:
        counts (list)

    Returns: Volume
    """

    if not id:
        id = str(uuid.uuid4())

    data = {

        'id': id,

        'metadata': {
            'pubDate': str(year),
            'language': language,
        },

        'features': {
            'pages': []
        },

    }

    for c in counts:
        data['features']['pages'].append(make_page(c).data)

    return Volume(data)
