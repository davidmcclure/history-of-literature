

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


def make_vol(counts):

    """
    Make a volume instance with the provided tokenPosCount maps.

    Args:
        counts (list)

    Returns: Volume
    """

    data = {
        'features': {
            'pages': []
        }
    }

    for c in counts:
        data['features']['pages'].append(make_page(c).data)

    return Volume(data)
