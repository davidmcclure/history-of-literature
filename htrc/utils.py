

import os

from collections import OrderedDict


def ensure_dir(path):

    """
    Ensure that a file path's directory exists.

    Args:
        path (str)
    """

    os.makedirs(
        os.path.dirname(path),
        exist_ok=True,
    )


def sort_dict(d, desc=True):

    """
    Sort an ordered dictionary by value, descending.

    Args:
        d (OrderedDict): An ordered dictionary.
        desc (bool): If true, sort desc.

    Returns:
        OrderedDict: The sorted dictionary.
    """

    sort = sorted(d.items(), key=lambda x: x[1], reverse=desc)
    return OrderedDict(sort)
