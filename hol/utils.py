

from itertools import islice, chain
from collections import OrderedDict


def grouper(iterable, size):

    """
    Yield "groups" from an iterable.

    Args:
        iterable (iter): The iterable.
        size (int): The number of elements in each group.

    Yields:
        The next group.
    """

    source = iter(iterable)

    while True:
        group = islice(source, size)
        yield chain([next(group)], group)


def window(seq, n):

    """
    Yield a sliding window over an iterable.

    Args:
        seq (iter): The sequence.
        n (int): Window width.

    Yields:
        tuple: The next window.
    """

    it = iter(seq)
    result = tuple(islice(it, n))

    if len(result) == n:
        yield result

    for token in it:
        result = result[1:] + (token,)
        yield result


def flatten_dict(d):

    """
    Flatten a dict into a list of tuples.

    Args:
        nested (dict)

    Returns: tuple
    """

    for k, v in d.items():

        if isinstance(v, dict):
            for item in flatten_dict(v):
                yield (k,) + item

        else:
            yield (k, v)


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
