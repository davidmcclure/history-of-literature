

import numpy as np

from itertools import islice, chain
from collections import OrderedDict
from sklearn import preprocessing


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


def scale_01(seq):

    """
    Scale a list from 0-1.

    Args:
        d (list)

    Returns: np.array
    """

    scaler = preprocessing.MinMaxScaler()

    data = np.array([[float(s)] for s in seq])

    return scaler.fit_transform(data)
