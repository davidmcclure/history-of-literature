

import numpy as np

from itertools import islice, chain
from sklearn import preprocessing
from collections import OrderedDict
from functools import reduce


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


def group_counts(counts, size=1000):

    """
    Given a set of integer counts, group them together into buckets so that the
    counts in each bucket add up to roughly a given size.

    Args:
        counts (list<int>)
        size (int)
    """

    groups = [[]]

    locked = 0

    for c in counts:

        s0 = sum(groups[-1])
        s1 = sum(groups[-1] + [c])

        # Current group mean.
        m0 = (locked + s0) / len(groups)

        # Mean if new count is added to the running group.
        m1 = (locked + s1) / len(groups)

        # If adding the new count to the running group gets the average group
        # size closer to the target, add it to the group.

        if abs(m1-size) <= abs(m0-size):
            groups[-1].append(c)

        # Otherwise, start a new group with the count.

        else:
            groups.append([c])
            locked += s0

    return groups
