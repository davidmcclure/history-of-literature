

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

    def _group(groups, c):

        if not groups:
            groups.append([])

        s0 = sum(groups[-1])
        s1 = sum(groups[-1] + [c])

        # If the new count gets the group closer to the target size, add it to
        # the running group.

        if abs(s1-size) <= abs(s0-size):
            groups[-1].append(c)

        # Otherwise, start a new group with the count.

        else:
            groups.append([c])

        return groups

    return reduce(_group, counts, [])
