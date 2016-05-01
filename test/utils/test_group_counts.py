

import random

from hol.utils import group_counts


def test_group_counts():

    counts = [random.randint(1, 5) for _ in range(1000)]

    groups = group_counts(counts, 10)

    mean = sum(map(sum, groups)) / len(groups)

    assert abs(mean - 10) < 0.1
