

import pytest

from hol.utils import group_counts


@pytest.mark.parametrize('counts,grouped,size', [

    (

        [
            10, 10, 10,
            10, 10, 10,
            10, 10, 10,
        ],

        [
            [10, 10, 10],
            [10, 10, 10],
            [10, 10, 10],
        ],

        30,

    ),

    (

        [
            10, 10, 10, 0,
            10, 10, 10, 0,
            10, 10, 10, 0,
        ],

        [
            [10, 10, 10, 0],
            [10, 10, 10, 0],
            [10, 10, 10, 0],
        ],

        30,

    ),

    (

        [
            1, 2, 3,
            1, 2, 3,
            1, 2, 3,
        ],

        [
            [1, 2],
            [3],
            [1, 2],
            [3],
            [1, 2],
            [3],
        ],

        3,

    ),

    (

        [
            2,
            2,
            2,
            2,
            2,
            2,
        ],

        [
            [2, 2], # 4
            [2],    # 3
            [2, 2], # 3.33
            [2],    # 3
        ],

        3,

    ),

])
def test_group_counts(counts, grouped, size):
    assert group_counts(counts, size) == grouped
