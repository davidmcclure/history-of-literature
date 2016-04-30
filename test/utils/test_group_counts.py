

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
            1, 2, 3, 4, 5,
            1, 2, 3, 4, 5,
            1, 2, 3, 4, 5,
        ],

        [
            [1, 2, 3],
            [4],
            [5, 1],
            [2, 3],
            [4],
            [5, 1],
            [2, 3],
            [4],
            [5],
        ],

        6,

    ),

])
def test_group_counts(counts, grouped, size):
    assert group_counts(counts, size) == grouped
