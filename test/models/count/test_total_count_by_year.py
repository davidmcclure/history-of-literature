

import pytest

from hol.models import Count


pytestmark = pytest.mark.usefixtures('db')


@pytest.mark.parametrize('year1,year2,count', [

    # No filter
    (None, None, 2+4+8+16),

    # Start year
    (1910, None, 4+8+16),

    # End year
    (None, 1915, 2+4+8),

    # Individual year
    (1910, 1910, 4),

    # Range.
    (1910, 1915, 4+8),

])
def test_filter_by_year(year1, year2, count, config):

    with config.get_session() as session:

        session.add(Count(
            token='token',
            year=1905,
            count=2,
        ))

        session.add(Count(
            token='token',
            year=1910,
            count=4,
        ))

        session.add(Count(
            token='token',
            year=1915,
            count=8,
        ))

        session.add(Count(
            token='token',
            year=1920,
            count=16,
        ))

    res = Count.total_count_by_year(
        year1=year1,
        year2=year2,
    )

    assert res == count
