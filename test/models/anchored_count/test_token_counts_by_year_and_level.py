

import pytest

from hol.models import AnchoredCount


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

        session.add(AnchoredCount(
            token='token',
            year=1905,
            anchor_count=1,
            count=2,
        ))

        session.add(AnchoredCount(
            token='token',
            year=1910,
            anchor_count=2,
            count=4,
        ))

        session.add(AnchoredCount(
            token='token',
            year=1915,
            anchor_count=3,
            count=8,
        ))

        session.add(AnchoredCount(
            token='token',
            year=1920,
            anchor_count=4,
            count=16,
        ))

    counts = AnchoredCount.token_counts_by_year_and_level(
        year1=year1,
        year2=year2,
    )

    assert counts['token'] == count


@pytest.mark.parametrize('level1,level2,count', [

    # No filter
    (None, None, 2+4+8+16),

    # Start level
    (3, None, 4+8+16),

    # End level
    (None, 5, 2+4+8),

    # Individual level
    (3, 3, 4),

    # Range
    (3, 5, 4+8),

])
def test_filter_by_level(level1, level2, count, config):

    with config.get_session() as session:

        session.add(AnchoredCount(
            token='token',
            year=1900,
            anchor_count=1,
            count=2,
        ))

        session.add(AnchoredCount(
            token='token',
            year=1900,
            anchor_count=3,
            count=4,
        ))

        session.add(AnchoredCount(
            token='token',
            year=1900,
            anchor_count=5,
            count=8,
        ))

        session.add(AnchoredCount(
            token='token',
            year=1900,
            anchor_count=7,
            count=16,
        ))

    counts = AnchoredCount.token_counts_by_year_and_level(
        level1=level1,
        level2=level2,
    )

    assert counts['token'] == count
