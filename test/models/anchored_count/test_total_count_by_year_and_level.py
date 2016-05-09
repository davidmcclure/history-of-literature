

import pytest

from hol.models import AnchoredCount


pytestmark = pytest.mark.usefixtures('db')


@pytest.mark.parametrize('year1,year2,count', [

    # No filter
    (None, None, (2+4)+(8+16)+(32+64)+(128+256)),

    # Start year
    (1910, None, (8+16)+(32+64)+(128+256)),

    # End year
    (None, 1915, (2+4)+(8+16)+(32+64)),

    # Individual year
    (1910, 1910, (8+16)),

    # Range
    (1910, 1915, (8+16)+(32+64)),

])
def test_filter_by_year(year1, year2, count, config):

    with config.get_session() as session:

        session.add(AnchoredCount(
            token='token1',
            year=1905,
            anchor_count=1,
            count=2,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1905,
            anchor_count=1,
            count=4,
        ))

        session.add(AnchoredCount(
            token='token1',
            year=1910,
            anchor_count=1,
            count=8,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1910,
            anchor_count=1,
            count=16,
        ))

        session.add(AnchoredCount(
            token='token1',
            year=1915,
            anchor_count=1,
            count=32,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1915,
            anchor_count=1,
            count=64,
        ))

        session.add(AnchoredCount(
            token='token1',
            year=1920,
            anchor_count=1,
            count=128,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1920,
            anchor_count=1,
            count=256,
        ))


    res = AnchoredCount.total_count_by_year_and_level(
        year1=year1,
        year2=year2,
    )

    assert res == count


@pytest.mark.parametrize('level1,level2,count', [

    # No filter
    (None, None, (2+4)+(8+16)+(32+64)+(128+256)),

    # Start level
    (3, None, (8+16)+(32+64)+(128+256)),

    # End level
    (None, 5, (2+4)+(8+16)+(32+64)),

    # Individual level
    (3, 3, (8+16)),

    # Range
    (3, 5, (8+16)+(32+64)),

])
def test_filter_by_year(level1, level2, count, config):

    with config.get_session() as session:

        session.add(AnchoredCount(
            token='token1',
            year=1900,
            anchor_count=1,
            count=2,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1900,
            anchor_count=1,
            count=4,
        ))

        session.add(AnchoredCount(
            token='token1',
            year=1900,
            anchor_count=3,
            count=8,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1900,
            anchor_count=3,
            count=16,
        ))

        session.add(AnchoredCount(
            token='token1',
            year=1900,
            anchor_count=5,
            count=32,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1900,
            anchor_count=5,
            count=64,
        ))

        session.add(AnchoredCount(
            token='token1',
            year=1900,
            anchor_count=7,
            count=128,
        ))

        session.add(AnchoredCount(
            token='token2',
            year=1900,
            anchor_count=7,
            count=256,
        ))


    res = AnchoredCount.total_count_by_year_and_level(
        level1=level1,
        level2=level2,
    )

    assert res == count
