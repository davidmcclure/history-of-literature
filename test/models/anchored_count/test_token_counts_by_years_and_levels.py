

import pytest

from hol.models import AnchoredCount


pytestmark = pytest.mark.usefixtures('db')


def test_filter_by_year(config):

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

    counts = AnchoredCount.token_counts_by_years_and_levels()
    assert counts['token'] == 2+4+8

    # Start year.
    counts = AnchoredCount.token_counts_by_years_and_levels(y1=1910)
    assert counts['token'] == 4+8

    # End year.
    counts = AnchoredCount.token_counts_by_years_and_levels(y2=1910)
    assert counts['token'] == 2+4

    # Start + end year.
    counts = AnchoredCount.token_counts_by_years_and_levels(y1=1909, y2=1911)
    assert counts['token'] == 4
