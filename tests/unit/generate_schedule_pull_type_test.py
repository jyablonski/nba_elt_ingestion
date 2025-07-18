import pytest

from src.utils import (
    generate_schedule_pull_type,
)


@pytest.mark.parametrize(
    "season_type, playoff_type, expected",
    [
        # Regular season
        (
            1,
            0,
            [
                "october",
                "november",
                "december",
                "january",
                "february",
                "march",
                "april",
            ],
        ),
        # Playoffs
        (1, 1, ["april", "may", "june"]),
        # Season type disabled
        (0, 0, []),
        (0, 1, []),
        # Edge or unexpected playoff_type values
        (1, 999, ["april", "may", "june"]),
        (1, -1, ["april", "may", "june"]),
    ],
)
def test_generate_schedule_pull_type(season_type, playoff_type, expected):
    assert generate_schedule_pull_type(season_type, playoff_type) == expected
