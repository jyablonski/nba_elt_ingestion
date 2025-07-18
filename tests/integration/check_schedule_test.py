import pytest
from pytest_mock import MockerFixture

from src.utils import check_schedule


@pytest.mark.parametrize(
    "date, mock_input, expected",
    [
        (
            "2025-01-19",
            [
                {
                    "game_date": "2025-01-19",
                    "day_name": "Sunday",
                    "start_time": "3:00 PM",
                    "avg_team_rank": 18,
                    "home_team": "Miami Heat",
                    "home_moneyline_raw": None,
                    "away_team": "San Antonio Spurs",
                    "away_moneyline_raw": None,
                },
                {
                    "game_date": "2025-01-19",
                    "day_name": "Sunday",
                    "start_time": "6:00 PM",
                    "avg_team_rank": 9,
                    "home_team": "Orlando Magic",
                    "home_moneyline_raw": None,
                    "away_team": "Denver Nuggets",
                    "away_moneyline_raw": None,
                },
            ],
            True,
        ),
        ("2024-12-24", [], False),
    ],
)
def test_check_schedule(mocker: MockerFixture, date, mock_input, expected):
    mocker.patch("src.utils.requests.get").return_value.json.return_value = mock_input
    is_games_played = check_schedule(date=date)
    assert is_games_played == expected
