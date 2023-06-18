from datetime import date

from src.utils import get_season_type


def test_get_season_type():
    regular_season = get_season_type(date(2023, 4, 8))
    play_in = get_season_type(date(2023, 4, 14))
    playoffs = get_season_type(date(2023, 4, 21))

    assert regular_season == "Regular Season"
    assert play_in == "Play-In"
    assert playoffs == "Playoffs"
