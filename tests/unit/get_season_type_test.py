from datetime import date

from src.utils import get_season_type


def test_get_season_type():
    regular_season = get_season_type(date(2025, 4, 8))
    play_in = get_season_type(date(2025, 4, 17))
    playoffs = get_season_type(date(2025, 4, 23))

    assert regular_season == "Regular Season"
    assert play_in == "Play-In"
    assert playoffs == "Playoffs"
