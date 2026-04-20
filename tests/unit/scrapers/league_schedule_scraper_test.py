from urllib.error import HTTPError

import pandas as pd

from src.scrapers import get_schedule_data


def test_league_schedule_data(schedule_data):
    expected_columns = [
        "date",
        "start_time",
        "away_team",
        "home_team",
        "proper_date",
    ]

    assert list(schedule_data.columns) == expected_columns
    assert len(schedule_data) == 0


def test_league_schedule_skips_missing_month_page(mocker):
    schedule_month = pd.DataFrame(
        {
            "Date": ["Mon, Apr 20, 2026"],
            "Start (ET)": ["7:00p"],
            "Visitor/Neutral": ["Boston Celtics"],
            "Home/Neutral": ["New York Knicks"],
        }
    )
    missing_month = HTTPError(
        url="https://www.basketball-reference.com/leagues/NBA_2026_games-june.html",
        code=404,
        msg="Not Found",
        hdrs=None,
        fp=None,
    )
    mocker.patch(
        "src.scrapers.pd.read_html",
        side_effect=[[schedule_month], missing_month],
    )

    schedule = get_schedule_data(
        month_list=["april", "june"],
        include_past_games=True,
    )

    assert len(schedule) == 1
    assert schedule.iloc[0]["away_team"] == "Boston Celtics"
