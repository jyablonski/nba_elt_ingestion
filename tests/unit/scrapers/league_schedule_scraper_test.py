def test_league_schedule_data(schedule_data):
    expected_columns = [
        "start_time",
        "away_team",
        "home_team",
        "date",
        "proper_date",
    ]

    assert list(schedule_data.columns) == expected_columns
    assert len(schedule_data) == 0
