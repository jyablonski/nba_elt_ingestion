def test_player_injuries_data(injuries_data):
    expected_columns = [
        "player",
        "team",
        "date",
        "description",
        "scrape_date",
    ]

    assert list(injuries_data.columns) == expected_columns
    assert len(injuries_data) == 17
