def test_game_odds_data(odds_data):
    expected_columns = ["team", "spread", "total", "moneyline", "date", "datetime1"]

    assert list(odds_data.columns) == expected_columns
    assert len(odds_data) == 4
