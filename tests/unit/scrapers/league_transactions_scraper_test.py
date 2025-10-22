def test_league_transactions_data(transactions_data):
    expected_columns = ["date", "transaction", "scrape_date"]

    assert list(transactions_data.columns) == expected_columns
    assert len(transactions_data) == 662
