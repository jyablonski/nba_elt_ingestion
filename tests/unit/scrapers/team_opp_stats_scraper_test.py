def test_team_opp_stats_data(opp_stats_data):
    expected_columns = [
        "team",
        "fg_percent_opp",
        "threep_percent_opp",
        "threep_made_opp",
        "ppg_opp",
        "scrape_date",
    ]

    assert list(opp_stats_data.columns) == expected_columns
    assert len(opp_stats_data) == 30
