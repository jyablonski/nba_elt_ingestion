def test_player_pbp_data(pbp_transformed_data):
    expected_columns = [
        "timequarter",
        "descriptionplayvisitor",
        "awayscore",
        "score",
        "homescore",
        "descriptionplayhome",
        "numberperiod",
        "hometeam",
        "awayteam",
        "scoreaway",
        "scorehome",
        "marginscore",
        "date",
        "scrape_date",
    ]

    assert list(pbp_transformed_data.columns) == expected_columns
    assert len(pbp_transformed_data) == 100
