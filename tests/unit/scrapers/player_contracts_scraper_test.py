def test_player_contracts_data(player_contracts_data):
    expected_columns = ["player", "season", "season_salary"]

    assert list(player_contracts_data.columns) == expected_columns
    assert len(player_contracts_data) == 490
    assert player_contracts_data["season_salary"].dtype.name == "int64"
    assert player_contracts_data["season"].nunique() == 1
    assert player_contracts_data["season"].iloc[0] == "2025-26"
    assert not player_contracts_data["player"].duplicated().any()


def test_player_contracts_data_failure(mocker):
    mocker.patch(
        "src.scrapers.pd.read_html",
        side_effect=Exception("contracts page unavailable"),
    )

    from src.scrapers import get_player_contracts_data

    result = get_player_contracts_data()

    assert result.empty
