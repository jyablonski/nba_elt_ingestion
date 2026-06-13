import pandas as pd

from src.database import filter_unchanged_rows, write_to_sql


def test_write_to_sql_skips_empty_dataframe(mocker):
    mock_con = mocker.MagicMock()
    to_sql = mocker.patch("src.database.pd.DataFrame.to_sql")

    write_to_sql(mock_con, "bbref_player_contracts", pd.DataFrame(), "append")

    to_sql.assert_not_called()


def test_write_to_sql_logs_error_on_failure(mocker):
    mock_con = mocker.MagicMock()
    mocker.patch(
        "src.database.pd.DataFrame.to_sql",
        side_effect=Exception("write failed"),
    )

    write_to_sql(
        mock_con,
        "bbref_player_contracts",
        pd.DataFrame({"player": ["Test Player"]}),
        "append",
    )


def test_filter_unchanged_rows_returns_empty_input(mocker):
    mock_conn = mocker.MagicMock()

    result = filter_unchanged_rows(
        conn=mock_conn,
        schema="bronze",
        table="bbref_player_contracts",
        df=pd.DataFrame(),
        primary_keys=["player", "season"],
        compare_columns=["season_salary"],
    )

    assert result.empty


def test_filter_unchanged_rows_returns_all_rows_when_table_empty(mocker):
    mock_conn = mocker.MagicMock()
    mocker.patch("src.database.pd.read_sql", return_value=pd.DataFrame())
    df = pd.DataFrame(
        {
            "player": ["Stephen Curry"],
            "season": ["2025-26"],
            "season_salary": [59606817],
        }
    )

    result = filter_unchanged_rows(
        conn=mock_conn,
        schema="bronze",
        table="bbref_player_contracts",
        df=df,
        primary_keys=["player", "season"],
        compare_columns=["season_salary"],
    )

    pd.testing.assert_frame_equal(result, df)


def test_filter_unchanged_rows_returns_only_changed_rows(mocker):
    mock_conn = mocker.MagicMock()
    mocker.patch(
        "src.database.pd.read_sql",
        return_value=pd.DataFrame(
            {
                "player": ["Stephen Curry", "Joel Embiid"],
                "season": ["2025-26", "2025-26"],
                "season_salary": [59606817, 55224526],
            }
        ),
    )
    df = pd.DataFrame(
        {
            "player": ["Stephen Curry", "Joel Embiid", "Kevin Durant"],
            "season": ["2025-26", "2025-26", "2025-26"],
            "season_salary": [60000000, 55224526, 54708609],
        }
    )

    result = filter_unchanged_rows(
        conn=mock_conn,
        schema="bronze",
        table="bbref_player_contracts",
        df=df,
        primary_keys=["player", "season"],
        compare_columns=["season_salary"],
    )

    assert len(result) == 2
    assert set(result["player"]) == {"Stephen Curry", "Kevin Durant"}
