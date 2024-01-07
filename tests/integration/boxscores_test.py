import logging
import os

import pandas as pd

from src.utils import get_boxscores_data, write_to_sql_upsert


def test_get_boxscores_data_no_data_available(
    mocker, get_feature_flags_postgres, schedule_mock_data, caplog
):
    fname = os.path.join(
        os.path.dirname(__file__), "../fixtures/boxscores_no_data.html"
    )
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mock_get = mocker.patch("src.utils.requests.get")
    mock_get.return_value.content = mock_content
    mock_json = mocker.MagicMock()
    mock_get.return_value.json = mock_json
    mock_json.return_value = schedule_mock_data

    # Clear existing logs and enable log capturing
    caplog.clear()
    caplog.set_level(logging.INFO)

    boxscores = get_boxscores_data(
        feature_flags_df=get_feature_flags_postgres, day=2, month=1, year=2024
    )

    assert (
        "Box Scores Function Failed, no Data Available yet for 2024-01-02"
        in caplog.text
    )
    assert len(boxscores) == 0


def test_get_boxscores_data_no_games_played(
    mocker, get_feature_flags_postgres, schedule_mock_data, caplog
):
    fname = os.path.join(
        os.path.dirname(__file__), "../fixtures/boxscores_no_data.html"
    )
    with open(fname, "rb") as fp:
        mock_content = fp.read()

    mock_get = mocker.patch("src.utils.requests.get")
    mock_get.return_value.content = mock_content
    mock_json = mocker.MagicMock()
    mock_get.return_value.json = mock_json
    mock_json.return_value = schedule_mock_data

    # Clear existing logs and enable log capturing
    caplog.clear()
    caplog.set_level(logging.INFO)

    boxscores = get_boxscores_data(
        feature_flags_df=get_feature_flags_postgres, day=1, month=1, year=2024
    )

    # Assert on log messages
    assert "No Games were played on 2024-01-01; no Box Scores to pull" in caplog.text
    assert len(boxscores) == 0


def test_boxscores_upsert(postgres_conn, boxscores_data):
    # postgres_conn.execute(statement="truncate table
    # nba_source.aws_boxscores_source;", con=postgres_conn)

    count_check = "SELECT count(*) FROM nba_source.aws_boxscores_source"
    count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

    # upsert 145 records
    write_to_sql_upsert(
        postgres_conn, "boxscores", boxscores_data, "upsert", ["player", "date"]
    )

    count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

    assert (
        count_check_results_before["count"][0] == 1
    )  # check row count is 1 from the bootstrap

    assert (
        count_check_results_after["count"][0] == 145
    )  # check row count is 145, 144 new and 1 upsert
