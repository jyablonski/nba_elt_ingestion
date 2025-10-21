import logging
from datetime import datetime
from pathlib import Path

from jyablonski_common_modules.sql import write_to_sql_upsert

from src.scrapers import get_boxscores_data
from tests.utils.db_assertions import assert_db_row_count_change


def test_get_boxscores_data_no_games_played(mocker, caplog):
    fname = Path(__file__).parent / "../fixtures/boxscores_no_data.html"
    with fname.open("rb") as fp:
        mock_content = fp.read()

    mock_response = mocker.MagicMock()
    mock_response.__enter__.return_value.read.return_value = mock_content

    mocker.patch("src.scrapers.urllib.request.urlopen", return_value=mock_response)

    mock_json = mocker.MagicMock()
    mock_json.return_value = []
    mocker.patch("src.utils.requests.get").return_value.json = mock_json

    # Clear existing logs and enable log capturing
    caplog.clear()
    caplog.set_level(logging.INFO)

    boxscores = get_boxscores_data(run_date=datetime(2024, 1, 2))

    assert (
        "Box Scores Function Warning, no games played on 2024-01-02 so no data "
        "available" in caplog.text
    )
    assert len(boxscores) == 0


def test_get_boxscores_data_no_data_available(mocker, schedule_mock_data, caplog):
    fname = Path(__file__).parent / "../fixtures/boxscores_no_data.html"
    with fname.open("rb") as fp:
        mock_content = fp.read()

    mock_response = mocker.MagicMock()
    mock_response.__enter__.return_value.read.return_value = mock_content

    mocker.patch("src.scrapers.urllib.request.urlopen", return_value=mock_response)

    mock_json = mocker.MagicMock()
    mock_json.return_value = schedule_mock_data
    mocker.patch("src.utils.requests.get").return_value.json = mock_json

    # Clear existing logs and enable log capturing
    caplog.clear()
    caplog.set_level(logging.INFO)

    boxscores = get_boxscores_data(run_date=datetime(2024, 1, 1))

    # Assert on log messages
    assert (
        "Box Scores Function Failed, Box Scores aren't available yet for 2024-01-01"
        in caplog.text
    )
    assert len(boxscores) == 0


def test_boxscores_upsert(postgres_conn, boxscores_data):
    assert_db_row_count_change(
        conn=postgres_conn,
        table="bbref_player_boxscores",
        schema="nba_source",
        expected_before=1,
        expected_after=145,
        writer=write_to_sql_upsert,
        writer_kwargs={
            "conn": postgres_conn,
            "table": "bbref_player_boxscores",
            "schema": "nba_source",
            "df": boxscores_data,
            "primary_keys": ["player", "date"],
        },
    )
