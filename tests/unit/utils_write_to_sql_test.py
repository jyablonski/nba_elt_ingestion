import pandas as pd

from src.utils import write_to_sql


def test_utils_write_to_sql_skips_empty_dataframe(mocker):
    mock_con = mocker.MagicMock()
    to_sql = mocker.patch("src.utils.pd.DataFrame.to_sql")

    write_to_sql(mock_con, "stats", pd.DataFrame(), "append")

    to_sql.assert_not_called()


def test_utils_write_to_sql_logs_error_on_failure(mocker):
    mock_con = mocker.MagicMock()
    mocker.patch(
        "src.utils.pd.DataFrame.to_sql",
        side_effect=Exception("write failed"),
    )

    write_to_sql(mock_con, "stats", pd.DataFrame({"player": ["Test Player"]}), "append")
