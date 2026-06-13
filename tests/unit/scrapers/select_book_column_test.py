import pandas as pd
import pytest

from src.scrapers import (
    _find_covers_sportsbook_column_index,
    _select_sportsbook_column,
)


def test_find_covers_sportsbook_column_index_finds_draftkings():
    html = """
    <table>
      <tr>
        <th>Time (ET) Game Open</th>
        <th><img alt="Kalshi Sports"></th>
        <th><img alt="DraftKings"></th>
      </tr>
    </table>
    """

    assert _find_covers_sportsbook_column_index(html, table_index=0) == 2


def test_find_covers_sportsbook_column_index_raises_when_missing():
    html = "<table><tr><th><img alt='BetMGM'></th></tr></table>"

    with pytest.raises(ValueError, match="DraftKings column not found"):
        _find_covers_sportsbook_column_index(html, table_index=0)


def test_select_sportsbook_column_returns_string_series():
    table = pd.DataFrame(
        {
            "game": ["Today, 20:30 NY SA"],
            "open": ["+166  -200"],
            "draftkings": ["+176  -188"],
        }
    )

    assert _select_sportsbook_column(table, 2).iloc[0] == "+176  -188"


def test_select_sportsbook_column_raises_when_column_empty():
    table = pd.DataFrame({"game": ["Today, 20:30 NY SA"], "empty": [None]})

    with pytest.raises(ValueError, match="No data in sportsbook column"):
        _select_sportsbook_column(table, 1)
