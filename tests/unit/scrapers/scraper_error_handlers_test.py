import pandas as pd
import pytest

from src.scrapers import (
    get_boxscores_data,
    get_injuries_data,
    get_odds_data,
    get_opp_stats_data,
    get_player_adv_stats_data,
    get_player_stats_data,
    get_shooting_stats_data,
    get_team_adv_stats_data,
    get_transactions_data,
)


@pytest.mark.parametrize(
    "scraper,patch_target",
    [
        (get_player_stats_data, "src.scrapers.urllib.request.urlopen"),
        (get_boxscores_data, "src.scrapers.urllib.request.urlopen"),
        (get_opp_stats_data, "src.scrapers.pd.read_html"),
        (get_injuries_data, "src.scrapers.pd.read_html"),
        (get_player_adv_stats_data, "src.scrapers.pd.read_html"),
        (get_team_adv_stats_data, "src.scrapers.pd.read_html"),
        (get_shooting_stats_data, "src.scrapers.pd.read_html"),
        (get_odds_data, "src.scrapers.pd.read_html"),
    ],
)
def test_scraper_returns_empty_dataframe_on_failure(mocker, scraper, patch_target):
    mocker.patch(patch_target, side_effect=Exception("scrape failed"))

    result = scraper()

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_transactions_returns_empty_when_page_index_missing(mocker):
    mock_response = mocker.MagicMock()
    mock_response.read.return_value = b"<html><body></body></html>"
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None
    mocker.patch("urllib.request.urlopen", return_value=mock_response)

    result = get_transactions_data()

    assert result.empty
