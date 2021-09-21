import pytest

@pytest.fixture(scope = "session")
def input_value():
    input = 35
    return(input)

@pytest.fixture(scope = "session")
def bbref_url():
    url_sample = "https://www.basketball-reference.com/"
    return(url_sample)

@pytest.fixture(scope = "session")
def draftkings_url():
    url_sample = "https://sportsbook.draftkings.com/leagues/basketball/88670846?category=game-lines&subcategory=game"
    return(url_sample)