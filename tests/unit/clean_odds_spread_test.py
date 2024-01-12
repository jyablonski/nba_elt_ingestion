import pytest

from src.utils import clean_odds_spread

@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        ("-4.5", "-4.5"),
        ("5", "5"),
        ("5.0 115.0", "5.0"),
        ("-8.5 -118", "-8.5"),
        ("-2.5 -111", "-2.5"),
    ],
)
def test_clean_odds_spread(test_input, expected_output):
    clean_spread = clean_odds_spread(test_input)

    assert clean_spread == expected_output
