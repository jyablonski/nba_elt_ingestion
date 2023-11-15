import pytest

from src.utils import clean_player_names


@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        ("Marcus Morris IV", "Marcus Morris"),
        ("Kelly Oubre Jr.", "Kelly Oubre"),
        ("Marcus Morris Sr.", "Marcus Morris"),
        ("Robert Williams IV", "Robert Williams"),
        ("Gary Payton II", "Gary Payton"),
        ("Foobar II", "Foobar"),
    ],
)
def test_clean_player_names_2(test_input, expected_output):
    clean_name = clean_player_names(test_input)

    assert clean_name == expected_output
