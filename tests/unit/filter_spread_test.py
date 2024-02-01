import pytest

from src.utils import filter_spread


@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        ("+16.0 -112 -16.0 -108", "+16.0 -16.0"),
        ("+7.0 -108 -7.0 -112", "+7.0 -7.0"),
        ("-8.0 -112 +8.0 -108", "-8.0 +8.0"),
        ("-6.0 -112 +6.0 -108", "-6.0 +6.0"),
        ("+11.5 -110 -11.5 -110", "+11.5 -11.5"),
    ],
)
def test_filter_spread(test_input, expected_output):
    clean_spread = filter_spread(test_input)

    assert clean_spread == expected_output
