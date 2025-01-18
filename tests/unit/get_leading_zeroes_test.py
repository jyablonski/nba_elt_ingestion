from datetime import date

import pytest

from src.utils import get_leading_zeroes


@pytest.mark.parametrize(
    "input_date, expected",
    [
        (date(2023, 1, 1).month, "01"),
        (date(2023, 9, 1).month, "09"),
        (date(2023, 10, 1).month, "10"),
    ],
)
def test_get_leading_zeroes(input_date, expected):
    assert get_leading_zeroes(value=input_date) == expected
