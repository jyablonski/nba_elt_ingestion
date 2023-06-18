from datetime import date

from src.utils import get_leading_zeroes


def test_get_leading_zeroes():
    month_1 = get_leading_zeroes(date(2023, 1, 1).month)
    month_9 = get_leading_zeroes(date(2023, 9, 1).month)
    month_10 = get_leading_zeroes(date(2023, 10, 1).month)

    assert month_1 == "01"
    assert month_9 == "09"
    assert month_10 == 10
