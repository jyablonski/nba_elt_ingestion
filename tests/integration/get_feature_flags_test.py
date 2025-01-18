import pytest
from src.utils import check_feature_flag


@pytest.mark.parametrize(
    "flag, expected",
    [
        ("fake", False),
        ("odds", True),
        ("playoffs", True),
        ("season", True),
    ],
)
def test_get_and_check_feature_flags_postgres(
    get_feature_flags_postgres, flag, expected
):
    result = check_feature_flag(flag=flag, flags_df=get_feature_flags_postgres)
    assert result == expected


def test_feature_flags_count(get_feature_flags_postgres):
    assert len(get_feature_flags_postgres) == 16
