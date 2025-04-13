import pandas as pd
import pytest
from src.decorators import check_feature_flag_decorator

missing_flag = "flag_doesnt_exist"


# Flag 'stats' is enabled in DB
@check_feature_flag_decorator(flag_name="stats")
def decorated_enabled():
    return pd.DataFrame({"foo": [1, 2, 3]})


# Flag 'fake' is disabled in DB
@check_feature_flag_decorator(flag_name="fake")
def decorated_disabled():
    return pd.DataFrame({"foo": [1, 2, 3]})


# Flag does not exist
@check_feature_flag_decorator(flag_name=missing_flag)
def decorated_missing():
    return pd.DataFrame({"foo": [1, 2, 3]})


def test_check_feature_flag_decorator_enabled(load_feature_flags):
    df = decorated_enabled()
    assert not df.empty


def test_check_feature_flag_decorator_disabled(load_feature_flags):
    df = decorated_disabled()
    assert df.empty


def test_check_feature_flag_decorator_missing(load_feature_flags):
    with pytest.raises(
        ValueError, match=f"Feature flag '{missing_flag}' not found in loaded flags."
    ):
        decorated_missing()
