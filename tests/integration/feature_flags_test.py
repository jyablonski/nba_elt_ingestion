import pandas as pd
import pytest

from src.decorators import check_feature_flag_decorator
from src.feature_flags import FeatureFlagManager

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


def test_feature_flag_manager_loads_flags(load_feature_flags):
    flags = FeatureFlagManager._flags  # access internal state
    assert isinstance(flags, dict)
    assert "stats" in flags
    assert isinstance(flags["stats"], int)
    assert FeatureFlagManager.get("stats") == 1  # check it's enabled
    assert FeatureFlagManager.get("fake") == 0  # check it's disabled
    assert FeatureFlagManager.get(missing_flag) is None  # check it doesn't exist
