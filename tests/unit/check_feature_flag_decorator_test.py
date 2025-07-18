from unittest.mock import patch

import pandas as pd
import pytest

from src.decorators import check_feature_flag_decorator


def test_decorator_runs_function_when_flag_enabled():
    with patch("src.decorators.FeatureFlagManager.get", return_value=True):

        @check_feature_flag_decorator(flag_name="enabled_flag")
        def sample():
            return pd.DataFrame({"foo": [1, 2, 3]})

        result = sample()
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert list(result.columns) == ["foo"]


def test_decorator_returns_empty_dataframe_when_flag_disabled():
    with patch("src.decorators.FeatureFlagManager.get", return_value=False):

        @check_feature_flag_decorator(flag_name="disabled_flag")
        def sample():
            return pd.DataFrame({"foo": [1, 2, 3]})

        result = sample()
        assert isinstance(result, pd.DataFrame)
        assert result.empty


def test_decorator_raises_error_when_flag_missing():
    flag = "missing_flag"
    with patch("src.decorators.FeatureFlagManager.get", return_value=None):

        @check_feature_flag_decorator(flag_name=flag)
        def sample():
            return pd.DataFrame({"foo": [1, 2, 3]})

        with pytest.raises(
            ValueError, match=f"Feature flag '{flag}' not found in loaded flags."
        ):
            sample()
