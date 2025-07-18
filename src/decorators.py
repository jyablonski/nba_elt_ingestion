import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

import pandas as pd

from src.feature_flags import FeatureFlagManager


def record_function_time_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator function used to record the execution time of any function

    Args:
        func (Callable): Function to track the execution time on.

    Returns:
        Callable[..., Any]: The wrapped function that records
            the execution time.
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        total_func_time = round(time.time() - start_time, 2)
        logging.info(f"{func.__name__} took {total_func_time} seconds")

        return result

    return wrapper


def check_feature_flag_decorator(*, flag_name: str) -> Callable[..., pd.DataFrame]:
    """Decorator used in most Scraper Functions

    Checks the status of a feature flag before executing
    the decorated function.

    - If the feature flag is enabled, the function executes normally.
    - If the feature flag is disabled, the function returns an empty DataFrame.
    - If the feature flag is not found, a ValueError is raised.

    The `*` in the function signature allows for keyword-only arguments, so
    calling it like `(flag_name="boxscores")` is required.

    Args:
        flag_name (str): The name of the feature flag to check.

    Returns:
        Callable[..., pd.DataFrame]: A wrapped function that conditionally
        executes based on the feature flag's value.

    Raises:
        ValueError: If the feature flag is not found in the loaded flags.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            value = FeatureFlagManager.get(flag=flag_name)

            if value is None:
                raise ValueError(
                    f"Feature flag '{flag_name}' not found in loaded flags."
                )

            if not value:
                logging.info(
                    f"Feature flag '{flag_name}' is disabled. Skipping {func.__name__}."
                )
                return pd.DataFrame()
            return func(*args, **kwargs)

        return wrapper

    return decorator
