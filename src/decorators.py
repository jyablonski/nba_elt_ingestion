from functools import wraps
import logging
import time
from typing import Any, Callable

import pandas as pd

from src.feature_flags import FeatureFlagManager


def time_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator function used to record the execution time of any
    function it's applied to.

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


def check_feature_flag_decorator(flag_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not FeatureFlagManager.get(flag=flag_name):
                logging.info(
                    f"Feature flag '{flag_name}' is disabled. Skipping {func.__name__}."
                )
                return pd.DataFrame()
            return func(*args, **kwargs)

        return wrapper

    return decorator
