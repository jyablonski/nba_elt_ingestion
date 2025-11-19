import logging

import pandas as pd
from sqlalchemy.engine import Connection, Engine


def get_feature_flags(connection: Connection | Engine) -> pd.DataFrame:
    """Small Utility Function

    Load all Feature Flags from the Database.

    Args:
        connection (Connection | Engine): Connection object to connect to
            the database. Can be a SQLAlchemy Engine or Connection because
            Pandas allows both

    Returns:
        pd.DataFrame: DataFrame containing all feature flags

    """
    flags = pd.read_sql_query(sql="select * from gold.feature_flags;", con=connection)

    logging.info(f"Retrieving {len(flags)} Feature Flags")
    return flags


class FeatureFlagManager:
    """Class to manage loading and checking feature flags

    Used in combination w/ the `check_feature_flag_decorator`.

    This class is a singleton, meaning it should only be instantiated once
    and used throughout the application. It loads feature flags from the
    database and provides a method to check if a specific feature flag
    is enabled or disabled.

    Note: `is_enabled` is an integer stored as 0 or 1 in the database.
    If the feature flag doesn't exist when calling `get`, it returns None.

    """

    _flags = {}

    @classmethod
    def load(cls, engine):
        """_summary_

        Args:
            engine (_type_): _description_
        """
        df = get_feature_flags(connection=engine)
        cls._flags = df.set_index("flag")["is_enabled"].to_dict()

    @classmethod
    def get(cls, flag: str) -> int:
        """_summary_

        Args:
            flag (str): _description_

        Returns:
            int: _description_
        """
        if flag not in cls._flags:
            return None  # or raise here if you want
        return cls._flags[flag]
