import logging

import pandas as pd
from sqlalchemy.engine import Connection, Engine


def get_feature_flags(connection: Connection | Engine) -> pd.DataFrame:
    flags = pd.read_sql_query(sql="select * from marts.feature_flags;", con=connection)

    logging.info(f"Retrieving {len(flags)} Feature Flags")
    return flags


class FeatureFlagManager:
    _flags = {}

    @classmethod
    def load(cls, engine):
        df = get_feature_flags(connection=engine)
        cls._flags = df.set_index("flag")["is_enabled"].to_dict()

    @classmethod
    def get(cls, flag: str) -> bool:
        if flag not in cls._flags:
            return None  # or raise here if you want
        return cls._flags[flag]
