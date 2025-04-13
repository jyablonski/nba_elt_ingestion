import logging

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine


def get_feature_flags(connection: Connection | Engine) -> pd.DataFrame:
    flags = pd.read_sql_query(sql="select * from marts.feature_flags;", con=connection)

    logging.info(f"Retrieving {len(flags)} Feature Flags")
    return flags


def check_feature_flag(flag: str, flags_df: pd.DataFrame) -> bool:
    flags_df = flags_df.query(f"flag == '{flag}'")

    if len(flags_df) > 0 and flags_df["is_enabled"].iloc[0] == 1:
        return True
    else:
        return False


def write_to_sql(con, table_name: str, df: pd.DataFrame, table_type: str) -> None:
    """
    Simple Wrapper Function to write a Pandas DataFrame to SQL

    Args:
        con (SQL Connection): The connection to the SQL DB.

        table_name (str): The Table name to write to SQL as.

        df (DataFrame): The Pandas DataFrame to store in SQL

        table_type (str): Whether the table should replace or append to an
            existing SQL Table under that name

    Returns:
        Writes the Pandas DataFrame to a Table in the Schema we connected to.

    """
    try:
        if len(df) == 0:
            logging.info(f"{table_name} is empty, not writing to SQL")
        else:
            df.to_sql(
                con=con,
                name=table_name,
                index=False,
                if_exists=table_type,
            )
            logging.info(
                f"Writing {len(df)} {table_name} rows to aws_{table_name}_source to SQL"
            )

        return None
    except Exception as error:
        logging.error(f"SQL Write Script Failed, {error}")
        return None
        # sentry_sdk.capture_exception(error)


def load_feature_flags(engine: Engine) -> None:
    global FEATURE_FLAGS

    with engine.begin() as connection:
        df = get_feature_flags(connection=connection)

    FEATURE_FLAGS = df.set_index("flag")["is_enabled"].to_dict()
    return None
