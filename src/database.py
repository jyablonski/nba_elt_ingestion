from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

import pandas as pd

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Connection


def write_to_sql(
    con,
    table_name: str,
    df: pd.DataFrame,
    table_type: Literal["fail", "replace", "append"],
    schema: str = "bronze",
) -> None:
    """Simple Wrapper Function to write a Pandas DataFrame to SQL

    Args:
        con (SQL Connection): The connection to the SQL DB.

        table_name (str): The Table name to write to SQL as.

        df (DataFrame): The Pandas DataFrame to store in SQL

        table_type (str): Whether the table should replace or append to an
            existing SQL Table under that name

        schema (str): Schema to write to

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
                schema=schema,
            )
            logging.info(
                f"Writing {len(df)} {table_name} rows to {schema}.{table_name} to SQL"
            )

        return
    except Exception as error:
        logging.error(f"SQL Write Script Failed, {error}")
        return


def filter_unchanged_rows(
    conn: Connection,
    schema: str,
    table: str,
    df: pd.DataFrame,
    primary_keys: list[str],
    compare_columns: list[str],
) -> pd.DataFrame:
    """Return only rows that are new or have changed values in compare_columns."""
    if df.empty:
        return df

    select_columns = primary_keys + compare_columns
    existing = pd.read_sql(
        f"SELECT {', '.join(select_columns)} FROM {schema}.{table}",
        con=conn,
    )
    if existing.empty:
        return df

    merged = df.merge(existing, on=primary_keys, how="left", suffixes=("", "_existing"))
    is_new = merged[f"{compare_columns[0]}_existing"].isna()
    is_changed = pd.Series(False, index=merged.index)
    for col in compare_columns:
        is_changed = is_changed | (merged[col] != merged[f"{col}_existing"])

    return df.loc[(is_new | is_changed).values].reset_index(drop=True)
