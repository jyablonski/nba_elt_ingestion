import logging

import pandas as pd


def write_to_sql(con, table_name: str, df: pd.DataFrame, table_type: str) -> None:
    """Simple Wrapper Function to write a Pandas DataFrame to SQL

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

        return
    except Exception as error:
        logging.error(f"SQL Write Script Failed, {error}")
        return
