from collections.abc import Callable

import pandas as pd


def assert_db_row_count_change(
    conn,
    table: str,
    expected_before: int | None,
    expected_after: int,
    *,
    schema: str | None = None,
    writer: Callable[..., None],
    writer_kwargs: dict,
):
    """Generic helper for asserting a row count change in a table.

    Args:
        conn: SQLAlchemy connection or engine.

        table: Table name (without schema).

        expected_before: Optional expected row count before insert/upsert.

        expected_after: Expected row count after operation.

        schema: Optional schema name.

        writer: Function that writes the data (e.g., write_to_sql_upsert).

        writer_kwargs: Dict of kwargs to pass to writer.

    Raises:
        AssertionError if row counts don't match expectations.
    """
    full_table = f"{schema}.{table}" if schema else table
    count_sql = f"SELECT count(*) FROM {full_table}"

    before_count = pd.read_sql_query(sql=count_sql, con=conn)["count"][0]

    if expected_before is not None:
        assert before_count == expected_before, (
            f"Expected {expected_before} rows before insert, got {before_count}"
        )

    # Execute insert/upsert
    writer(**writer_kwargs)

    after_count = pd.read_sql_query(sql=count_sql, con=conn)["count"][0]

    assert after_count == expected_after, (
        f"Expected {expected_after} rows after insert, got {after_count}"
    )
