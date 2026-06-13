import pandas as pd

from src.aws import write_to_s3


def test_write_to_s3_handles_error(mocker):
    mocker.patch(
        "src.aws.wr.s3.to_parquet",
        side_effect=Exception("s3 upload failed"),
    )

    write_to_s3("player_contracts", pd.DataFrame({"player": ["Stephen Curry"]}))
