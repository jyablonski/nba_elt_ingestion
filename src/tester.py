from datetime import datetime
import logging

import pandas as pd

from src.decorators import time_function, feature_flagged
from src.database import check_feature_flag
from src.utils import clean_player_names


@feature_flagged("injuries")
@time_function
def get_injuries_data() -> pd.DataFrame:
    """
    Web Scrape function w/ pandas read_html that grabs all current injuries

    Args:
        feature_flags_df (pd.DataFrame): Feature Flags DataFrame to check
            whether to run this function or not

    Returns:
        Pandas DataFrame of all current player injuries & their associated team
    """
    try:
        url = "https://www.basketball-reference.com/friv/injuries.fcgi"
        df = pd.read_html(url)[0]
        df = df.rename(columns={"Update": "Date"})
        df.columns = df.columns.str.lower()
        df["scrape_date"] = datetime.now().date()
        df["player"] = (
            df["player"]
            .str.normalize("NFKD")  # this is removing all accented characters
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )
        df["player"] = df["player"].apply(clean_player_names)
        df = df.drop_duplicates()
        logging.info(
            f"Injury Transformation Function Successful, retrieving {len(df)} rows"
        )
        return df
    except Exception as error:
        logging.error(f"Injury Web Scrape Function Failed, {error}")
        df = pd.DataFrame()
        return df
