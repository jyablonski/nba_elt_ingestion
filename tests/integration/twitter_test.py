# import pandas as pd

# from src.utils import write_to_sql_upsert


# def test_twitter_tweepy_data_upsert(postgres_conn, twitter_tweepy_data):
#     count_check = "SELECT count(*) FROM nba_source.aws_twitter_tweepy_data_source"
#     count_check_results_before = pd.read_sql_query(sql=count_check, con=postgres_conn)

#     # upsert 1060 records
#     write_to_sql_upsert(
#         conn=postgres_conn,
#         table_name="twitter_tweepy_data",
#         df=twitter_tweepy_data,
#         pd_index=["tweet_id"],
#     )

#     count_check_results_after = pd.read_sql_query(sql=count_check, con=postgres_conn)

#     assert (
#         count_check_results_before["count"][0] == 1
#     )  # check row count is 1 from the bootstrap

#     assert (
#         count_check_results_after["count"][0] == 1060
#     )  # check row count is 1060, 1059 new and 1 upsert
