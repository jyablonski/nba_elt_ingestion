def test_add_sentiment_analysis(add_sentiment_analysis_df):
    assert len(add_sentiment_analysis_df) == 1000
    assert add_sentiment_analysis_df["compound"][0] == 0.0
    assert add_sentiment_analysis_df["neg"][0] == 0.0
    assert add_sentiment_analysis_df["neu"][0] == 1.0
    assert add_sentiment_analysis_df["pos"][0] == 0.0
    assert add_sentiment_analysis_df["sentiment"][0] == 0
