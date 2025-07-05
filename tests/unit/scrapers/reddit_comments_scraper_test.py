def test_reddit_comments_data(reddit_comments_data):
    expected_columns = [
        "author",
        "comment",
        "score",
        "url",
        "flair1",
        "flair2",
        "edited",
        "scrape_date",
        "scrape_ts",
        "compound",
        "neg",
        "neu",
        "pos",
        "sentiment",
        "md5_pk",
    ]

    assert list(reddit_comments_data.columns) == expected_columns
    assert len(reddit_comments_data) == 998
