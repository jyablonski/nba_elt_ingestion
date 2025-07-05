def test_player_shooting_stats_data(shooting_stats_data):
    expected_columns = [
        "player",
        "avg_shot_distance",
        "pct_fga_2p",
        "pct_fga_0_3",
        "pct_fga_3_10",
        "pct_fga_10_16",
        "pct_fga_16_3p",
        "pct_fga_3p",
        "fg_pct_0_3",
        "fg_pct_3_10",
        "fg_pct_10_16",
        "fg_pct_16_3p",
        "pct_2pfg_ast",
        "pct_3pfg_ast",
        "dunk_pct_tot_fg",
        "dunks",
        "corner_3_ast_pct",
        "corner_3pm_pct",
        "heaves_att",
        "heaves_makes",
        "scrape_date",
        "scrape_ts",
    ]

    assert list(shooting_stats_data.columns) == expected_columns
    assert len(shooting_stats_data) == 474
