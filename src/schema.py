# schemas
adv_stats_cols = [
    "index",
    "team",
    "age",
    "w",
    "l",
    "pw",
    "pl",
    "mov",
    "sos",
    "srs",
    "ortg",
    "drtg",
    "nrtg",
    "pace",
    "ftr",
    "3par",
    "ts%",
    "efg%",
    "tov%",
    "orb%",
    "ft/fga",
    "efg%_opp",
    "tov%_opp",
    "drb%_opp",
    "ft/fga_opp",
    "arena",
    "attendance",
    "att/game",
    "scrape_date",
]

boxscores_cols = [
    "player",
    "team",
    "location",
    "opponent",
    "outcome",
    "mp",
    "fgm",
    "fga",
    "fgpercent",
    "threepfgmade",
    "threepattempted",
    "threepointpercent",
    "ft",
    "fta",
    "ftpercent",
    "oreb",
    "dreb",
    "trb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "plusminus",
    "gmsc",
    "date",
    "type",
    "season",
]

injury_cols = ["player", "team", "date", "description", "scrape_date"]

opp_stats_cols = [
    "team",
    "fg_percent_opp",
    "threep_percent_opp",
    "threep_made_opp",
    "ppg_opp",
    "scrape_date",
]

pbp_cols = [
    "timequarter",
    "descriptionplayvisitor",
    "awayscore",
    "score",
    "homescore",
    "descriptionplayhome",
    "numberperiod",
    "hometeam",
    "awayteam",
    "scoreaway",
    "scorehome",
    "marginscore",
    "date",
]

reddit_cols = [
    "title",
    "score",
    "id",
    "url",
    "reddit_url",
    "num_comments",
    "body",
    "scrape_date",
    "scrape_time",
]

reddit_comment_cols = [
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
]

odds_cols = ["team", "spread", "total", "moneyline", "date", "datetime1"]

stats_cols = [
    "player",
    "pos",
    "age",
    "tm",
    "g",
    "gs",
    "mp",
    "fg",
    "fga",
    "fg%",
    "3p",
    "3pa",
    "3p%",
    "2p",
    "2pa",
    "2p%",
    "efg%",
    "ft",
    "fta",
    "ft%",
    "orb",
    "drb",
    "trb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "scrape_date",
]

transactions_cols = ["date", "transaction", "scrape_date"]

twitter_cols = [
    "created_at",
    "date",
    "username",
    "tweet",
    "language",
    "link",
    "likes_count",
    "retweets_count",
    "replies_count",
    "scrape_date",
    "scrape_ts",
    "compound",
    "neg",
    "neu",
    "pos",
    "sentiment",
]

schedule_cols = ["start_time", "away_team", "home_team", "date", "proper_date"]

shooting_stats_cols = [
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
