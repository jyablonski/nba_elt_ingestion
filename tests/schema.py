import numpy as np

adv_stats_schema = {
    "index": np.dtype("int64"),
    "team": np.dtype("O"),
    "age": np.dtype("float64"),
    "w": np.dtype("float64"),
    "l": np.dtype("float64"),
    "pw": np.dtype("int64"),
    "pl": np.dtype("int64"),
    "mov": np.dtype("float64"),
    "sos": np.dtype("float64"),
    "srs": np.dtype("float64"),
    "ortg": np.dtype("float64"),
    "drtg": np.dtype("float64"),
    "nrtg": np.dtype("float64"),
    "pace": np.dtype("float64"),
    "ftr": np.dtype("float64"),
    "3par": np.dtype("float64"),
    "ts%": np.dtype("float64"),
    "efg%": np.dtype("float64"),
    "tov%": np.dtype("float64"),
    "orb%": np.dtype("float64"),
    "ft/fga": np.dtype("float64"),
    "efg%_opp": np.dtype("float64"),
    "tov%_opp": np.dtype("float64"),
    "drb%_opp": np.dtype("float64"),
    "ft/fga_opp": np.dtype("float64"),
    "arena": np.dtype("O"),
    "attendance": np.dtype("int64"),
    "att/game": np.dtype("int64"),
    "scrape_date": np.dtype("O"),
}

boxscores_schema = {
    "player": np.dtype("O"),
    "team": np.dtype("O"),
    "location": np.dtype("O"),
    "opponent": np.dtype("O"),
    "outcome": np.dtype("O"),
    "mp": np.dtype("O"),
    "fgm": np.dtype("float64"),
    "fga": np.dtype("float64"),
    "fgpercent": np.dtype("float64"),
    "threepfgmade": np.dtype("float64"),
    "threepattempted": np.dtype("float64"),
    "threepointpercent": np.dtype("float64"),
    "ft": np.dtype("O"),
    "fta": np.dtype("O"),
    "ftpercent": np.dtype("O"),
    "oreb": np.dtype("float64"),
    "dreb": np.dtype("float64"),
    "trb": np.dtype("float64"),
    "ast": np.dtype("float64"),
    "stl": np.dtype("float64"),
    "blk": np.dtype("float64"),
    "tov": np.dtype("float64"),
    "pf": np.dtype("float64"),
    "pts": np.dtype("float64"),
    "plusminus": np.dtype("float64"),
    "gmsc": np.dtype("float64"),
    "date": np.dtype("<M8[ns]"),
    "type": np.dtype("O"),
    "scrape_date": np.dtype("O"),
}

boxscores_schema_fake = {
    "player": np.dtype("O"),
    "team": np.dtype("O"),
    "location": np.dtype("O"),
    "opponent": np.dtype("O"),
    "outcome": np.dtype("O"),
    "mp": np.dtype("O"),
    "fgm": np.dtype("float64"),
    "fga": np.dtype("float64"),
    "fgpercent": np.dtype("float64"),
    "threepfgmade": np.dtype("float64"),
    "threepattempted": np.dtype("float64"),
    "threepointpercent": np.dtype("float64"),
    "ft": np.dtype("float64"),
    "fta": np.dtype("float64"),
    "ftpercent": np.dtype("float64"),
    "oreb": np.dtype("float64"),
    "dreb": np.dtype("float64"),
    "trb": np.dtype("float64"),
    "ast": np.dtype("float64"),
    "stl": np.dtype("float64"),
    "blk": np.dtype("float64"),
    "tov": np.dtype("float64"),
    "pf": np.dtype("float64"),
    "pts": np.dtype("float64"),
    "plusminus": np.dtype("float64"),
    "gmsc": np.dtype("float64"),
    "date": np.dtype("<M8[ns]"),
    "type": np.dtype("O"),
    "FAKE_COLUMN": np.dtype("O"),
}

injury_schema = {
    "player": np.dtype("O"),
    "team": np.dtype("O"),
    "date": np.dtype("O"),
    "description": np.dtype("O"),
    "scrape_date": np.dtype("O"),
}

opp_stats_schema = {
    "team": np.dtype("O"),
    "fg_percent_opp": np.dtype("float64"),
    "threep_percent_opp": np.dtype("float64"),
    "threep_made_opp": np.dtype("float64"),
    "ppg_opp": np.dtype("float64"),
    "scrape_date": np.dtype("O"),
}


pbp_data_schema = {
    "timequarter": np.dtype("O"),
    "descriptionplayvisitor": np.dtype("O"),
    "awayscore": np.dtype("O"),
    "score": np.dtype("O"),
    "homescore": np.dtype("O"),
    "descriptionplayhome": np.dtype("O"),
    "numberperiod": np.dtype("O"),
    "hometeam": np.dtype("O"),
    "awayteam": np.dtype("O"),
    "scoreaway": np.dtype("float64"),
    "scorehome": np.dtype("float64"),
    "marginscore": np.dtype("float64"),
    "date": np.dtype("<M8[ns]"),
    "scrape_date": np.dtype("O"),
}

reddit_data_schema = {
    "title": np.dtype("O"),
    "score": np.dtype("int64"),
    "id": np.dtype("O"),
    "url": np.dtype("O"),
    "reddit_url": np.dtype("O"),
    "num_comments": np.dtype("int64"),
    "body": np.dtype("O"),
    "scrape_date": np.dtype("O"),
    "scrape_time": np.dtype("<M8[ns]"),
}

reddit_comment_data_schema = {
    "author": np.dtype("O"),
    "comment": np.dtype("O"),
    "score": np.dtype("int64"),
    "url": np.dtype("O"),
    "flair1": np.dtype("O"),
    "flair2": np.dtype("O"),
    "edited": np.dtype("int64"),
    "scrape_date": np.dtype("O"),
    "scrape_ts": np.dtype("O"),
    "compound": np.dtype("float64"),
    "neg": np.dtype("float64"),
    "neu": np.dtype("float64"),
    "pos": np.dtype("float64"),
    "sentiment": np.dtype("int64"),
    "md5_pk": np.dtype("O"),
}

odds_schema = {
    "team": np.dtype("O"),
    "spread": np.dtype("O"),
    "total": np.dtype("int64"),
    "moneyline": np.dtype("int64"),
    "date": np.dtype("O"),
    "datetime1": np.dtype("<M8[ns]"),
}

schedule_schema = {
    "start_time": np.dtype("O"),
    "away_team": np.dtype("O"),
    "home_team": np.dtype("O"),
    "date": np.dtype("O"),
    "proper_date": np.dtype("O"),
}

shooting_stats_schema = {
    "player": np.dtype("O"),
    "avg_shot_distance": np.dtype("float64"),
    "pct_fga_2p": np.dtype("float64"),
    "pct_fga_0_3": np.dtype("float64"),
    "pct_fga_3_10": np.dtype("float64"),
    "pct_fga_10_16": np.dtype("float64"),
    "pct_fga_16_3p": np.dtype("float64"),
    "pct_fga_3p": np.dtype("float64"),
    "fg_pct_0_3": np.dtype("float64"),
    "fg_pct_3_10": np.dtype("float64"),
    "fg_pct_10_16": np.dtype("float64"),
    "fg_pct_16_3p": np.dtype("float64"),
    "pct_2pfg_ast": np.dtype("float64"),
    "pct_3pfg_ast": np.dtype("float64"),
    "dunk_pct_tot_fg": np.dtype("float64"),
    "dunks": np.dtype("float64"),
    "corner_3_ast_pct": np.dtype("float64"),
    "corner_3pm_pct": np.dtype("float64"),
    "heaves_att": np.dtype("float64"),
    "heaves_makes": np.dtype("float64"),
    "scrape_date": np.dtype("O"),
    "scrape_ts": np.dtype("<M8[us]"),
}


stats_schema = {
    "player": np.dtype("O"),
    "pos": np.dtype("O"),
    "age": np.dtype("O"),
    "team": np.dtype("O"),
    "g": np.dtype("O"),
    "gs": np.dtype("O"),
    "mp": np.dtype("O"),
    "fg": np.dtype("O"),
    "fga": np.dtype("O"),
    "fg%": np.dtype("O"),
    "3p": np.dtype("O"),
    "3pa": np.dtype("O"),
    "3p%": np.dtype("O"),
    "2p": np.dtype("O"),
    "2pa": np.dtype("O"),
    "2p%": np.dtype("O"),
    "efg%": np.dtype("O"),
    "ft": np.dtype("O"),
    "fta": np.dtype("O"),
    "ft%": np.dtype("O"),
    "orb": np.dtype("O"),
    "drb": np.dtype("O"),
    "trb": np.dtype("O"),
    "ast": np.dtype("O"),
    "stl": np.dtype("O"),
    "blk": np.dtype("O"),
    "tov": np.dtype("O"),
    "pf": np.dtype("O"),
    "pts": np.dtype("float64"),
    "scrape_date": np.dtype("O"),
}

transactions_schema = {
    "date": np.dtype("<M8[ns]"),
    "transaction": np.dtype("O"),
    "scrape_date": np.dtype("O"),
}

twitter_data_schema = {
    "api_created_at": np.dtype("O"),
    "date": np.dtype("O"),
    "username": np.dtype("O"),
    "tweet": np.dtype("O"),
    "language": np.dtype("O"),
    "link": np.dtype("O"),
    "likes_count": np.dtype("int64"),
    "retweets_count": np.dtype("int64"),
    "replies_count": np.dtype("int64"),
    "scrape_date": np.dtype("O"),
    "scrape_ts": np.dtype("<M8[ns]"),
    "compound": np.dtype("float64"),
    "neg": np.dtype("float64"),
    "neu": np.dtype("float64"),
    "pos": np.dtype("float64"),
    "sentiment": np.dtype("int64"),
}

twitter_tweepy_schema = {
    "api_created_at": np.dtype("O"),
    "tweet_id": np.dtype("int64"),
    "username": np.dtype("O"),
    "user_id": np.dtype("float64"),
    "tweet": np.dtype("O"),
    "likes": np.dtype("float64"),
    "retweets": np.dtype("float64"),
    "language": np.dtype("O"),
    "scrape_ts": np.dtype("O"),
    "profile_img": np.dtype("O"),
    "url": np.dtype("O"),
    "compound": np.dtype("float64"),
    "neg": np.dtype("float64"),
    "neu": np.dtype("float64"),
    "pos": np.dtype("float64"),
    "sentiment": np.dtype("int64"),
}
