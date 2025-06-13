CREATE SCHEMA nba_source;
CREATE SCHEMA marts;

SET search_path TO nba_source;


DROP TABLE IF EXISTS nba_source.bbref_player_boxscores;
CREATE TABLE IF NOT EXISTS nba_source.bbref_player_boxscores
(
    player text COLLATE pg_catalog."default",
    team text COLLATE pg_catalog."default",
    location text COLLATE pg_catalog."default",
    opponent text COLLATE pg_catalog."default",
    outcome text COLLATE pg_catalog."default",
    mp text COLLATE pg_catalog."default",
    fgm double precision,
    fga double precision,
    fgpercent double precision,
    threepfgmade double precision,
    threepattempted double precision,
    threepointpercent double precision,
    ft text COLLATE pg_catalog."default",
    fta text COLLATE pg_catalog."default",
    ftpercent text COLLATE pg_catalog."default",
    oreb double precision,
    dreb double precision,
    trb double precision,
    ast double precision,
    stl double precision,
    blk double precision,
    tov double precision,
    pf double precision,
    pts double precision,
    plusminus double precision,
    gmsc double precision,
    date timestamp without time zone,
    type text COLLATE pg_catalog."default",
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_boxscores UNIQUE (player, date)
);

INSERT INTO nba_source.bbref_player_boxscores(
	player, team, location, opponent, outcome, mp, fgm, fga, fgpercent, threepfgmade, threepattempted, threepointpercent, ft, fta, ftpercent, oreb, dreb, trb, ast, stl, blk, tov, pf, pts, plusminus, gmsc, date, type, scrape_date)
	VALUES ('Joel Embiid','PHI','A','HOU','W','26:24',9.0,16.0,0.563,0.0,11.0,0.545,4,4,1.000,0.0,7.0,7.0,7.0,2.0,1.0,2.0,4.0,34.0,8.0,30.2,current_date - INTERVAL '1 DAY','Regular Season','2022-09-24');

DROP TABLE IF EXISTS nba_source.draftkings_game_odds;
CREATE TABLE IF NOT EXISTS nba_source.draftkings_game_odds
(
    team text COLLATE pg_catalog."default",
    spread text COLLATE pg_catalog."default",
    total text COLLATE pg_catalog."default",
    moneyline double precision,
    date date,
    datetime1 timestamp without time zone,
    scrape_ts timestamp default current_timestamp,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_odds UNIQUE (team, date)
);

INSERT INTO nba_source.draftkings_game_odds(
	team, spread, total, moneyline, date, datetime1)
	VALUES ('POR', '-1.0', '200', -115, current_date, current_timestamp);

DROP TABLE IF EXISTS nba_source.bbref_player_pbp;
CREATE TABLE IF NOT EXISTS nba_source.bbref_player_pbp
(
    timequarter text COLLATE pg_catalog."default",
    descriptionplayvisitor text COLLATE pg_catalog."default",
    awayscore text COLLATE pg_catalog."default",
    score text COLLATE pg_catalog."default",
    homescore text COLLATE pg_catalog."default",
    descriptionplayhome text COLLATE pg_catalog."default",
    numberperiod text COLLATE pg_catalog."default",
    hometeam text COLLATE pg_catalog."default",
    awayteam text COLLATE pg_catalog."default",
    scoreaway double precision,
    scorehome double precision,
    marginscore double precision,
    date date,
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_pbp_data UNIQUE (hometeam, awayteam, date, timequarter, numberperiod, descriptionplayvisitor, descriptionplayhome)
);

INSERT INTO nba_source.bbref_player_pbp(
	timequarter, descriptionplayvisitor, awayscore, score, homescore, descriptionplayhome, numberperiod, hometeam, awayteam, scoreaway, scorehome, marginscore, date, scrape_date)
	VALUES ('0:00.0', 'End of 4th quarter', 'End of 4th quarter', 'End of 4th quarter', 'End of 4th quarter', 'End of 4th quarter', '4th Quarter', 'BOS', 'GSW', 103, 90, -13, '2022-06-16', current_date);

DROP TABLE IF EXISTS nba_source.bbref_team_opponent_shooting_stats;
CREATE TABLE IF NOT EXISTS nba_source.bbref_team_opponent_shooting_stats
(
    team text COLLATE pg_catalog."default",
    fg_percent_opp double precision,
    threep_percent_opp double precision,
    threep_made_opp double precision,
    ppg_opp double precision,
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_opp_stats UNIQUE (team)
);

INSERT INTO nba_source.bbref_team_opponent_shooting_stats(
	team, fg_percent_opp, threep_percent_opp, threep_made_opp, ppg_opp, scrape_date)
	VALUES ('Miami Heat*', 0.447, 0.339, 13, 105.6, current_date);

DROP TABLE IF EXISTS nba_source.bbref_player_shooting_stats;
CREATE TABLE IF NOT EXISTS nba_source.bbref_player_shooting_stats
(
    player text COLLATE pg_catalog."default",
    avg_shot_distance text COLLATE pg_catalog."default",
    pct_fga_2p text COLLATE pg_catalog."default",
    pct_fga_0_3 text COLLATE pg_catalog."default",
    pct_fga_3_10 text COLLATE pg_catalog."default",
    pct_fga_10_16 text COLLATE pg_catalog."default",
    pct_fga_16_3p text COLLATE pg_catalog."default",
    pct_fga_3p text COLLATE pg_catalog."default",
    fg_pct_0_3 text COLLATE pg_catalog."default",
    fg_pct_3_10 text COLLATE pg_catalog."default",
    fg_pct_10_16 text COLLATE pg_catalog."default",
    fg_pct_16_3p text COLLATE pg_catalog."default",
    pct_2pfg_ast text COLLATE pg_catalog."default",
    pct_3pfg_ast text COLLATE pg_catalog."default",
    dunk_pct_tot_fg text COLLATE pg_catalog."default",
    dunks text COLLATE pg_catalog."default",
    corner_3_ast_pct text COLLATE pg_catalog."default",
    corner_3pm_pct text COLLATE pg_catalog."default",
    heaves_att text COLLATE pg_catalog."default",
    heaves_makes text COLLATE pg_catalog."default",
    scrape_date date,
    scrape_ts timestamp without time zone,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_shooting_stats UNIQUE (player)
);

INSERT INTO nba_source.bbref_player_shooting_stats(
	player, avg_shot_distance, pct_fga_2p, pct_fga_0_3, pct_fga_3_10, pct_fga_10_16, pct_fga_16_3p, pct_fga_3p, fg_pct_0_3, fg_pct_3_10, fg_pct_10_16, fg_pct_16_3p, pct_2pfg_ast, pct_3pfg_ast, dunk_pct_tot_fg, dunks, corner_3_ast_pct, corner_3pm_pct, heaves_att, heaves_makes, scrape_date, scrape_ts)
	VALUES ('Aaron Gordon', '15.0', '.472', '.290', '.112', '.052', '.017', '.528', '.699', '.281', '.400', '.400', '.945', '.028', '6', '.490', '.435', '.405', '1', '0', current_date, current_timestamp);

DROP TABLE IF EXISTS nba_source.reddit_posts;
CREATE TABLE IF NOT EXISTS nba_source.reddit_posts
(
    title text COLLATE pg_catalog."default",
    score bigint,
    id text COLLATE pg_catalog."default",
    url text COLLATE pg_catalog."default",
    reddit_url text COLLATE pg_catalog."default",
    num_comments bigint,
    body text COLLATE pg_catalog."default",
    scrape_date date,
    scrape_time timestamp without time zone,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_reddit_data UNIQUE (reddit_url)
);

INSERT INTO nba_source.reddit_posts(
	title, score, id, url, reddit_url, num_comments, body, scrape_date, scrape_time)
	VALUES ('Daily Discussion Thread + Game Thread Index', 67, 'y823pn', 'https://www.reddit.com/r/nba/comments/y823pn/daily_discussion_thread_game_thread_index/', 'https://www.reddit.com/r/nba/comments/y823pn/daily_discussion_thread_game_thread_index/', 89, 'z', current_date, current_timestamp);

DROP TABLE IF EXISTS nba_source.reddit_comments;
CREATE TABLE IF NOT EXISTS nba_source.reddit_comments
(
    comment text COLLATE pg_catalog."default",
    score bigint,
    url text COLLATE pg_catalog."default",
    author text COLLATE pg_catalog."default",
    flair1 text COLLATE pg_catalog."default",
    flair2 text COLLATE pg_catalog."default",
    edited text COLLATE pg_catalog."default",
    scrape_date text,
    scrape_ts text,
    compound double precision,
    neg double precision,
    neu double precision,
    pos double precision,
    sentiment bigint,
    row_col bigint,
    md5_pk text COLLATE pg_catalog."default",
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_reddit_comment_data UNIQUE (md5_pk)
);

INSERT INTO nba_source.reddit_comments(
	comment, score, url, author, flair1, flair2, edited, scrape_date, scrape_ts, compound, neg, neu, pos, sentiment, row_col, md5_pk)
	VALUES ('Hah. No way.', 0, 'https://www.reddit.com/r/nba/comments/ubkeiw/james_alexander_i_think_this_whole_nets/', 'cosmicdave86', 'Jazz1', 'Jazz', 1, '2022-04-25', '2022-04-25 11:30:37.057985', 0, 0, 1, 0, 0, null, '41b96f29ea2e52b6f371f96c66cb44dd');

DROP TABLE IF EXISTS nba_source.bbref_league_transactions;
CREATE TABLE IF NOT EXISTS nba_source.bbref_league_transactions
(
    date timestamp without time zone,
    transaction text COLLATE pg_catalog."default",
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_transactions UNIQUE (date, transaction)
);

INSERT INTO nba_source.bbref_league_transactions(
	date, transaction, scrape_date)
	VALUES ('2022-01-10 00:00:00', 'The Denver Nuggets signed James Ennis to a 10-day contract.', current_date);

DROP TABLE IF EXISTS nba_source.bbref_player_injuries;
CREATE TABLE IF NOT EXISTS nba_source.bbref_player_injuries
(
    player text COLLATE pg_catalog."default",
    team text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    date text COLLATE pg_catalog."default",
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_injury_data UNIQUE (player, team, description)
);

INSERT INTO nba_source.bbref_player_injuries(
	player, team, description, date, scrape_date)
	VALUES ('Danilo Gallinari', 'Boston Celtics', 'Out For Season (Knee) - The Celtics announced that Gallinari tore his ACL in his left knee. There is no timetable for his return.', 'Fri, Sep 2, 2022', current_date);

DROP TABLE IF EXISTS nba_source.twitter_tweepy_legacy;
CREATE TABLE IF NOT EXISTS nba_source.twitter_tweepy_legacy
(
    tweet_id text COLLATE pg_catalog."default",
    api_created_at text COLLATE pg_catalog."default",
    username text COLLATE pg_catalog."default",
    user_id double precision,
    tweet text COLLATE pg_catalog."default",
    likes double precision,
    retweets double precision,
    language text COLLATE pg_catalog."default",
    scrape_ts text,
    profile_img text COLLATE pg_catalog."default",
    url text COLLATE pg_catalog."default",
    compound double precision,
    neg double precision,
    neu double precision,
    pos double precision,
    sentiment bigint,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_twitter_tweepy_data UNIQUE (tweet_id)
);

INSERT INTO nba_source.twitter_tweepy_legacy(
	tweet_id, api_created_at, username, user_id, tweet, likes, retweets, language, scrape_ts, profile_img, url, compound, neg, neu, pos, sentiment)
	VALUES ('1546907743050612736', 'Wed Jul 13 00:20:47 +0000 2022', 'wojespn', 2379056251.0, 'ESPN Sources: EuroLeague', 1, 1, 'en', current_timestamp, 'z', 'z', 1.0, 1.0, 1.0, 1.0, 1);

DROP TABLE IF EXISTS nba_source.bbref_league_schedule;
CREATE TABLE IF NOT EXISTS nba_source.bbref_league_schedule
(
    start_time text COLLATE pg_catalog."default",
    away_team text COLLATE pg_catalog."default",
    home_team text COLLATE pg_catalog."default",
    date text COLLATE pg_catalog."default",
    proper_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp,
    CONSTRAINT unique_constraint_for_upsert_schedule UNIQUE (away_team, home_team, proper_date)
);

INSERT INTO nba_source.bbref_league_schedule(
	start_time, away_team, home_team, date, proper_date)
	VALUES ('7:00p', 'Los Angeles Lakers', 'Utah Jazz', 'Tue, May 16, 2022', '2022-03-31');

DROP TABLE IF EXISTS nba_source.bbref_player_stats_snapshot;
CREATE TABLE IF NOT EXISTS nba_source.bbref_player_stats_snapshot
(
    player text COLLATE pg_catalog."default",
    pos text COLLATE pg_catalog."default",
    age text COLLATE pg_catalog."default",
    tm text COLLATE pg_catalog."default",
    g text COLLATE pg_catalog."default",
    gs text COLLATE pg_catalog."default",
    mp text COLLATE pg_catalog."default",
    fg text COLLATE pg_catalog."default",
    fga text COLLATE pg_catalog."default",
    "fg%" text COLLATE pg_catalog."default",
    "3p" text COLLATE pg_catalog."default",
    "3pa" text COLLATE pg_catalog."default",
    "3p%" text COLLATE pg_catalog."default",
    "2p" text COLLATE pg_catalog."default",
    "2pa" text COLLATE pg_catalog."default",
    "2p%" text COLLATE pg_catalog."default",
    "efg%" text COLLATE pg_catalog."default",
    ft text COLLATE pg_catalog."default",
    fta text COLLATE pg_catalog."default",
    "ft%" text COLLATE pg_catalog."default",
    orb text COLLATE pg_catalog."default",
    drb text COLLATE pg_catalog."default",
    trb text COLLATE pg_catalog."default",
    ast text COLLATE pg_catalog."default",
    stl text COLLATE pg_catalog."default",
    blk text COLLATE pg_catalog."default",
    tov text COLLATE pg_catalog."default",
    pf text COLLATE pg_catalog."default",
    pts double precision,
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp
);

DROP TABLE IF EXISTS nba_source.bbref_team_adv_stats_snapshot;
CREATE TABLE IF NOT EXISTS nba_source.bbref_team_adv_stats_snapshot
(
    index bigint,
    team text COLLATE pg_catalog."default",
    age double precision,
    w double precision,
    l double precision,
    pw double precision,
    pl double precision,
    mov double precision,
    sos double precision,
    srs double precision,
    ortg double precision,
    drtg double precision,
    nrtg double precision,
    pace double precision,
    ftr double precision,
    "3par" double precision,
    "ts%" double precision,
    "efg%" double precision,
    "tov%" double precision,
    "orb%" double precision,
    "ft/fga" double precision,
    "efg%_opp" double precision,
    "tov%_opp" double precision,
    "drb%_opp" double precision,
    "ft/fga_opp" double precision,
    arena text COLLATE pg_catalog."default",
    attendance double precision,
    "att/game" double precision,
    scrape_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp
);

DROP TABLE IF EXISTS marts.feature_flags;
CREATE TABLE IF NOT EXISTS marts.feature_flags
(
	id serial primary key,
	flag text,
	is_enabled integer,
	created_at timestamp without time zone default now(),
	modified_at timestamp without time zone default now(),
    CONSTRAINT flag_unique UNIQUE (flag)
);
INSERT INTO marts.feature_flags(flag, is_enabled)
VALUES ('season', 1),
       ('playoffs', 1),
       ('pbp', 1),
       ('twitter', 1),
       ('reddit_posts', 1),
       ('reddit_comments', 1),
       ('boxscores', 1),
       ('injuries', 1),
       ('transactions', 1),
       ('stats', 1),
       ('adv_stats', 1),
       ('opp_stats', 1),
       ('odds', 1),
       ('schedule', 1),
       ('shooting_stats', 1),
       ('fake', 0);

CREATE TABLE nba_source.play_in_details(
    id serial primary key,
    name varchar,
    start_date date,
    end_date date,
    created_at timestamp default current_timestamp,
    modified_at timestamp default current_timestamp
);

INSERT INTO nba_source.play_in_details(name, start_date, end_date)
VALUES ('Play-In', '2025-04-15', '2025-04-18');
