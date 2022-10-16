CREATE SCHEMA nba_source;
SET search_path TO nba_source;

DROP TABLE IF EXISTS aws_players_source;
CREATE TABLE aws_players_source
(
    player             varchar       PRIMARY KEY,
    team                varchar  NOT NULL,
    avg_ppg             float       NOT NULL
);

INSERT INTO aws_players_source (player, team, avg_ppg)
VALUES ('Stephen Curry', 'Golden State Warriors', '28.4'),
       ('Klay Thompson', 'Golden State Warriors', '20.2'),
       ('Jordan Poole', 'Golden State Warriors', '18.3')