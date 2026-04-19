---
name: nba-odds-scraper
description: >
  Scrape NBA game odds from DraftKings (via ESPN's DraftKings odds page and supplementary sources) and generate SQL INSERT statements for the bronze.draftkings_game_odds table. Use this skill whenever the user asks to pull NBA odds, scrape DraftKings lines, generate odds INSERT statements, load game odds into the database, or anything related to collecting NBA spread/moneyline/total data for games on a given date. Also trigger when the user mentions "odds inserts", "DraftKings odds", "game lines", or "pull odds for today's games".
---

# NBA Odds Scraper

This skill collects NBA game odds from DraftKings and produces SQL INSERT statements targeting the `bronze.draftkings_game_odds` table.

## Target Table DDL

```sql
CREATE TABLE bronze.draftkings_game_odds (
    team text NULL,
    spread text NULL,
    total text NULL,
    moneyline float8 NULL,
    "date" date NULL,
    datetime1 timestamp NULL,
    scrape_ts timestamp NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    modified_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT unique_constraint_for_upsert_aws_odds_source UNIQUE (team, date),
    CONSTRAINT unique_constraint_for_upsert_draftkings_game_odds UNIQUE (team, date),
    CONSTRAINT unique_constraint_for_upsert_odds UNIQUE (team, date)
);
```

## Workflow

### Step 1: Identify the games

The user will provide either:
- A date and ask you to pull all NBA games for that date, OR
- A specific list of games (possibly via pasted schedule data)

If the user provides schedule data, extract all unique matchups from it. Each game produces **two rows** (one per team).

### Step 2: Search for DraftKings odds

The primary source is the **ESPN NBA Odds page** which displays DraftKings lines:
- Search: `ESPN NBA odds DraftKings [date]`
- Fetch: `https://www.espn.com/nba/odds`

The ESPN odds page contains DraftKings spread, total, and moneyline for each game. Parse the data carefully — the format from ESPN is dense and interleaved.

**How to read the ESPN odds page output:**

Each game block follows this pattern:
```
[datetime]OpenSpreadTotalML
[Away Team][Abbrev] · (record) · [open spread] · [spread juice] · [current spread] · [spread juice] · [over total] · [over juice] · [moneyline]
[Home Team][Abbrev] · (record) · [open spread or total] · [juice] · [current spread] · [spread juice] · [under total] · [under juice] · [moneyline]
```

Example:
```
Boston CelticsCelticsBOS · (42-21) · -1.5 · EVEN · +1.5 · -120 · o223.5 · -115 · -105
Cleveland CavaliersCavaliersCLE · (39-24) · u225.5 · -110 · -1.5 · EVEN · u223.5 · -105 · -115
```

This means:
- BOS: spread +1.5, moneyline -105
- CLE: spread -1.5, moneyline -115
- Total: 223.5 (use the current total, not the opening total)

If ESPN data is incomplete or stale (check that the dates match), supplement with targeted searches:
- `[Team1] [Team2] odds spread moneyline DraftKings [date]`
- Sources like CBS Sports, FanDuel Research, SportsBettingDime, and Covers often report DraftKings lines explicitly

Prioritize DraftKings-sourced lines. If DraftKings-specific data isn't available for a game, use the consensus line (BetMGM, FanDuel lines are usually within 0.5-1 point) and note this to the user.

### Step 3: Generate INSERT statements

Produce a single INSERT statement with all rows. Follow these formatting rules exactly:

#### Column mappings:

| Column | Value | Example |
|--------|-------|---------|
| `team` | 3-letter uppercase NBA acronym | `'BOS'` |
| `spread` | Signed spread as text, favorite is negative | `'+1.5'`, `'-5.5'` |
| `total` | Over/under number as text, no prefix characters | `'223.5'` |
| `moneyline` | American odds as float, "EVEN" = 100 | `-105`, `100` |
| `date` | Game date in YYYY-MM-DD | `'2026-03-08'` |
| `datetime1` | Game tip-off timestamp in ET | `'2026-03-08 13:00:00'` |
| `scrape_ts` | Use `NOW()` | `NOW()` |

#### Team acronyms:

Use standard NBA abbreviations:
```
ATL, BOS, BKN, CHA, CHI, CLE, DAL, DEN, DET, GSW,
HOU, IND, LAC, LAL, MEM, MIA, MIL, MIN, NOP, NYK,
OKC, ORL, PHI, PHX, POR, SAC, SAS, TOR, UTA, WAS
```

#### Key rules:

1. **Two rows per game** — one for each team
2. **`total` column** — Both teams in a game get the same total value (just the number, no `o`/`u` prefix). Example: `'223.5'`
3. **`spread` column** — The favorite gets a negative spread, the underdog gets a positive spread. Always include the sign. Example: `'-3.5'`, `'+3.5'`
4. **`moneyline` column** — Use American odds as a number. Convert "EVEN" to `100`. Favorites are negative, underdogs are positive.
5. **`datetime1`** — Use Eastern Time for all game times
6. **Comment each game** with `-- AWAY @ HOME (time ET)` for readability
7. **Sort games chronologically** by tip-off time

#### Output template:

```sql
INSERT INTO bronze.draftkings_game_odds (team, spread, total, moneyline, date, datetime1, scrape_ts)
VALUES
-- BOS @ CLE (1:00 PM ET)
('BOS', '+1.5', '223.5', -105, '2026-03-08', '2026-03-08 13:00:00', NOW()),
('CLE', '-1.5', '223.5', -115, '2026-03-08', '2026-03-08 13:00:00', NOW()),

-- NYK @ LAL (3:30 PM ET)
('NYK', '-3.5', '226.5', -148, '2026-03-08', '2026-03-08 15:30:00', NOW()),
('LAL', '+3.5', '226.5', 124, '2026-03-08', '2026-03-08 15:30:00', NOW());
```

### Step 4: Present to user

After generating the SQL, present it directly in the chat. If there are any games where DraftKings-specific odds were unavailable, note which games used alternative sportsbook sources.

## Common pitfalls

- The ESPN odds page sometimes shows cached/old data from a different date. Always verify the dates in the ESPN response match the requested date. If stale, rely on individual game searches.
- ESPN times are in UTC (e.g., `2026-03-08T17:00Z` = 1:00 PM ET). Convert to ET for the `datetime1` column.
- "EVEN" moneyline means +100 or -100 depending on context. Default to `100`.
- Some games may not have odds posted yet if they're far in the future. Let the user know.
- The total is the same number for both teams in a matchup — do not differentiate over/under in the `total` column.