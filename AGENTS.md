# AGENTS.md

## Repo Shape

- Python ingestion project for NBA ELT data.
- Core runtime code lives in `src/`.
- Scraper entry points live in `src/scrapers.py` as `get_*_data` functions.
- Database bootstrap and test compose files live in `docker/`.
- Tests live in `tests/unit` and `tests/integration`, with shared fixtures in `tests/conftest.py` and `tests/fixtures`.

## Feature Flags

Feature flags are database-backed and are loaded once before the scraper calls run.

- `src/app.py` creates a SQLAlchemy engine from `RDS_*` environment variables.
- `FeatureFlagManager.load(engine=engine)` reads `gold.feature_flags`.
- `src/feature_flags.py` stores flags in the class-level `FeatureFlagManager._flags` dict, keyed by `flag`.
- `FeatureFlagManager.get(flag)` returns the integer `is_enabled` value, or `None` when the flag has not been loaded/found.

Most scraper functions are wrapped with `@check_feature_flag_decorator(flag_name="...")` from `src/decorators.py`.
The wrapper checks `FeatureFlagManager` at call time:

- missing flag: raise `ValueError`;
- disabled flag (`0`/falsey): log and return an empty `pd.DataFrame`;
- enabled flag (`1`/truthy): execute the scraper normally.

When adding a new scraper, add the flag to `gold.feature_flags` in `docker/postgres_bootstrap.sql`, decorate the scraper function, and make sure tests load the flag through the normal fixture path.

The `season` and `playoffs` flags are also read directly in `src/app.py` to build `schedule_months_to_pull` via `generate_schedule_pull_type`.

## Tests

Use the Docker-backed flow when possible:

```bash
make test
```

This runs `docker/docker-compose-test.yml`, starts a real `postgres:16-alpine` container, bootstraps it with `docker/postgres_bootstrap.sql`, then runs pytest inside the `ingestion_script_test_runner` container.

For local pytest against a Docker Postgres container:

```bash
make ci-test
```

`make ci-test` starts the Postgres service, runs:

```bash
uv run --frozen pytest -vv --cov-report term --cov-report xml:coverage.xml --cov=src --color=yes
```

and then stops the container.

`tests/conftest.py` is important:

- it blocks real internet sockets, so tests should use fixtures/mocks instead of live network calls;
- it creates a SQLAlchemy engine pointed at Docker Postgres;
- the session-scoped `load_feature_flags` fixture calls `FeatureFlagManager.load(engine=postgres_conn)`;
- scraper fixtures patch HTTP calls and feed data from `tests/fixtures`.

Integration tests are real database integration tests, not pure mocks. Keep schema assumptions aligned with `docker/postgres_bootstrap.sql`.

## Working Notes

- Prefer extending existing scraper/decorator patterns over introducing new orchestration paths.
- Keep scraper outputs as `pd.DataFrame` objects; disabled scrapers should continue to produce empty frames through the decorator.
- Do not bypass `FeatureFlagManager` in scraper code unless the caller truly needs direct flag semantics like the schedule month selection does.
- If a test needs feature flags, update the bootstrap SQL instead of manually mutating `FeatureFlagManager._flags` in integration coverage.
