# databricks-books

A Goodreads reading analytics platform built on Databricks. Ingests reading data from Goodreads RSS feeds and authenticated web scraping, enriches it with genre data from the Open Library API, and serves a Dash dashboard to visualise reading habits.

## Architecture

Medallion pipeline (Bronze → Silver → Gold) across three Databricks jobs, with a Dash web app deployed on Databricks Apps.

```
Goodreads RSS / review pages          Open Library API
        │                                    │
  books_job ──triggers──► pages_job    open_library_job
        │                      │              │
  silver_books       gold_pages_per_day   gold_genre
                                │              │
                          ┌─────┴──────────────┤
                          │      Dash App      │ 
                          └────────────────────┘
```

## Documentation

- [Pipeline](./docs/pipeline.md) — jobs, notebooks, table reference, data model
- [App](./docs/app.md) — Dash features, environment variables, key files
- [Deployment](./docs/deployment.md) — GitHub Actions, manual deployment, CLI reference
- [Permissions](./docs/permissions.md) — one-time UC grants and service principal setup

## Development

### Running tests

Tests live in `tests/` split by component:

```
tests/
  app/        — Dash app unit tests
  notebooks/  — notebook utility unit tests
```

Install test dependencies and run the full suite:

```bash
uv run --extra test pytest
```

Run a specific subdirectory:

```bash
uv run --extra test pytest tests/app
uv run --extra test pytest tests/notebooks
```

Run with verbose output:

```bash
uv run --extra test pytest -v
```

## Quick start

1. Store the Goodreads session cookie as a Databricks secret — run `notebooks/goodreads_setup.py`
2. Bind the bundle to an existing app (one-time) — trigger the `setup.yml` workflow or run:
   ```bash
   databricks bundle deployment bind goodreads_app goodreads-app
   ```
3. Push to `main` — `deploy.yml` handles the rest

See [deployment.md](./docs/deployment.md) for full details including manual steps and permission grants.
