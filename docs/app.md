# Dash App

A Plotly Dash web application served by Databricks Apps. Source code lives in `app/`.

## Features

| Feature | Description | Data source |
|---------|-------------|-------------|
| Pages Per Day | Line chart per user with configurable rolling average (1–30 days) | `goodreads.gold_pages_per_day` |
| Books in Progress | Bar chart of concurrent books per day | `goodreads.gold_pages_per_day` |
| Genre Breakdown | Bar chart by genre; toggle between avg rating, total pages, book count | `goodreads.gold_genre` |
| Job Control | "Re-ingest data" button triggers `pages_job`; polls status every 5 seconds | Databricks Jobs API |

## Key files

| File | Purpose |
|------|---------|
| `app/app.py` | Dash layout, callbacks, background task setup |
| `app/data.py` | SQL warehouse queries via Databricks SDK statement execution API |
| `app/figures/pages.py` | Pages per day line chart with rolling average |
| `app/figures/books.py` | Books in progress bar chart |
| `app/figures/genre.py` | Genre breakdown chart |
| `app/job_status.py` | Format job run task statuses |

## Environment variables

Injected via `app/app.yml` from bundle resources at deploy time:

| Variable | Bundle resource | Description |
|----------|----------------|-------------|
| `DATABRICKS_WAREHOUSE_ID` | `sql_warehouse` | SQL warehouse for queries |
| `TABLE_NAME` | — | `gold_pages_per_day` table name |
| `GENRE_TABLE_NAME` | — | `gold_genre` table name |
| `JOB_ID` | `pages_job` | pages_job ID for re-ingestion trigger |

Variables backed by bundle resources are declared in `databricks.yml`. UC table permissions (not expressible in the bundle schema) are injected separately by the deploy workflow — see [deployment.md](./deployment.md).

## Background callbacks

The job polling UI uses Dash background callbacks backed by `diskcache`. This avoids blocking the main Dash process while waiting for job status updates.
