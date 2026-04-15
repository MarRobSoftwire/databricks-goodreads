# Dash App

Source code in `app/`. Charts: pages per day (rolling avg), books in progress, genre breakdown. A "Re-ingest data" button triggers `pages_job` and polls status every 5 seconds using Dash background callbacks (diskcache).

## Environment variables

Injected via `app/app.yml` from bundle resources:

| Variable | Source | Description |
|----------|--------|-------------|
| `DATABRICKS_WAREHOUSE_ID` | `sql_warehouse` resource | SQL warehouse for queries |
| `TABLE_NAME` | — | `gold_pages_per_day` table |
| `GENRE_TABLE_NAME` | — | `gold_genre` table |
| `JOB_ID` | `pages_job` resource | Job ID for re-ingestion |

UC table permissions can't be expressed in the bundle schema — they're patched in by CI via `app/uc-extras.json`. See [deployment.md](./deployment.md).
