# Pipeline

Medallion architecture across three independent jobs. Bronze = append-only; Silver/Gold = overwrite each run.

## books_job → pages_job

| Task | Notebook | Output table |
|------|----------|-------------|
| `bronze_books` | `goodreads_bronze.py` | `goodreads.bronze_rss` |
| `silver_books` | `goodreads_silver.py` | `goodreads.silver_books` |
| `run_pages_job` | _(triggers pages_job)_ | — |
| `bronze_review_pages` | `goodreads_bronze_pages.py` | `goodreads.bronze_review_pages` |
| `silver_review_pages` | `goodreads_silver_pages.py` | `goodreads.silver_books_enriched` |
| `gold_pages_per_day` | `goodreads_gold_pages_per_day.py` | `goodreads.gold_pages_per_day` |

`bronze_review_pages` fetches authenticated Goodreads pages using a session cookie stored as a Databricks secret (see `goodreads_setup.py`).

`gold_pages_per_day` distributes `num_pages / reading_days` uniformly across `[started_reading, read_at]`, weighting first/last day at 0.5. Includes a full date spine so zero-reading days appear as `0.0`.

## open_library_job

| Task | Notebook | Output table |
|------|----------|-------------|
| `bronze_open_library` | `goodreads_bronze_open_library.py` | `goodreads.bronze_open_library` |
| `silver_open_library` | `goodreads_silver_open_library.py` | `goodreads.silver_open_library` |
| `gold_genre` | `goodreads_gold_genre.py` | `goodreads.gold_genre` |

`gold_genre` maps Open Library subjects to 12 genre categories and aggregates `avg_user_rating`, `total_pages`, `book_count` per `(username, genre)`.
