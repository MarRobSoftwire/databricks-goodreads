# Pipeline

The pipeline follows the medallion architecture (Bronze → Silver → Gold) across three independent Databricks jobs. Bronze tables are append-only; Silver and Gold tables are fully overwritten on each run.

## Jobs

### books_job (`goodreads-books-job`)

| Task | Notebook | Output table |
|------|----------|-------------|
| `bronze_books` | `goodreads_bronze.py` | `goodreads.bronze_rss` |
| `silver_books` | `goodreads_silver.py` | `goodreads.silver_books` |
| `run_pages_job` | _(triggers pages_job)_ | — |

**bronze_books** — Downloads Goodreads RSS feeds for each configured user. Stores all fields as raw strings. Appends new records; never overwrites.

**silver_books** — Reads the latest ingestion batch. Casts types (int, double, date), strips HTML from text fields, derives `is_read` from `read_at`.

---

### pages_job (`goodreads-pages-job`)

Triggered by `books_job` after `silver_books` completes.

| Task | Notebook | Output table |
|------|----------|-------------|
| `bronze_review_pages` | `goodreads_bronze_pages.py` | `goodreads.bronze_review_pages` |
| `silver_review_pages` | `goodreads_silver_pages.py` | `goodreads.silver_books_enriched` |
| `gold_pages_per_day` | `goodreads_gold_pages_per_day.py` | `goodreads.gold_pages_per_day` |

**bronze_review_pages** — Fetches each book's Goodreads review page using an authenticated session cookie (stored as a Databricks secret). Stores raw HTML with a 2-second polite delay between requests.

**silver_review_pages** — Parses "Started Reading" date from the HTML using BeautifulSoup. Joins with `silver_books` to produce `silver_books_enriched`.

**gold_pages_per_day** — Estimates pages read per user per day:
- `pages_per_day = num_pages / reading_days` distributed uniformly across `[started_reading, read_at]`
- First and last day weighted 0.5 to smooth boundaries
- Concurrent books are summed per day
- A full date spine is included so days with no reading appear as `0.0`

Output columns: `username`, `date`, `est_pages_read`, `books_in_progress` (array of titles).

---

### open_library_job (`goodreads-open-library-job`)

Run independently of books_job/pages_job.

| Task | Notebook | Output table |
|------|----------|-------------|
| `bronze_open_library` | `goodreads_bronze_open_library.py` | `goodreads.bronze_open_library` |
| `silver_open_library` | `goodreads_silver_open_library.py` | `goodreads.silver_open_library` |
| `gold_genre` | `goodreads_gold_genre.py` | `goodreads.gold_genre` |

**bronze_open_library** — Fetches ISBNs from `silver_books` and calls the Open Library Books API in batches of 20. Stores raw JSON responses with status: `ok`, `not_found`, or `error`.

**silver_open_library** — Parses JSON responses. One row per ISBN. Fields: `ol_key`, `ol_title`, `subjects`, `authors`, `publishers`, `publish_date`, `number_of_pages`, `cover_url`.

**gold_genre** — Joins `silver_books` with `silver_open_library`. Maps Open Library subjects to genre categories, then aggregates per `(username, genre)`.

Supported genres: Science Fiction, Fantasy, Historical Fiction, Mystery & Crime, Thriller, Romance, Dystopian, Young Adult, Literary Fiction, Graphic Novel, LGBTQ+, Action & Adventure.

Output columns: `username`, `genre`, `avg_user_rating`, `total_pages`, `book_count`.

---

## Notebook conventions

Each pipeline notebook has a corresponding `*_utils.py` file containing the core logic. Notebooks are thin orchestration wrappers; all testable logic lives in the utils module. Tests are co-located in `notebooks/` and run by the `test.yml` CI workflow.

## Initial setup

Before running `pages_job` for the first time, store the Goodreads session cookie as a Databricks secret by running `notebooks/goodreads_setup.py`.
