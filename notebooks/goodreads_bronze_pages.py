# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Bronze Review Pages
# MAGIC Reads the clean book list from `goodreads.silver_books`, fetches the raw HTML of each
# MAGIC Goodreads review page using an authenticated session cookie, and appends it to
# MAGIC `goodreads.bronze_review_pages`.
# MAGIC
# MAGIC Raw HTML is stored with no transformations — parsing (e.g. start date extraction) belongs in silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites — one-time setup
# MAGIC
# MAGIC Before running this notebook, store your Goodreads session cookie as a Databricks secret.
# MAGIC
# MAGIC **Step 1 — Get the cookie**
# MAGIC 1. Log into [Goodreads](https://www.goodreads.com) in your browser
# MAGIC 2. Open DevTools (F12) → **Application** → **Cookies** → `https://www.goodreads.com`
# MAGIC 3. Copy the value of `_session_id`
# MAGIC
# MAGIC **Step 2 — Store as a Databricks secret (run once in a terminal with the Databricks CLI)**
# MAGIC ```
# MAGIC databricks secrets create-scope goodreads
# MAGIC databricks secrets put-secret goodreads session_id --string-value "<paste value here>"
# MAGIC ```
# MAGIC
# MAGIC **Re-running after cookie expiry**
# MAGIC
# MAGIC If the notebook raises `RuntimeError: Goodreads returned a login page`, your session has expired.
# MAGIC Repeat Step 1 and re-run the `put-secret` command above, then re-run this notebook.

# COMMAND ----------

# DBTITLE 1,Configuration
SILVER_TABLE       = "goodreads.silver_books"
BRONZE_PAGES_TABLE = "goodreads.bronze_review_pages"
REQUEST_DELAY_S    = 2  # polite delay between HTTP requests

# COMMAND ----------

# DBTITLE 1,Load session cookie from Databricks secret
session_cookie = dbutils.secrets.get(scope="goodreads", key="session_id")
print("Session cookie loaded.")

# COMMAND ----------

# DBTITLE 1,Read review URLs from silver_books
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

urls_df = spark.table(SILVER_TABLE).select("book_id", "goodreads_url").filter(col("goodreads_url") != "")
print(f"Found {urls_df.count()} books to fetch.")
display(urls_df)

# COMMAND ----------

# DBTITLE 1,Fetch raw HTML from each review page
import urllib.request
import time

def assert_authenticated(html: str, book_id: str):
    if book_id not in html:
        print(f"[{book_id}] Auth failed — HTML preview: {html[:500].strip()!r}")
        raise RuntimeError(
            f"Book ID '{book_id}' not found in response. "
            "Cookie may have expired — update the Databricks secret and re-run."
        )

rows = []
for row in urls_df.collect():
    req = urllib.request.Request(
        row.goodreads_url,
        headers={"Cookie": f"_session_id={session_cookie}"}
    )
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8")

    assert_authenticated(html, row.book_id)

    rows.append({
        "book_id":       row.book_id,
        "goodreads_url": row.goodreads_url,
        "raw_html":      html,
    })
    print(f"Fetched book ID: {row.book_id}. Found {len(html):,} characters")
    time.sleep(REQUEST_DELAY_S)

print(f"\nSuccessfully fetched {len(rows)} pages.")

# COMMAND ----------

# DBTITLE 1,Append raw HTML to bronze Delta table
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit

schema = StructType([
    StructField("book_id",       StringType(), True),
    StructField("goodreads_url", StringType(), True),
    StructField("raw_html",      StringType(), True),
])

df = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("review_page"))
)

(
    df.write
    .mode("append")
    .saveAsTable(BRONZE_PAGES_TABLE)
)

print(f"Appended {len(rows)} records to {BRONZE_PAGES_TABLE}")
