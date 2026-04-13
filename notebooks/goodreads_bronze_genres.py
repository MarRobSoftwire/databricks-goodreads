# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Bronze Genre Enrichment (Open Library)
# MAGIC Reads distinct ISBNs from `goodreads.silver_books`, fetches bibliographic data
# MAGIC (including subjects) from the [Open Library Books API](https://openlibrary.org/dev/docs/api#books),
# MAGIC and appends the raw JSON response to `goodreads.bronze_open_library`.
# MAGIC
# MAGIC ISBNs are batched into groups and sent in a single HTTP request per batch using the
# MAGIC `bibkeys` parameter — one request per `BATCH_SIZE` books rather than one per book.
# MAGIC Raw JSON is stored with no transformations — subject/genre extraction belongs in silver.
# MAGIC
# MAGIC **API:** `GET https://openlibrary.org/api/books?bibkeys=ISBN:x,ISBN:y&format=json&jscmd=data`
# MAGIC No API key required.
# MAGIC
# MAGIC Run order: `goodreads_bronze` → `goodreads_silver` → **this notebook**

# COMMAND ----------

# DBTITLE 1,Configuration
SILVER_TABLE        = "goodreads.silver_books"
BRONZE_GENRES_TABLE = "goodreads.bronze_open_library"
BATCH_SIZE          = 20   # ISBNs per request — keeps URLs well under limits
REQUEST_DELAY_S     = 1    # polite delay between batch requests

# COMMAND ----------

# DBTITLE 1,Read distinct ISBNs from silver_books
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Genre data is per-ISBN, not per-user — deduplicate across users
isbns_df = (
    spark.table(SILVER_TABLE)
    .filter(col("isbn").isNotNull() & (col("isbn") != ""))
    .select("isbn", "book_id")
    .distinct()
)

isbns = isbns_df.collect()
print(f"Found {len(isbns)} distinct ISBNs to enrich")

# COMMAND ----------

# DBTITLE 1,Fetch raw JSON from Open Library in batches
import json
import time
from goodreads_bronze_genres_utils import fetch_batch

rows = []
batches = [isbns[i:i + BATCH_SIZE] for i in range(0, len(isbns), BATCH_SIZE)]
print(f"Fetching {len(isbns)} ISBNs in {len(batches)} batch(es) of up to {BATCH_SIZE}")

for batch_num, batch in enumerate(batches, 1):
    try:
        bibkeys_str, api_url, result = fetch_batch(batch)
        # One row per ISBN in the batch — store raw JSON keyed to that ISBN
        for row in batch:
            key = f"ISBN:{row.isbn}"
            book_data = result.get(key)
            rows.append({
                "isbn":     row.isbn,
                "book_id":  row.book_id,
                "api_url":  api_url,
                "raw_json": json.dumps(book_data) if book_data is not None else None,
                "status":   "ok" if book_data is not None else "not_found",
            })
        found    = sum(1 for r in rows[-len(batch):] if r["status"] == "ok")
        not_found = len(batch) - found
        print(f"Batch {batch_num}/{len(batches)}: {found} found, {not_found} not found")
    except Exception as e:
        print(f"Batch {batch_num}/{len(batches)}: ERROR — {e}")
        for row in batch:
            rows.append({
                "isbn":     row.isbn,
                "book_id":  row.book_id,
                "api_url":  "",
                "raw_json": None,
                "status":   f"error: {e}",
            })
    time.sleep(REQUEST_DELAY_S)

ok_count  = sum(1 for r in rows if r["status"] == "ok")
bad_count = len(rows) - ok_count
print(f"\nTotal — Found: {ok_count}  |  Not found / errored: {bad_count}")
if bad_count:
    for r in rows:
        if r["status"] != "ok":
            print(f"  - {r['isbn']}: {r['status']}")

# COMMAND ----------

# DBTITLE 1,Append raw JSON to bronze Delta table
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit

schema = StructType([
    StructField("isbn",     StringType(), True),
    StructField("book_id",  StringType(), True),
    StructField("api_url",  StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("status",   StringType(), True),
])

df = (
    spark.createDataFrame(rows, schema=schema)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("open_library"))
)

(
    df.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_GENRES_TABLE)
)

print(f"Appended {len(rows)} records to {BRONZE_GENRES_TABLE}")
