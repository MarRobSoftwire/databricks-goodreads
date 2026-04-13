# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Bronze Ingest
# MAGIC Downloads the Goodreads RSS feed and **appends** raw records to the bronze table.
# MAGIC All fields stored as STRING. No transformations applied — raw HTML is preserved.
# MAGIC After this runs, execute `goodreads_silver` to produce the curated table.

# COMMAND ----------

# DBTITLE 1,Configuration
RSS_URLS = [
    "https://www.goodreads.com/review/list_rss/178442944?shelf=read",
    "https://www.goodreads.com/review/list_rss/179258507?shelf=read",
]

BRONZE_TABLE = "goodreads.bronze_rss"

# COMMAND ----------

# DBTITLE 1,Download and parse RSS feeds
import urllib.request
from goodreads_bronze_utils import parse_rss_items

books = []
for url in RSS_URLS:
    with urllib.request.urlopen(url) as response:
        rss_bytes = response.read()
    user_books = parse_rss_items(rss_bytes)
    books.extend(user_books)
    username = user_books[0]["username"] if user_books else url
    print(f"[{username}] Downloaded {len(rss_bytes):,} bytes, parsed {len(user_books)} books")

print(f"Total: {len(books)} books across {len(RSS_URLS)} user(s)")

# COMMAND ----------

# DBTITLE 1,Create Spark DataFrame with ingestion metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

schema = StructType([StructField(col, StringType(), True) for col in books[0].keys()])
df = (
    spark.createDataFrame(books, schema=schema)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("rss"))
)

df.select("title", "author", "year_published", "num_pages", "user_rating", "average_rating", "read_at").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Append to bronze Delta table
(
    df.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"Appended {len(books)} records to {BRONZE_TABLE}")
