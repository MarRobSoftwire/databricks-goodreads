# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Bronze Ingest
# MAGIC Downloads the Goodreads RSS feed and **appends** raw records to the bronze table.
# MAGIC All fields stored as STRING. No transformations applied — raw HTML is preserved.
# MAGIC After this runs, execute `goodreads_silver` to produce the curated table.

# COMMAND ----------

# DBTITLE 1,Configuration
RSS_URL = (
    "https://www.goodreads.com/review/list_rss/178442944"
    "?shelf=read"
)

BRONZE_TABLE = "goodreads.bronze_rss"

# COMMAND ----------

# DBTITLE 1,Download RSS feed
import urllib.request

with urllib.request.urlopen(RSS_URL) as response:
    rss_bytes = response.read()

print(f"Downloaded {len(rss_bytes):,} bytes")

# COMMAND ----------

# DBTITLE 1,Parse XML — no transformations applied
import xml.etree.ElementTree as ET

root = ET.fromstring(rss_bytes)
channel = root.find("channel")

books = []
for item in channel.findall("item"):

    def text(tag):
        el = item.find(tag)
        return (el.text or "").strip() if el is not None else ""

    books.append({
        "title":            text("title"),
        "author":           text("author_name"),
        "isbn":             text("isbn"),
        "book_id":          text("book_id"),
        "num_pages":        item.findtext("book/num_pages", default=""),
        "year_published":   text("book_published"),
        "average_rating":   text("average_rating"),
        "user_rating":      text("user_rating"),
        "read_at":          text("user_read_at"),
        "date_added":       text("user_date_added"),
        "shelves":          text("user_shelves"),
        "review":           text("user_review"),           # raw HTML preserved
        "book_description": text("book_description"),      # raw HTML preserved
        "cover_url":        text("book_medium_image_url"),
        "goodreads_url":    text("link"),
    })

print(f"Parsed {len(books)} books")

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
    .saveAsTable(BRONZE_TABLE)
)

print(f"Appended {len(books)} records to {BRONZE_TABLE}")
