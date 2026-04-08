# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads "Read" Shelf — RSS to CSV
# MAGIC Downloads the Goodreads RSS feed for the **read** shelf and saves the book list as a Delta table / CSV.

# COMMAND ----------

# DBTITLE 1,Configuration
RSS_URL = (
    "https://www.goodreads.com/review/list_rss/178442944"
    "?shelf=read"
)

# Output path — adjust to your DBFS or Unity Catalog volume as needed
OUTPUT_PATH = "/tmp/goodreads_read_shelf.csv"

# COMMAND ----------

# DBTITLE 1,Download RSS feed
import urllib.request

with urllib.request.urlopen(RSS_URL) as response:
    rss_bytes = response.read()

print(f"Downloaded {len(rss_bytes):,} bytes")

# COMMAND ----------

# DBTITLE 1,Parse XML and extract book fields
import xml.etree.ElementTree as ET
import re
from html import unescape

def strip_html(raw: str) -> str:
    """Remove HTML tags and unescape entities."""
    return unescape(re.sub(r"<[^>]+>", "", raw or "")).strip()

root = ET.fromstring(rss_bytes)
channel = root.find("channel")

books = []
for item in channel.findall("item"):

    def text(tag):
        el = item.find(tag)
        return (el.text or "").strip() if el is not None else ""

    books.append({
        "title":           text("title"),
        "author":          text("author_name"),
        "isbn":            text("isbn"),
        "book_id":         text("book_id"),
        "num_pages":       item.findtext("book/num_pages", default=""),
        "year_published":  text("book_published"),
        "average_rating":  text("average_rating"),
        "user_rating":     text("user_rating"),
        "read_at":         text("user_read_at"),
        "date_added":      text("user_date_added"),
        "shelves":         text("user_shelves"),
        "review":          strip_html(text("user_review")),
        "book_description": strip_html(text("book_description")),
        "cover_url":       text("book_medium_image_url"),
        "goodreads_url":   text("link"),
    })

print(f"Parsed {len(books)} books")
books[:2]

# COMMAND ----------

# DBTITLE 1,Create Spark DataFrame and display
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([StructField(col, StringType(), True) for col in books[0].keys()])
df = spark.createDataFrame(books, schema=schema)

df.select("title", "author", "year_published", "num_pages", "user_rating", "average_rating", "read_at").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Save to CSV (single file)
(
    df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(OUTPUT_PATH)
)

print(f"CSV written to {OUTPUT_PATH}")

