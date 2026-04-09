# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Silver Review Pages
# MAGIC Parses `started_reading` date from raw HTML in `goodreads.bronze_review_pages`,
# MAGIC then joins with `goodreads.silver_books` to produce an enriched
# MAGIC `goodreads.silver_books_enriched` table with the start date alongside all existing fields.
# MAGIC
# MAGIC Run order: `goodreads_bronze` → `goodreads_silver` → `goodreads_bronze_pages` → **this notebook**

# COMMAND ----------

# MAGIC %pip install beautifulsoup4 python-dateutil

# COMMAND ----------

# DBTITLE 1,Configuration
BRONZE_PAGES_TABLE = "goodreads.bronze_review_pages"
SILVER_TABLE       = "goodreads.silver_books"
SILVER_OUT_TABLE   = "goodreads.silver_books_enriched"

# COMMAND ----------

# DBTITLE 1,Parse started_reading date from raw HTML
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, udf
from pyspark.sql.types import StringType, DateType

spark = SparkSession.builder.getOrCreate()

@udf(StringType())
def extract_start_date_str(raw_html):
    """
    Finds the readingTimeline row containing 'Started Reading'
    and returns the raw date string (e.g. 'March 29, 2026').
    Returns None if no start date is recorded.
    """
    if not raw_html:
        return None
    soup = BeautifulSoup(raw_html, "html.parser")
    matches = [
        row.get_text(separator=" ", strip=True)
        for row in soup.find_all("div", class_="readingTimeline__text")
        if "Started Reading" in row.get_text() and "–" in row.get_text()
    ]
    if not matches:
        return None
    # Use the last entry in case the book was read multiple times
    return matches[-1].split("–")[0].strip()

@udf(DateType())
def parse_date(raw):
    if not raw:
        return None
    return dateutil_parser.parse(raw).date()

# COMMAND ----------

# DBTITLE 1,Read latest bronze_review_pages batch
bronze_pages = spark.table(BRONZE_PAGES_TABLE)
latest_ts = bronze_pages.agg(spark_max("_ingested_at")).collect()[0][0]

pages_latest = (
    bronze_pages
    .filter(col("_ingested_at") == latest_ts)
    .select("book_id", "raw_html")
)

print(f"Latest pages batch: {latest_ts}  —  {pages_latest.count()} pages")

# COMMAND ----------

# DBTITLE 1,Extract started_reading date per book
start_dates = (
    pages_latest
    .withColumn("started_reading_raw", extract_start_date_str(col("raw_html")))
    .withColumn("started_reading", parse_date(col("started_reading_raw")))
    .select("book_id", "started_reading")
)

display(start_dates)

# COMMAND ----------

# DBTITLE 1,Join with silver_books and write enriched table
silver = spark.table(SILVER_TABLE)

enriched = silver.join(start_dates, on="book_id", how="left")

display(enriched.select("title", "author", "started_reading", "read_at", "user_rating"))

# COMMAND ----------

# DBTITLE 1,Write to silver_books_enriched
(
    enriched.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_OUT_TABLE)
)

print(f"Written {enriched.count()} records to {SILVER_OUT_TABLE}")
