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
from goodreads_silver_pages_utils import extract_start_date_str as _extract_start_date_str
from goodreads_utils import parse_date as _parse_date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, udf
from pyspark.sql.types import StringType, DateType

spark = SparkSession.builder.getOrCreate()

extract_start_date_str = udf(_extract_start_date_str, StringType())
parse_date = udf(_parse_date, DateType())

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
from pyspark.sql.functions import current_timestamp

silver = spark.table(SILVER_TABLE)

enriched = (
    silver.join(start_dates, on="book_id", how="left")
    .withColumn("_enriched_at", current_timestamp())
)

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
