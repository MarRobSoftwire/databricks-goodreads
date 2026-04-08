# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Silver Transform
# MAGIC Reads the **latest ingestion batch** from `goodreads.bronze_rss` — if a book is
# MAGIC absent from the most recent RSS fetch it will not appear in silver. Casts all fields
# MAGIC to their proper types and strips HTML from free-text fields.
# MAGIC Overwrites `goodreads.silver_books` — this table can always be rebuilt from bronze.

# COMMAND ----------

# DBTITLE 1,Configuration
BRONZE_TABLE = "goodreads.bronze_rss"
SILVER_TABLE  = "goodreads.silver_books"

# COMMAND ----------

# DBTITLE 1,Read latest ingestion batch from bronze
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

spark = SparkSession.builder.getOrCreate()

bronze = spark.table(BRONZE_TABLE)
latest_ts = bronze.agg(spark_max("_ingested_at")).collect()[0][0]

bronze_latest = (
    bronze
    .filter(col("_ingested_at") == latest_ts)
    .filter(col("book_id") != "")
    .drop("_source")
)

print(f"Latest ingestion: {latest_ts}  —  {bronze_latest.count()} books")

# COMMAND ----------

# DBTITLE 1,Cast types and clean text fields
from pyspark.sql.functions import to_date

silver = (
    bronze_latest
    .withColumn("num_pages",        col("num_pages").cast("int"))
    .withColumn("year_published",   col("year_published").cast("int"))
    .withColumn("average_rating",   col("average_rating").cast("double"))
    .withColumn("user_rating",      col("user_rating").cast("int"))
    # Goodreads RSS dates are in RFC-2822 format, e.g. "Thu, 03 Apr 2025 00:00:00 -0700"
    .withColumn("read_at",          to_date(col("read_at"),   "EEE, dd MMM yyyy HH:mm:ss Z"))
    .withColumn("date_added",       to_date(col("date_added"), "EEE, dd MMM yyyy HH:mm:ss Z"))
    .withColumn("is_read",          col("read_at").isNotNull())
    .drop("book_description")
    .drop("review")
)

silver.select("title", "author", "year_published", "num_pages", "user_rating", "average_rating", "read_at", "is_read").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Write to silver Delta table
(
    silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Written {silver.count()} records to {SILVER_TABLE}")
