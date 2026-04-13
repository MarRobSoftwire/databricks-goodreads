# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Silver Genres (Open Library)
# MAGIC Parses the raw Open Library JSON from `goodreads.bronze_open_library` and produces
# MAGIC a clean, typed `goodreads.silver_genres` table with one row per ISBN.
# MAGIC
# MAGIC Extracted fields: `ol_key`, `ol_title`, `subjects`, `authors`, `publishers`,
# MAGIC `publish_date`, `number_of_pages`, `cover_url`.
# MAGIC
# MAGIC Run order: `goodreads_bronze_genres` → **this notebook**

# COMMAND ----------

# DBTITLE 1,Configuration
BRONZE_TABLE = "goodreads.bronze_open_library"
SILVER_TABLE = "goodreads.silver_genres"

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
    .select("isbn", "book_id", "raw_json", "status")
)

total      = bronze_latest.count()
has_data   = bronze_latest.filter(col("status") == "ok").count()
no_data    = total - has_data
print(f"Latest batch: {latest_ts}  —  {total} rows ({has_data} ok, {no_data} not found/errored)")

# COMMAND ----------

# DBTITLE 1,Parse raw JSON into typed fields
from goodreads_silver_genres_utils import parse_open_library_record as _parse
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType, IntegerType, StringType, StructField, StructType,
)

PARSED_SCHEMA = StructType([
    StructField("ol_key",          StringType(),              True),
    StructField("ol_title",        StringType(),              True),
    StructField("subjects",        ArrayType(StringType()),   True),
    StructField("authors",         ArrayType(StringType()),   True),
    StructField("publishers",      ArrayType(StringType()),   True),
    StructField("publish_date",    StringType(),              True),
    StructField("number_of_pages", IntegerType(),             True),
    StructField("cover_url",       StringType(),              True),
])

parse_record = udf(_parse, PARSED_SCHEMA)

parsed = (
    bronze_latest
    .withColumn("_parsed", parse_record(col("raw_json")))
    .select(
        "isbn",
        "book_id",
        "status",
        col("_parsed.ol_key").alias("ol_key"),
        col("_parsed.ol_title").alias("ol_title"),
        col("_parsed.subjects").alias("subjects"),
        col("_parsed.authors").alias("authors"),
        col("_parsed.publishers").alias("publishers"),
        col("_parsed.publish_date").alias("publish_date"),
        col("_parsed.number_of_pages").alias("number_of_pages"),
        col("_parsed.cover_url").alias("cover_url"),
    )
)

display(parsed.select("isbn", "ol_title", "subjects", "publish_date", "number_of_pages"))

# COMMAND ----------

# DBTITLE 1,Write to silver Delta table
from pyspark.sql.functions import current_timestamp

(
    parsed
    .withColumn("_parsed_at", current_timestamp())
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Written {parsed.count()} records to {SILVER_TABLE}")
