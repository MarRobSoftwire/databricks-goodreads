# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Silver Transform
# MAGIC Reads the **latest ingestion batch** from `goodreads.bronze_rss` — if a book is
# MAGIC absent from the most recent RSS fetch it will not appear in silver. Casts all fields
# MAGIC to their proper types and strips HTML from free-text fields.
# MAGIC Overwrites `goodreads.silver_books` — this table can always be rebuilt from bronze.

# COMMAND ----------

# DBTITLE 1,Configuration
BRONZE_TABLE = "goodreads.goodreads.bronze_rss"
SILVER_TABLE  = "goodreads.goodreads.silver_books"

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
from goodreads_utils import parse_date as _parse_date
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType

parse_date = udf(_parse_date, DateType())

silver = (
    bronze_latest
    .withColumn("num_pages",      col("num_pages").try_cast("int"))
    .withColumn("year_published", col("year_published").try_cast("int"))
    .withColumn("average_rating", col("average_rating").try_cast("double"))
    .withColumn("user_rating",    col("user_rating").try_cast("int"))
    .withColumn("read_at",        parse_date(col("read_at")))
    .withColumn("date_added",     parse_date(col("date_added")))
    .withColumn("is_read",        col("read_at").isNotNull())
    .drop("book_description", "review")
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
