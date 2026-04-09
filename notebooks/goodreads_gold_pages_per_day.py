# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Gold: Estimated Pages Read Per Day
# MAGIC Reads from `goodreads.silver_books_enriched` and models estimated pages read
# MAGIC per calendar day.
# MAGIC
# MAGIC **Model:** for each fully-read book, `pages_per_day = num_pages / reading_days`
# MAGIC is distributed uniformly across every day in `[started_reading, read_at]`.
# MAGIC The first and last day of each book are weighted at 0.5 to smooth boundary spikes
# MAGIC where one book ends and another begins on the same day.
# MAGIC Daily weighted contributions from concurrently-read books are summed.
# MAGIC
# MAGIC Books without a recorded start date are skipped and logged.

# COMMAND ----------

# DBTITLE 1,Configuration
SILVER_TABLE = "goodreads.silver_books_enriched"
GOLD_TABLE   = "goodreads.gold_pages_per_day"

# COMMAND ----------

# DBTITLE 1,Load and filter
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

silver = spark.table(SILVER_TABLE)

skipped = (
    silver
    .filter(
        col("started_reading").isNull() |
        col("read_at").isNull() |
        col("num_pages").isNull() |
        (col("num_pages") <= 0)
    )
    .select("title", "started_reading", "read_at", "num_pages")
)
skipped_count = skipped.count()
if skipped_count > 0:
    print(f"Skipping {skipped_count} book(s) with incomplete data:")
    for row in skipped.collect():
        print(f"  - {row.title} (started {row.started_reading}, finished {row.read_at}, {row.num_pages} pages)")

filtered = (
    silver
    .filter(col("started_reading").isNotNull())
    .filter(col("read_at").isNotNull())
    .filter(col("num_pages").isNotNull())
    .filter(col("num_pages") > 0)
)

print(f"\nModelling {filtered.count()} books")

# COMMAND ----------

# DBTITLE 1,Compute pages_per_day per book
from pyspark.sql.functions import datediff, greatest, lit

modelled = (
    filtered
    .withColumn("reading_days", greatest(datediff(col("read_at"), col("started_reading")), lit(1)))
    .withColumn("base_pages_per_day", col("num_pages") / col("reading_days"))
)

display(modelled.select("title", "started_reading", "read_at", "num_pages", "reading_days", "base_pages_per_day"))

# COMMAND ----------

# DBTITLE 1,Explode into one row per calendar day per book
from pyspark.sql.functions import sequence, explode, when, lit

daily = (
    modelled
    .withColumn("date", explode(sequence(col("started_reading"), col("read_at"))))
    .withColumn(
        "weight",
        when(
            col("started_reading") == col("read_at"), lit(1.0)
        ).when(
            (col("date") == col("started_reading")) | (col("date") == col("read_at")), lit(0.5)
        ).otherwise(lit(1.0))
    )
    .withColumn("pages_per_day", col("base_pages_per_day") * col("weight"))
    .select("date", "title", "pages_per_day")
)

print(f"Total daily rows: {daily.count():,}")

# COMMAND ----------

# DBTITLE 1,Aggregate and display
from pyspark.sql.functions import sum as spark_sum, round as spark_round, collect_list

result = (
    daily
    .groupBy("date")
    .agg(
        spark_round(spark_sum("pages_per_day"), 1).alias("est_pages_read"),
        collect_list("title").alias("books_in_progress"),
    )
    .orderBy("date")
)

display(result)

# COMMAND ----------

# DBTITLE 1,Write to gold Delta table
(
    result.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

print(f"Written {result.count()} daily records to {GOLD_TABLE}")
