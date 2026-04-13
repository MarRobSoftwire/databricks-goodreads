# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Gold: Genre
# MAGIC Joins `goodreads.silver_books` with `goodreads.silver_open_library` to produce
# MAGIC a per-user, per-genre summary.
# MAGIC
# MAGIC **Metrics per (username, genre):**
# MAGIC - `avg_user_rating` — mean of the user's own star ratings for books in that genre
# MAGIC - `total_pages` — sum of `num_pages` across all read books in that genre
# MAGIC - `book_count` — number of read books in that genre
# MAGIC
# MAGIC Only books where `is_read = true`, `user_rating > 0`, and `num_pages > 0` are included.
# MAGIC Books without a match in `silver_open_library` (no ISBN / not in Open Library) are excluded.
# MAGIC A single book can contribute to multiple genres.
# MAGIC
# MAGIC Run order: `goodreads_silver` + `goodreads_silver_open_library` → **this notebook**

# COMMAND ----------

# DBTITLE 1,Configuration
SILVER_BOOKS_TABLE = "goodreads.silver_books"
SILVER_OL_TABLE    = "goodreads.silver_open_library"
GOLD_TABLE         = "goodreads.gold_genre"

# COMMAND ----------

# DBTITLE 1,Load silver_books — read books with valid rating and page count
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

silver_books = (
    spark.table(SILVER_BOOKS_TABLE)
    .filter(col("is_read"))
    .filter(col("user_rating").isNotNull() & (col("user_rating") > 0))
    .filter(col("num_pages").isNotNull()   & (col("num_pages")   > 0))
    .select("username", "book_id", "user_rating", "num_pages")
)

print(f"Qualifying read books: {silver_books.count()}")

# COMMAND ----------

# DBTITLE 1,Load silver_open_library — books with subject data
silver_ol = (
    spark.table(SILVER_OL_TABLE)
    .filter(col("status") == "ok")
    .select("book_id", "subjects")
)

print(f"Books with Open Library subjects: {silver_ol.count()}")

# COMMAND ----------

# DBTITLE 1,Join and map subjects to genres
from goodreads_gold_genre_utils import subject_to_genres as _subject_to_genres
from pyspark.sql.functions import explode, size, udf
from pyspark.sql.types import ArrayType, StringType

subject_to_genres = udf(_subject_to_genres, ArrayType(StringType()))

joined = silver_books.join(silver_ol, on="book_id", how="inner")

with_genres = (
    joined
    .withColumn("genres", subject_to_genres(col("subjects")))
    .filter(size(col("genres")) > 0)
)

no_genre_df = (
    joined
    .withColumn("genres", subject_to_genres(col("subjects")))
    .filter(size(col("genres")) == 0)
    .select("username", "book_id", "subjects")
)

print(f"Books mapped to at least one genre: {with_genres.select('book_id').distinct().count()}")
print(f"Books excluded (no genre match):    {no_genre_df.count()}")
display(no_genre_df)

# COMMAND ----------

# DBTITLE 1,Explode genres and aggregate per user
from pyspark.sql.functions import (
    avg, count, round as spark_round, sum as spark_sum,
)

result = (
    with_genres
    .select(
        "username", "user_rating", "num_pages",
        explode(col("genres")).alias("genre"),
    )
    .groupBy("username", "genre")
    .agg(
        spark_round(avg("user_rating"), 2).alias("avg_user_rating"),
        spark_sum("num_pages").cast("int").alias("total_pages"),
        count("*").cast("int").alias("book_count"),
    )
    .orderBy("username", "genre")
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

print(f"Written {result.count()} rows to {GOLD_TABLE}")
