# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Hive Metastore Setup
# MAGIC Run this notebook **once** to create the schema before populating data.
# MAGIC After this notebook succeeds, run `goodreads_rss_to_csv` to populate the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS goodreads
# MAGIC COMMENT 'Goodreads reading data';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN goodreads;
