# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Hive Metastore Setup
# MAGIC Run this notebook **once** to create the schema before populating data.
# MAGIC After this succeeds, run notebooks in order: `goodreads_bronze` → `goodreads_silver`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS goodreads
# MAGIC COMMENT 'Goodreads reading data';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Store Goodreads session cookie as a Databricks secret
# MAGIC
# MAGIC Required before running `goodreads_bronze_pages`. Run the following in a terminal with the Databricks CLI installed:
# MAGIC
# MAGIC ```
# MAGIC databricks secrets create-scope goodreads
# MAGIC databricks secrets put-secret goodreads session_id --string-value "<paste _session_id cookie value here>"
# MAGIC ```
# MAGIC
# MAGIC To find the cookie: log into Goodreads in your browser → DevTools (F12) → **Application** → **Cookies** → copy `_session_id`.
# MAGIC
# MAGIC When the cookie expires, re-run only the `put-secret` command with the fresh value.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN goodreads;
