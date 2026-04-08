# Databricks notebook source
# MAGIC %md
# MAGIC # Goodreads — Unity Catalog Setup
# MAGIC Run this notebook **once** with an account that has `CREATE CATALOG` privilege (typically a workspace admin).
# MAGIC After this notebook succeeds, run `goodreads_rss_to_csv` to populate the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS main;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Create schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.goodreads
# MAGIC COMMENT 'Goodreads reading data';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Grant access (optional)
# MAGIC Replace `<your-user-or-group>` with your Databricks account email or a group name.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GRANT USAGE ON CATALOG main TO `<your-user-or-group>`;
# MAGIC -- GRANT USAGE ON SCHEMA main.goodreads TO `<your-user-or-group>`;
# MAGIC -- GRANT CREATE ON SCHEMA main.goodreads TO `<your-user-or-group>`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN main;
