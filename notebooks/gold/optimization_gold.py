# Databricks notebook source
# MAGIC %md
# MAGIC ## Why Do We Optimize the Gold Layer?
# MAGIC
# MAGIC ### Optimize dim_product
# MAGIC
# MAGIC This table is frequently joined with the fact table on `product_name`
# MAGIC and filtered by `product_category`.
# MAGIC Z-Ordering improves join and filter performance.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_analytics;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_product
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/retail_analytics/raw/walmart_data/gold/dim_product`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE retail_analytics.raw.dim_product;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE retail_analytics.raw.dim_product
# MAGIC ZORDER BY (product_name, product_category);
# MAGIC
