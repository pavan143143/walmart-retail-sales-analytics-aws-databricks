# Databricks notebook source
from pyspark.sql.functions import *

# Paths
BRONZE_PATH = "/Volumes/retail_analytics/raw/walmart_data/bronze/walmart_sales"
SILVER_PATH = "/Volumes/retail_analytics/raw/walmart_data/silver/silver_products"

# Read Bronze
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# Transform to Silver Products
silver_products = (
    bronze_df
    .select(
        "product_name",
        "product_category",
        "product_sub_category",
        "product_container",
        expr("try_cast(product_base_margin as decimal(5,2))").alias("product_base_margin"),
        expr("try_cast(unit_price as decimal(12,2))").alias("unit_price"),
        "ingestion_timestamp"
    )
    .dropDuplicates(["product_name"])
)

# Write Delta
(
    silver_products.write
                   .format("delta")
                   .mode("overwrite")
                   .save(SILVER_PATH)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_analytics;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_products
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/silver/silver_products`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_products;
# MAGIC

# COMMAND ----------

orders = spark.table("retail_analytics.raw.silver_orders")
customers = spark.table("retail_analytics.raw.silver_customers")
products = spark.table("retail_analytics.raw.silver_products")
