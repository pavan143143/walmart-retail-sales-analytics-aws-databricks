# Databricks notebook source
from pyspark.sql.functions import *

DIM_PATH = "/Volumes/retail_analytics/raw/walmart_data/gold/dim_product"

dim_product = (
    spark.table("retail_analytics.raw.silver_products")
         .select(
             "product_name",
             "product_category",
             "product_sub_category",
             "product_container",
             "product_base_margin",
             "unit_price"
         )
)

(
    dim_product.write
               .format("delta")
               .mode("overwrite")
               .save(DIM_PATH)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_product
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/gold/dim_product`;
# MAGIC
