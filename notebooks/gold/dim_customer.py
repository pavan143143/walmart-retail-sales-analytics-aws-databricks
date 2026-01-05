# Databricks notebook source
from pyspark.sql.functions import *

DIM_PATH = "/Volumes/retail_analytics/raw/walmart_data/gold/dim_customer"

dim_customer = (
    spark.table("retail_analytics.raw.silver_customers")
         .select(
             "customer_name",
             "customer_age",
             "customer_segment",
             "city",
             "state",
             "region",
             "zip_code"
         )
)

(
    dim_customer.write
                .format("delta")
                .mode("overwrite")
                .save(DIM_PATH)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customer
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/gold/dim_customer`;
# MAGIC
