# Databricks notebook source
from pyspark.sql.functions import *

# Paths
BRONZE_PATH = "/Volumes/retail_analytics/raw/walmart_data/bronze/walmart_sales"
SILVER_PATH = "/Volumes/retail_analytics/raw/walmart_data/silver/silver_customers"

# Read Bronze
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# Transform to Silver Customers
silver_customers = (
    bronze_df
    .select(
        "customer_name",
        expr("try_cast(customer_age as int)").alias("customer_age"),
        "customer_segment",
        "city",
        "state",
        "region",
        "zip_code",
        "ingestion_timestamp"
    )
    .dropDuplicates(["customer_name"])
)

# Write Delta
(
    silver_customers.write
                    .format("delta")
                    .mode("overwrite")
                    .save(SILVER_PATH)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_analytics;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_customers
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/silver/silver_customers`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_customers;
