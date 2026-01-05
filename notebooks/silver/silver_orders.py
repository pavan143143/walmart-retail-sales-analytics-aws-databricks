# Databricks notebook source
dbutils.fs.rm(
    "/Volumes/retail_analytics/raw/walmart_data/silver/silver_orders",
    recurse=True
)

# COMMAND ----------

# writes Delta with customer_name & product_name
silver_orders.write.format("delta").mode("overwrite").save(SILVER_PATH)


# COMMAND ----------

from pyspark.sql.functions import *

# Paths
BRONZE_PATH = "/Volumes/retail_analytics/raw/walmart_data/bronze/walmart_sales"
SILVER_PATH = "/Volumes/retail_analytics/raw/walmart_data/silver/silver_orders"

# Read Bronze
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# Transform to Silver Orders
silver_orders = (
    bronze_df
    .select(
        "order_id",

        # Safe date parsing
        expr("try_to_date(order_date, 'M/d/yyyy')").alias("order_date"),
        expr("try_to_date(ship_date, 'M/d/yyyy')").alias("ship_date"),

        # Foreign keys (required for Gold)
        "customer_name",
        "product_name",

        # Order attributes
        "order_priority",
        "ship_mode",

        # Safe numeric casting
        expr("try_cast(order_quantity as int)").alias("order_quantity"),
        expr("try_cast(sales as decimal(12,2))").alias("sales"),
        expr("try_cast(profit as decimal(12,2))").alias("profit"),
        expr("try_cast(discount as decimal(5,2))").alias("discount"),
        expr("try_cast(shipping_cost as decimal(12,2))").alias("shipping_cost"),
        expr("try_cast(number_of_records as int)").alias("number_of_records"),

        # Metadata
        "ingestion_timestamp"
    )
    .dropDuplicates(["order_id"])
)

# Write Delta
(
    silver_orders.write
                 .format("delta")
                 .mode("overwrite")
                 .save(SILVER_PATH)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_analytics;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_orders
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/silver/silver_orders`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_orders
