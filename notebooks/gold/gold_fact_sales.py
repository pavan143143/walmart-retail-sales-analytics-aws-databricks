# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_analytics.raw.silver_orders;

# COMMAND ----------

from pyspark.sql.functions import *

FACT_PATH = "/Volumes/retail_analytics/raw/walmart_data/gold/fact_sales"

# Read Silver tables (FULLY QUALIFIED — IMPORTANT)
orders = spark.table("retail_analytics.raw.silver_orders")
customers = spark.table("retail_analytics.raw.silver_customers")
products = spark.table("retail_analytics.raw.silver_products")

# Build Fact table
fact_sales = (
    orders
    .join(customers, "customer_name", "left")
    .join(products, "product_name", "left")
    .select(
        # Business keys
        "order_id",
        "order_date",
        "ship_date",

        # Dimension keys
        "customer_name",
        "product_name",

        # Dimension attributes (denormalized for analytics)
        "customer_segment",
        "city",
        "state",
        "region",
        "product_category",
        "product_sub_category",

        # Measures
        "order_quantity",
        "sales",
        "profit",
        "discount",
        "shipping_cost"
    )
)

# Write Delta
(
    fact_sales.write
              .format("delta")
              .mode("overwrite")
              .save(FACT_PATH)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_analytics;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fact_sales
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/gold/fact_sales`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_analytics.raw.fact_sales;
