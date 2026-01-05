# Databricks notebook source
from pyspark.sql.functions import *

orders = spark.table("retail_analytics.raw.silver_orders")

dates = (
    orders
    .select(col("order_date").alias("date"))
    .union(
        orders.select(col("ship_date").alias("date"))
    )
    .filter(col("date").isNotNull())
    .distinct()
)

dim_date = (
    dates
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("day_of_week", date_format("date", "E"))
)

(
    dim_date.write
            .format("delta")
            .mode("overwrite")
            .save("/Volumes/retail_analytics/raw/walmart_data/gold/dim_date")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_date
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`/Volumes/retail_analytics/raw/walmart_data/gold/dim_date`;
# MAGIC
