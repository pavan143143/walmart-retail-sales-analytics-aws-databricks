# Databricks notebook source
import os

print(os.getcwd())

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_analytics;
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/Volumes/retail_analytics/raw/walmart_data")

# COMMAND ----------

dbutils.fs.ls("/Volumes/retail_analytics/raw/walmart_data")

# COMMAND ----------

from pyspark.sql.functions import *

RAW_PATH = "/Volumes/retail_analytics/raw/walmart_data/walmart_Retail_Data.csv"
BRONZE_PATH = "/Volumes/retail_analytics/raw/walmart_data/bronze/walmart_sales"

raw_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(RAW_PATH)
)

rename_columns = {
    "Customer Age": "customer_age",
    "Customer Name": "customer_name",
    "Customer Segment": "customer_segment",
    "Number of Records": "number_of_records",
    "Order Date": "order_date",
    "Order ID": "order_id",
    "Order Priority": "order_priority",
    "Order Quantity": "order_quantity",
    "Product Base Margin": "product_base_margin",
    "Product Category": "product_category",
    "Product Container": "product_container",
    "Product Name": "product_name",
    "Product Sub-Category": "product_sub_category",
    "Row ID": "row_id",
    "Ship Date": "ship_date",
    "Ship Mode": "ship_mode",
    "Shipping Cost": "shipping_cost",
    "Unit Price": "unit_price",
    "Zip Code": "zip_code"
}

for old_col, new_col in rename_columns.items():
    raw_df = raw_df.withColumnRenamed(old_col, new_col)

bronze_df = (
    raw_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn(
        "row_hash",
        sha2(
            concat_ws("||", *[col(c).cast("string") for c in raw_df.columns]),
            256
        )
    )
)

(
    bronze_df.write
             .format("delta")
             .mode("overwrite")
             .save(BRONZE_PATH)
)


# COMMAND ----------

spark.read.format("delta").load(
    "/Volumes/retail_analytics/raw/walmart_data/bronze/walmart_sales"
).printSchema()
