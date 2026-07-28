# Databricks notebook source
# Project: Online Store Data Pipeline
# Notebook: 01_bronze_ingestion
# Purpose: Read raw CSV files and save them as Bronze Delta tables

# COMMAND ----------

raw_path = "/Volumes/workspace/default/online_store_raw"

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

bronze_customers_df = (spark.read.option("header", True).csv(f"{raw_path}/customers"))

# COMMAND ----------

display(bronze_customers_df)

# COMMAND ----------

bronze_customers_df = (
    bronze_customers_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("customers"))
)

# COMMAND ----------

display(bronze_customers_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_customers")

# COMMAND ----------

bronze_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_customers")

# COMMAND ----------

display(spark.table("workspace.default.bronze_customers"))

# COMMAND ----------

bronze_products_df = (
    spark.read
    .option("header", True)
    .csv(f"{raw_path}/products")
)

display(bronze_products_df)

# COMMAND ----------

# Add metadata columns to products
bronze_products_df = (
    bronze_products_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("products"))
)

display(bronze_products_df)

# COMMAND ----------

# Drop old bronze products table if it exists
spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_products")

# COMMAND ----------

# Write products data as Bronze Delta table
bronze_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_products")

# COMMAND ----------

display(spark.table("workspace.default.bronze_products"))

# COMMAND ----------

# Read raw orders CSV
bronze_orders_df = (
    spark.read
    .option("header", True)
    .csv(f"{raw_path}/orders")
)

display(bronze_orders_df)

# COMMAND ----------

# Add metadata columns to orders
bronze_orders_df = (
    bronze_orders_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("orders"))
)

display(bronze_orders_df)

# COMMAND ----------


spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_orders")

# COMMAND ----------

# Write orders data as Bronze Delta table
bronze_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_orders")

# COMMAND ----------


display(spark.table("workspace.default.bronze_orders"))

# COMMAND ----------

# Read raw order_items CSV
bronze_order_items_df = (
    spark.read
    .option("header", True)
    .csv(f"{raw_path}/order_items")
)

display(bronze_order_items_df)

# COMMAND ----------

# Add metadata columns to order_items
bronze_order_items_df = (
    bronze_order_items_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("order_items"))
)

display(bronze_order_items_df)

# COMMAND ----------


spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_order_items")

# COMMAND ----------

# Write order_items data as Bronze Delta table
bronze_order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_order_items")

# COMMAND ----------

display(spark.table("workspace.default.bronze_order_items"))

# COMMAND ----------

# Read raw payments CSV
bronze_payments_df = (
    spark.read
    .option("header", True)
    .csv(f"{raw_path}/payments")
)

display(bronze_payments_df)

# COMMAND ----------

# Add metadata columns to payments
bronze_payments_df = (
    bronze_payments_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("payments"))
)

display(bronze_payments_df)

# COMMAND ----------


spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_payments")

# COMMAND ----------


bronze_payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_payments")

# COMMAND ----------

# Check bronze payments table
display(spark.table("workspace.default.bronze_payments"))

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show()