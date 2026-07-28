# Databricks notebook source
# Project: Online Store Data Pipeline
# Notebook: 02_silver_layer
# Purpose: Clean Bronze tables and save them as Silver Delta tables

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, to_date

# COMMAND ----------

bronze_customers_df = spark.table("workspace.default.bronze_customers")
display(bronze_customers_df)

# COMMAND ----------

silver_customers_df = (
    bronze_customers_df
    .select(
        col("customer_id").cast("int").alias("customer_id"),
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        lower(trim(col("email"))).alias("email"),
        trim(col("country")).alias("country"),
        to_date(col("created_at")).alias("created_at"),
        col("ingestion_timestamp"),
        col("source_file")
    )
    .dropDuplicates(["customer_id"])
)

display(silver_customers_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_customers")

# COMMAND ----------

silver_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_customers")

# COMMAND ----------

display(spark.table("workspace.default.silver_customers"))

# COMMAND ----------

# Read bronze products table
bronze_products_df = spark.table("workspace.default.bronze_products")

display(bronze_products_df)

# COMMAND ----------

# Clean products data for Silver layer
silver_products_df = (
    bronze_products_df
    .select(
        col("product_id").cast("int").alias("product_id"),
        trim(col("product_name")).alias("product_name"),
        lower(trim(col("category"))).alias("category"),
        col("price").cast("double").alias("price"),
        col("is_active").cast("boolean").alias("is_active"),
        col("ingestion_timestamp"),
        col("source_file")
    )
    .dropDuplicates(["product_id"])
)

display(silver_products_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_products")

# COMMAND ----------

silver_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_products")

# COMMAND ----------

display(spark.table("workspace.default.silver_products"))

# COMMAND ----------

# Read bronze orders table
bronze_orders_df = spark.table("workspace.default.bronze_orders")

display(bronze_orders_df)

# COMMAND ----------

# Clean orders data for Silver layer
silver_orders_df = (
    bronze_orders_df
    .select(
        col("order_id").cast("int").alias("order_id"),
        col("customer_id").cast("int").alias("customer_id"),
        to_date(col("order_date")).alias("order_date"),
        lower(trim(col("status"))).alias("status"),
        col("ingestion_timestamp"),
        col("source_file")
    )
    .dropDuplicates(["order_id"])
)
display(silver_orders_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_orders")

# COMMAND ----------

silver_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_orders")

# COMMAND ----------

display(spark.table("workspace.default.silver_orders"))

# COMMAND ----------

# Read bronze order_items table
bronze_order_items_df = spark.table("workspace.default.bronze_order_items")

display(bronze_order_items_df)

# COMMAND ----------

# Clean order_items data for Silver layer
silver_order_items_df = (
    bronze_order_items_df
    .select(
        col("order_item_id").cast("int").alias("order_item_id"),
        col("order_id").cast("int").alias("order_id"),
        col("product_id").cast("int").alias("product_id"),
        col("quantity").cast("int").alias("quantity"),
        col("unit_price").cast("double").alias("unit_price"),
        col("ingestion_timestamp"),
        col("source_file")
    )
    .dropDuplicates(["order_item_id"])
)

display(silver_order_items_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_order_items")

# COMMAND ----------

silver_order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_order_items")

# COMMAND ----------

display(spark.table("workspace.default.silver_order_items"))

# COMMAND ----------

# Read bronze payments table
bronze_payments_df = spark.table("workspace.default.bronze_payments")

display(bronze_payments_df)

# COMMAND ----------

# Clean payments data for Silver layer
silver_payments_df = (
    bronze_payments_df
    .select(
        col("payment_id").cast("int").alias("payment_id"),
        col("order_id").cast("int").alias("order_id"),
        lower(trim(col("payment_method"))).alias("payment_method"),
        lower(trim(col("payment_status"))).alias("payment_status"),
        col("amount").cast("double").alias("amount"),
        to_date(col("payment_date")).alias("payment_date"),
        col("ingestion_timestamp"),
        col("source_file")
    )
    .dropDuplicates(["payment_id"])
)

display(silver_payments_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_payments")

# COMMAND ----------

silver_payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.silver_payments")

# COMMAND ----------

display(spark.table("workspace.default.silver_payments"))

# COMMAND ----------

# Final check: Show only Silver tables
spark.sql("SHOW TABLES IN workspace.default LIKE 'silver*'").show()