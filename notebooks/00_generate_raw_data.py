# Databricks notebook source
# Project: Online Store Data Pipeline
# Notebook: 00_generate_raw_data
# Purpose: Create raw CSV files for customers, products, orders, order_items, and payments

# COMMAND ----------

# 1. Create a Databricks Volume to store raw files
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.online_store_raw")

# COMMAND ----------

raw_path = "/Volumes/workspace/default/online_store_raw"

# COMMAND ----------

customers_data = [
    (1, "Lira", "Kastrati", "lira.kastrati@gmail.com", "Kosovo", "2026-01-10"),
    (2, "Arta", "Limani", "arta@gmail.com", "Kosovo", "2026-01-12"),
    (3, "Dior", "Azemi", "dior@gmail.com", "Albania", "2026-01-15"),
    (4, "Elira", "Hoxha", None, "Kosovo", "2026-01-20"),
    (2, "Arta", "Kastrati", "arta@gmail.com", "Kosovo", "2026-01-12"),
]

customers_columns = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "created_at"
] 
customers_df = spark.createDataFrame(customers_data, customers_columns)
display(customers_df)

# COMMAND ----------

customers_df.write.mode("overwrite").option("header", True).csv(f"{raw_path}/customers")

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_path}/customers"))

# COMMAND ----------

test_customers_df = spark.read.option("header", True).csv(f"{raw_path}/customers")

display(test_customers_df)

# COMMAND ----------

product_data = [
    (101, "Laptop", "Electronics", 850.00, "True"),
    (102, "Mouse", "Electronics", 25.00, "True"),
    (103, "Keyboard", "Electronics", 45.00, "True"),
    (104, "Desk Chair", "Furniture", 120.00, True),
    (105, "Monitor", "Electronics", -200.00, True)
]

products_columns = [
    "product_id",
    "product_name",
    "category",
    "price",
    "is_active"
]
products_df = spark.createDataFrame(product_data, products_columns)
display(products_df)

# COMMAND ----------

# Write products data as raw CSV file
products_df.write.mode("overwrite").option("header", True).csv(f"{raw_path}/products")

# COMMAND ----------


display(dbutils.fs.ls(f"{raw_path}/products"))

# COMMAND ----------

# Read the saved products CSV file again for testing
test_products_df = spark.read.option("header", True).csv(f"{raw_path}/products")

display(test_products_df)

# COMMAND ----------

# Create raw orders data
orders_data = [
    (1001, 1, "2026-02-01", "completed"),
    (1002, 2, "2026-02-02", "completed"),
    (1003, 3, "2026-02-03", "pending"),
    (1004, 4, "2026-02-04", "cancelled"),
    (1005, 99, "2026-02-05", "completed")
]

orders_columns = [
    "order_id",
    "customer_id",
    "order_date",
    "status"
]

orders_df = spark.createDataFrame(orders_data, orders_columns)

display(orders_df)

# COMMAND ----------

# Write orders data as raw CSV file
orders_df.write.mode("overwrite").option("header", True).csv(f"{raw_path}/orders")

# COMMAND ----------

# Check if orders file was saved successfully
display(dbutils.fs.ls(f"{raw_path}/orders"))

# COMMAND ----------

# Read the saved orders CSV file again for testing
test_orders_df = spark.read.option("header", True).csv(f"{raw_path}/orders")

display(test_orders_df)

# COMMAND ----------

# Create raw order_items data
order_items_data = [
    (1, 1001, 101, 1, 850.00),
    (2, 1001, 102, 2, 25.00),
    (3, 1002, 103, 1, 45.00),
    (4, 1003, 104, 1, 120.00),
    (5, 1004, 105, 0, -200.00),
    (6, 9999, 101, 1, 850.00)
]

order_items_columns = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price"
]

order_items_df = spark.createDataFrame(order_items_data, order_items_columns)

display(order_items_df)

# COMMAND ----------

order_items_df.write.mode("overwrite").option("header", True).csv(f"{raw_path}/order_items")

# COMMAND ----------

# Check if order_items file was saved successfully
display(dbutils.fs.ls(f"{raw_path}/order_items"))

# COMMAND ----------

# Read the saved order_items CSV file again for testing
test_order_items_df = spark.read.option("header", True).csv(f"{raw_path}/order_items")

display(test_order_items_df)

# COMMAND ----------

# Create raw payments data
payments_data = [
    (5001, 1001, "card", "paid", 900.00, "2026-02-01"),
    (5002, 1002, "cash", "paid", 45.00, "2026-02-02"),
    (5003, 1003, "card", "pending", 120.00, "2026-02-03"),
    (5004, 1004, "card", "failed", -200.00, "2026-02-04"),
    (5005, 9999, "paypal", "paid", 850.00, "2026-02-05")
]

payments_columns = [
    "payment_id",
    "order_id",
    "payment_method",
    "payment_status",
    "amount",
    "payment_date"
]

payments_df = spark.createDataFrame(payments_data, payments_columns)

display(payments_df)

# COMMAND ----------

# Write payments data as raw CSV file
payments_df.write.mode("overwrite").option("header", True).csv(f"{raw_path}/payments")

# COMMAND ----------


display(dbutils.fs.ls(f"{raw_path}/payments"))

# COMMAND ----------

test_payments_df = spark.read.option("header", True).csv(f"{raw_path}/payments")

display(test_payments_df)

# COMMAND ----------

# Final check: list all raw folders
display(dbutils.fs.ls(raw_path))