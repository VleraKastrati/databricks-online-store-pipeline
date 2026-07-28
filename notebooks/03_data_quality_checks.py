# Databricks notebook source
# Project: Online Store Data Pipeline
# Notebook: 03_data_quality_checks
# Purpose: Apply data quality checks and separate valid and invalid records

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

silver_customers_df = spark.table("workspace.default.silver_customers")
display(silver_customers_df)

# COMMAND ----------

invalid_customers_df = (
    silver_customers_df
    .filter(
        col("customer_id").isNull() |
        col("email").isNull()
    )
    .withColumn("error_reason", lit("Missing customer_id or email"))
)
display(invalid_customers_df)

# COMMAND ----------

valid_customers_df = (
    silver_customers_df
    .filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull()
    )
)

display(valid_customers_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.invalid_customers")

invalid_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.invalid_customers")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.valid_customers")

valid_customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.valid_customers")

# COMMAND ----------

display(spark.table("workspace.default.valid_customers"))
display(spark.table("workspace.default.invalid_customers"))

# COMMAND ----------

# Read silver products table
silver_products_df = spark.table("workspace.default.silver_products")

display(silver_products_df)

# COMMAND ----------

# Create invalid products table
invalid_products_df = (
    silver_products_df
    .filter(
        col("product_id").isNull() |
        col("product_name").isNull() |
        (col("price") <= 0)
    )
    .withColumn("error_reason", lit("Missing product_id/product_name or invalid price"))
)

display(invalid_products_df)

# COMMAND ----------

# Create valid products table
valid_products_df = (
    silver_products_df
    .filter(
        col("product_id").isNotNull() &
        col("product_name").isNotNull() &
        (col("price") > 0)
    )
)

display(valid_products_df)

# COMMAND ----------

# Create valid products table
valid_products_df = (
    silver_products_df
    .filter(
        col("product_id").isNotNull() &
        col("product_name").isNotNull() &
        (col("price") > 0)
    )
)

display(valid_products_df)

# COMMAND ----------

# Save invalid products as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.invalid_products")

invalid_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.invalid_products")

# COMMAND ----------

# Save valid products as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.valid_products")

valid_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.valid_products")

# COMMAND ----------

# Check valid and invalid products
display(spark.table("workspace.default.valid_products"))
display(spark.table("workspace.default.invalid_products"))

# COMMAND ----------

# Read silver orders and valid customers
silver_orders_df = spark.table("workspace.default.silver_orders")
valid_customers_df = spark.table("workspace.default.valid_customers")

display(silver_orders_df)
display(valid_customers_df)

# COMMAND ----------

# Join orders with valid customers to check if customer exists
orders_with_customers_df = (
    silver_orders_df.alias("o")
    .join(
        valid_customers_df.select("customer_id").alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .select(
        col("o.*"),
        col("c.customer_id").alias("matched_customer_id")
    )
)

display(orders_with_customers_df)

# COMMAND ----------

# Create invalid orders table
invalid_orders_df = (
    orders_with_customers_df
    .filter(
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("order_date").isNull() |
        (~col("status").isin("completed", "pending", "cancelled")) |
        col("matched_customer_id").isNull()
    )
    .withColumn("error_reason", lit("Missing required fields, invalid status, or customer does not exist"))
    .drop("matched_customer_id")
)

display(invalid_orders_df)

# COMMAND ----------

# Create valid orders table
valid_orders_df = (
    orders_with_customers_df
    .filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("order_date").isNotNull() &
        col("status").isin("completed", "pending", "cancelled") &
        col("matched_customer_id").isNotNull()
    )
    .drop("matched_customer_id")
)

display(valid_orders_df)

# COMMAND ----------

# Save invalid orders as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.invalid_orders")

invalid_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.invalid_orders")

# COMMAND ----------

# Save valid orders as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.valid_orders")

valid_orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.valid_orders")

# COMMAND ----------

# Check valid and invalid orders
display(spark.table("workspace.default.valid_orders"))
display(spark.table("workspace.default.invalid_orders"))

# COMMAND ----------

# Read silver order_items, valid orders, and valid products
silver_order_items_df = spark.table("workspace.default.silver_order_items")
valid_orders_df = spark.table("workspace.default.valid_orders")
valid_products_df = spark.table("workspace.default.valid_products")

display(silver_order_items_df)
display(valid_orders_df)
display(valid_products_df)

# COMMAND ----------

# Join order_items with valid orders and valid products
order_items_checked_df = (
    silver_order_items_df.alias("oi")
    .join(
        valid_orders_df.select("order_id").alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "left"
    )
    .join(
        valid_products_df.select("product_id").alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "left"
    )
    .select(
        col("oi.*"),
        col("o.order_id").alias("matched_order_id"),
        col("p.product_id").alias("matched_product_id")
    )
)

display(order_items_checked_df)

# COMMAND ----------

# Create invalid order_items table
invalid_order_items_df = (
    order_items_checked_df
    .filter(
        col("order_item_id").isNull() |
        col("order_id").isNull() |
        col("product_id").isNull() |
        col("quantity").isNull() |
        col("unit_price").isNull() |
        (col("quantity") <= 0) |
        (col("unit_price") <= 0) |
        col("matched_order_id").isNull() |
        col("matched_product_id").isNull()
    )
    .withColumn(
        "error_reason",
        lit("Missing required fields, invalid quantity/price, order does not exist, or product does not exist")
    )
    .drop("matched_order_id", "matched_product_id")
)

display(invalid_order_items_df)

# COMMAND ----------

# Create valid order_items table
valid_order_items_df = (
    order_items_checked_df
    .filter(
        col("order_item_id").isNotNull() &
        col("order_id").isNotNull() &
        col("product_id").isNotNull() &
        col("quantity").isNotNull() &
        col("unit_price").isNotNull() &
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        col("matched_order_id").isNotNull() &
        col("matched_product_id").isNotNull()
    )
    .drop("matched_order_id", "matched_product_id")
)

display(valid_order_items_df)

# COMMAND ----------

# Save invalid order_items as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.invalid_order_items")

invalid_order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.invalid_order_items")

# COMMAND ----------

# Save valid order_items as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.valid_order_items")

valid_order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.valid_order_items")

# COMMAND ----------

# Check valid and invalid order_items
display(spark.table("workspace.default.valid_order_items"))
display(spark.table("workspace.default.invalid_order_items"))

# COMMAND ----------

# Read silver payments and valid orders
silver_payments_df = spark.table("workspace.default.silver_payments")
valid_orders_df = spark.table("workspace.default.valid_orders")

display(silver_payments_df)
display(valid_orders_df)

# COMMAND ----------

# Join payments with valid orders to check if order exists
payments_checked_df = (
    silver_payments_df.alias("p")
    .join(
        valid_orders_df.select("order_id").alias("o"),
        col("p.order_id") == col("o.order_id"),
        "left"
    )
    .select(
        col("p.*"),
        col("o.order_id").alias("matched_order_id")
    )
)

display(payments_checked_df)

# COMMAND ----------

# Create invalid payments table
invalid_payments_df = (
    payments_checked_df
    .filter(
        col("payment_id").isNull() |
        col("order_id").isNull() |
        col("payment_method").isNull() |
        col("payment_status").isNull() |
        col("amount").isNull() |
        col("payment_date").isNull() |
        (~col("payment_status").isin("paid", "pending", "failed")) |
        (col("amount") <= 0) |
        col("matched_order_id").isNull()
    )
    .withColumn(
        "error_reason",
        lit("Missing required fields, invalid payment status, invalid amount, or order does not exist")
    )
    .drop("matched_order_id")
)

display(invalid_payments_df)

# COMMAND ----------

# Create valid payments table
valid_payments_df = (
    payments_checked_df
    .filter(
        col("payment_id").isNotNull() &
        col("order_id").isNotNull() &
        col("payment_method").isNotNull() &
        col("payment_status").isNotNull() &
        col("amount").isNotNull() &
        col("payment_date").isNotNull() &
        col("payment_status").isin("paid", "pending", "failed") &
        (col("amount") > 0) &
        col("matched_order_id").isNotNull()
    )
    .drop("matched_order_id")
)

display(valid_payments_df)

# COMMAND ----------

# Save invalid payments as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.invalid_payments")

invalid_payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.invalid_payments")

# COMMAND ----------

# Save valid payments as Delta table
spark.sql("DROP TABLE IF EXISTS workspace.default.valid_payments")

valid_payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.valid_payments")

# COMMAND ----------

# Check valid and invalid payments
display(spark.table("workspace.default.valid_payments"))
display(spark.table("workspace.default.invalid_payments"))

# COMMAND ----------

# Show all valid and invalid tables
spark.sql("SHOW TABLES IN workspace.default LIKE 'valid*'").show()
spark.sql("SHOW TABLES IN workspace.default LIKE 'invalid*'").show()

# COMMAND ----------

# Count records in each valid and invalid table
tables = [
    "valid_customers",
    "invalid_customers",
    "valid_products",
    "invalid_products",
    "valid_orders",
    "invalid_orders",
    "valid_order_items",
    "invalid_order_items",
    "valid_payments",
    "invalid_payments"
]

for table in tables:
    count = spark.table(f"workspace.default.{table}").count()
    print(f"{table}: {count} rows")

# COMMAND ----------

# Display all invalid records
display(spark.table("workspace.default.invalid_customers"))
display(spark.table("workspace.default.invalid_products"))
display(spark.table("workspace.default.invalid_orders"))
display(spark.table("workspace.default.invalid_order_items"))
display(spark.table("workspace.default.invalid_payments"))

# COMMAND ----------

# Display all valid records
display(spark.table("workspace.default.valid_customers"))
display(spark.table("workspace.default.valid_products"))
display(spark.table("workspace.default.valid_orders"))
display(spark.table("workspace.default.valid_order_items"))
display(spark.table("workspace.default.valid_payments"))

# COMMAND ----------

