# Databricks notebook source
# Project: Online Store Data Pipeline
# Notebook: 04_gold_layer
# Purpose: Create Gold business tables for reporting and analytics

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, round

# COMMAND ----------

# Read valid tables
valid_customers_df = spark.table("workspace.default.valid_customers")
valid_orders_df = spark.table("workspace.default.valid_orders")
valid_order_items_df = spark.table("workspace.default.valid_order_items")
valid_products_df = spark.table("workspace.default.valid_products")
valid_payments_df = spark.table("workspace.default.valid_payments")

# COMMAND ----------

# Create Gold Sales Summary table
gold_sales_summary_df = (
    valid_orders_df.alias("o")
    .join(
        valid_customers_df.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "inner"
    )
    .join(
        valid_order_items_df.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    )
    .join(
        valid_products_df.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "inner"
    )
    .join(
        valid_payments_df.alias("pay"),
        col("o.order_id") == col("pay.order_id"),
        "left"
    )
    .select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.status").alias("order_status"),
        col("c.customer_id"),
        concat_ws(" ", col("c.first_name"), col("c.last_name")).alias("customer_name"),
        col("c.country"),
        col("p.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("oi.quantity"),
        col("oi.unit_price"),
        round(col("oi.quantity") * col("oi.unit_price"), 2).alias("total_amount"),
        col("pay.payment_method"),
        col("pay.payment_status"),
        col("pay.payment_date")
    )
)

display(gold_sales_summary_df)

# COMMAND ----------

# Save Gold Sales Summary table
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_sales_summary")

gold_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_sales_summary")

# COMMAND ----------

# Check Gold Sales Summary table
display(spark.table("workspace.default.gold_sales_summary"))

# COMMAND ----------

# Save Gold Sales Summary table
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_sales_summary")

gold_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_sales_summary")

# COMMAND ----------

# Check Gold Sales Summary table
display(spark.table("workspace.default.gold_sales_summary"))

# COMMAND ----------

from pyspark.sql.functions import date_format, countDistinct, sum, when

# COMMAND ----------

# Create Gold Monthly Revenue table
gold_monthly_revenue_df = (
    spark.table("workspace.default.gold_sales_summary")
    .groupBy(
        date_format(col("order_date"), "yyyy-MM").alias("month")
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(
            sum(
                when(col("payment_status") == "paid", col("total_amount")).otherwise(0)
            ),
            2
        ).alias("paid_revenue")
    )
    .orderBy("month")
)

display(gold_monthly_revenue_df)

# COMMAND ----------

# Save Gold Monthly Revenue table
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_monthly_revenue")

gold_monthly_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_monthly_revenue")

# COMMAND ----------

# Check Gold Monthly Revenue table
display(spark.table("workspace.default.gold_monthly_revenue"))

# COMMAND ----------

# Create Gold Top Products table
gold_top_products_df = (
    spark.table("workspace.default.gold_sales_summary")
    .groupBy(
        col("product_id"),
        col("product_name"),
        col("category")
    )
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        round(sum("total_amount"), 2).alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

display(gold_top_products_df)

# COMMAND ----------

# Save Gold Top Products table
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_top_products")

gold_top_products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_top_products")

# COMMAND ----------

# Check Gold Top Products table
display(spark.table("workspace.default.gold_top_products"))

# COMMAND ----------

# Create Gold Customer Revenue table
gold_customer_revenue_df = (
    spark.table("workspace.default.gold_sales_summary")
    .groupBy(
        col("customer_id"),
        col("customer_name"),
        col("country")
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(
            sum(
                when(col("payment_status") == "paid", col("total_amount")).otherwise(0)
            ),
            2
        ).alias("paid_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

display(gold_customer_revenue_df)

# COMMAND ----------

# Save Gold Customer Revenue table
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_customer_revenue")

gold_customer_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_customer_revenue")

# COMMAND ----------

# Check Gold Customer Revenue table
display(spark.table("workspace.default.gold_customer_revenue"))

# COMMAND ----------

# Create Gold Category Revenue table
gold_category_revenue_df = (
    spark.table("workspace.default.gold_sales_summary")
    .groupBy(
        col("category")
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity_sold"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(
            sum(
                when(col("payment_status") == "paid", col("total_amount")).otherwise(0)
            ),
            2
        ).alias("paid_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

display(gold_category_revenue_df)

# COMMAND ----------

# Save Gold Category Revenue table
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_category_revenue")

gold_category_revenue_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_category_revenue")

# COMMAND ----------

# Check Gold Category Revenue table
display(spark.table("workspace.default.gold_category_revenue"))

# COMMAND ----------

# Show all Gold tables
spark.sql("SHOW TABLES IN workspace.default LIKE 'gold*'").show()

# COMMAND ----------

# Count records in each Gold table
gold_tables = [
    "gold_sales_summary",
    "gold_monthly_revenue",
    "gold_top_products",
    "gold_customer_revenue",
    "gold_category_revenue"
]

for table in gold_tables:
    count = spark.table(f"workspace.default.{table}").count()
    print(f"{table}: {count} rows")

# COMMAND ----------

# Display all Gold tables
display(spark.table("workspace.default.gold_sales_summary"))
display(spark.table("workspace.default.gold_monthly_revenue"))
display(spark.table("workspace.default.gold_top_products"))
display(spark.table("workspace.default.gold_customer_revenue"))
display(spark.table("workspace.default.gold_category_revenue"))