import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    lit,
    col,
    sum as spark_sum,
    to_timestamp,
    to_date,
    hour,
    date_format,
    when
)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"


def save_single_csv(dataframe, file_name):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / file_name

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=dataframe.columns)
        writer.writeheader()
        for row in dataframe.toLocalIterator():
            writer.writerow(row.asDict())


# ============================================================
# 1. START SPARK
# ============================================================

spark = SparkSession.builder \
    .appName("Food Delivery Data Analytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\nSpark started successfully!")


# ============================================================
# 2. LOAD DATASET
# ============================================================

df = spark.read.csv(
    "data/food_delivery_analysis_dataset.csv",
    header=True,
    inferSchema=True
)

print("\nDataset loaded successfully!")

print("\n========== DATASET SCHEMA ==========")
df.printSchema()

print("\n========== FIRST 5 RECORDS ==========")
df.show(5, truncate=False)

print("\nTotal rows:", df.count())


# ============================================================
# 3. DATA CLEANING
# ============================================================

print("\n========== DATA CLEANING ==========")

# Remove duplicate orders
df = df.dropDuplicates(["order_id"])

print("Rows after removing duplicates:", df.count())

# Check missing values
print("\nMissing values:")

df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show()


# ============================================================
# 4. DATE/TIME PROCESSING
# ============================================================

print("\n========== DATE/TIME PROCESSING ==========")

df = df.withColumn(
    "order_timestamp",
    to_timestamp(
        col("order_placed_at"),
        "hh:mm a, MMMM dd yyyy"
    )
)

df = df.withColumn(
    "order_date",
    to_date("order_timestamp")
)

df = df.withColumn(
    "order_month",
    date_format("order_timestamp", "yyyy-MM")
)

df = df.withColumn(
    "order_day",
    date_format("order_timestamp", "EEEE")
)

df = df.withColumn(
    "order_hour",
    hour("order_timestamp")
)

print("Date/time columns created successfully!")

df.select(
    "order_placed_at",
    "order_date",
    "order_month",
    "order_day",
    "order_hour"
).show(5, truncate=False)


# Create SQL temporary view
df.createOrReplaceTempView("orders")


# ============================================================
# 5. ORDER ANALYSIS
# ============================================================

print("\n")
print("============================================================")
print("                    ORDER ANALYSIS")
print("============================================================")

# Total Orders
total_orders = df.count()

print("\nTotal Orders:", total_orders)


# Order Status Distribution
print("\n----- Order Status Distribution -----")

spark.sql("""
    SELECT
        order_status,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY order_status
    ORDER BY total_orders DESC
""").show()


# Orders by Date
print("\n----- Orders by Date -----")

df.groupBy("order_date") \
    .count() \
    .orderBy("order_date") \
    .show(30)


# Orders by Month
print("\n----- Orders by Month -----")

spark.sql("""
    SELECT
        order_month,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY order_month
    ORDER BY order_month
""").show()


# Orders by Day
print("\n----- Orders by Day of Week -----")

df.groupBy("order_day") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()


# Peak Ordering Hours
print("\n----- Peak Ordering Hours -----")

spark.sql("""
    SELECT
        order_hour,
        COUNT(*) AS total_orders
    FROM orders
    GROUP BY order_hour
    ORDER BY total_orders DESC
""").show()


# ============================================================
# 6. CUSTOMER ANALYSIS
# ============================================================

print("\n")
print("============================================================")
print("                  CUSTOMER ANALYSIS")
print("============================================================")


# Total Unique Customers
unique_customers = df.select(
    "customer_id"
).distinct().count()

print("\nTotal Unique Customers:", unique_customers)


# Orders per Customer
print("\n----- Orders per Customer -----")

customer_orders = df.groupBy(
    "customer_id"
).count().withColumnRenamed(
    "count",
    "order_count"
)

customer_orders.show(10, truncate=False)


# Top 10 Customers
print("\n----- Top 10 Customers -----")

spark.sql("""
    SELECT
        customer_id,
        COUNT(order_id) AS order_count
    FROM orders
    GROUP BY customer_id
    ORDER BY order_count DESC
    LIMIT 10
""").show(truncate=False)


# Repeat Customers
repeat_customers = customer_orders.filter(
    col("order_count") > 1
)

print(
    "\nNumber of Repeat Customers:",
    repeat_customers.count()
)


# One-Time Customers
one_time_customers = customer_orders.filter(
    col("order_count") == 1
)

print(
    "Number of One-Time Customers:",
    one_time_customers.count()
)


# ============================================================
# 7. RESTAURANT ANALYSIS
# ============================================================

print("\n")
print("============================================================")
print("                 RESTAURANT ANALYSIS")
print("============================================================")


# Total Restaurants
total_restaurants = df.select(
    "restaurant_id"
).distinct().count()

print("\nTotal Restaurants:", total_restaurants)


# Orders per Restaurant
print("\n----- Orders per Restaurant -----")

restaurant_orders = df.groupBy(
    "restaurant_id",
    "restaurant_name"
).count().withColumnRenamed(
    "count",
    "order_count"
)

restaurant_orders.show(10, truncate=False)


# Top 10 Restaurants
print("\n----- Top 10 Restaurants -----")

spark.sql("""
    SELECT
        restaurant_name,
        COUNT(order_id) AS order_count
    FROM orders
    GROUP BY restaurant_name
    ORDER BY order_count DESC
    LIMIT 10
""").show(truncate=False)


# Restaurant-wise Order Status
print("\n----- Restaurant-wise Order Status -----")

df.groupBy(
    "restaurant_name",
    "order_status"
).count() \
 .orderBy(
     "restaurant_name",
     "count",
     ascending=False
 ) \
 .show(30, truncate=False)


# ============================================================
# 8. SAVE DASHBOARD RESULTS
# ============================================================

print("\n")
print("============================================================")
print("                SAVING DASHBOARD RESULTS")
print("============================================================")

orders_by_date = df.groupBy(
    "order_date",
    "order_status"
).count().withColumnRenamed(
    "count",
    "total_orders"
)

orders_by_hour = df.groupBy(
    "order_hour",
    "order_status"
).count().withColumnRenamed(
    "count",
    "total_orders"
)

order_status_summary = df.groupBy(
    "order_status"
).count().withColumnRenamed(
    "count",
    "total_orders"
)

restaurant_summary = df.groupBy(
    "restaurant_id",
    "restaurant_name",
    "order_status"
).count().withColumnRenamed(
    "count",
    "total_orders"
)

status_customer_orders = df.groupBy(
    "order_status",
    "customer_id"
).count().withColumnRenamed(
    "count",
    "order_count"
)

status_customer_summary = status_customer_orders.groupBy(
    "order_status"
).agg(
    count("*").alias("total_unique_customers"),
    spark_sum(
        when(col("order_count") > 1, 1).otherwise(0)
    ).alias("repeat_customers"),
    spark_sum(
        when(col("order_count") == 1, 1).otherwise(0)
    ).alias("one_time_customers")
)

overall_customer_summary = customer_orders.agg(
    count("*").alias("total_unique_customers"),
    spark_sum(
        when(col("order_count") > 1, 1).otherwise(0)
    ).alias("repeat_customers"),
    spark_sum(
        when(col("order_count") == 1, 1).otherwise(0)
    ).alias("one_time_customers")
).withColumn(
    "order_status",
    lit("__ALL_ORDERS__")
).select(
    "order_status",
    "total_unique_customers",
    "repeat_customers",
    "one_time_customers"
)

customer_summary = overall_customer_summary.unionByName(
    status_customer_summary
)

save_single_csv(orders_by_date, "orders_by_date.csv")
save_single_csv(orders_by_hour, "orders_by_hour.csv")
save_single_csv(order_status_summary, "order_status.csv")
save_single_csv(restaurant_summary, "top_restaurants.csv")
save_single_csv(customer_summary, "customer_summary.csv")

print(f"Dashboard CSV files saved in: {OUTPUT_DIR}")


# ============================================================
# 9. FINISH
# ============================================================

print("\n")
print("============================================================")
print("          FOOD DELIVERY ANALYSIS COMPLETED")
print("============================================================")

spark.stop()

print("\nSpark stopped.")