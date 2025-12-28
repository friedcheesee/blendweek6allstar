from pyspark.sql.functions import lit, round
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month,
    sum, when,
    upper, trim
)
from pyspark.sql.types import DoubleType, IntegerType
import time

#1. spark session
spark = SparkSession.builder \
    .appName("Sales_ETL_Pipeline") \
    .master("local[*]") \
    .getOrCreate()


#2. read raw csv (postgres jdbc)

jdbc_url = "jdbc:postgresql://postgres:5432/salesdb"
table_name = "sales"
properties = {
    "user": "salesuser",
    "password": "salespass",
    "driver": "org.postgresql.Driver"
}

df_raw = spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    properties=properties
)

#3. select & rename columns

df = df_raw.select(
    col("Order ID").alias("order_id"),
    col("Date").alias("order_date"),
    col("Status").alias("status"),
    col("Qty").alias("quantity"),
    col("Amount").alias("amount"),
    col("ship-state").alias("state"),
    col("Category").alias("category"),
    col("Size").alias("size"),          
    col("B2B").alias("is_b2b"),
    col("promotion-ids").alias("promotion_ids"),
)


#4. data cleaning & transformation


df_all = (
    df
    .withColumn("order_date", to_date("order_date", "MM-dd-yy"))
    .withColumn("state", upper(trim(col("state"))))
    .withColumn("category", trim(col("category")))
    .withColumn("size", trim(col("size")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .fillna({
        "promotion_ids": "No Promotion",
        "is_b2b": "Unknown",
        "size": "Unknown"
    })
)
df_clean = (
    df_all
    .filter(col("amount") > 0)
    .filter(col("quantity") > 0)
    .dropna(subset=["order_id", "order_date", "amount"])
)


#5. global metrics

total_revenue = df_clean.agg(sum("amount")).first()[0]
total_orders = df_clean.select("order_id").distinct().count()


#6. kpi calculations


#monthly revenue
monthly_revenue = (
    df_clean
    .groupBy(year("order_date").alias("year"),
             month("order_date").alias("month"))
    .agg(sum("amount").alias("monthly_revenue"))
)


#region-wise sales
region_sales = (
    df_clean
    .groupBy("state")
    .agg(sum("amount").alias("state_revenue"))
)

#average order value (aov)
aov = total_revenue / total_orders

#cancellation rate
cancelled_orders = (
    df_all
    .filter(col("status") == "Cancelled")
    .select("order_id")
    .distinct()
    .count()
)

cancellation_rate = (cancelled_orders / total_orders) * 100

#promotion impact
promotion_impact = (
    df_clean
    .withColumn(
        "has_promotion",
        when(col("promotion_ids") != "No Promotion", "Promotion Used")
        .otherwise("No Promotion")
    )
    .groupBy("has_promotion")
    .agg(sum("amount").alias("revenue"))
)

#average basket size
total_quantity = df_clean.agg(sum("quantity")).first()[0]
avg_basket_size = total_quantity / total_orders


#category contribution
category_contribution = (
    df_clean
    .groupBy("category")
    .agg(sum("amount").alias("category_revenue"))
    .withColumn(
        "category_percentage",
        round((col("category_revenue") / lit(total_revenue)) * 100, 2)
    )
    .orderBy(col("category_revenue").desc())
)

#b2b vs b2c performance
b2b_performance = (
    df_clean
    .groupBy("is_b2b")
    .agg(sum("amount").alias("revenue"))
    .withColumn(
        "market_share_pct",
        round((col("revenue") / lit(total_revenue)) * 100, 2)
    )
)

#size-wise revenue
size_revenue = (
    df_clean
    .groupBy("size")
    .agg(sum("amount").alias("size_revenue"))
    .orderBy(col("size_revenue").desc())
)

#size-wise market share
size_market_share = (
    size_revenue
    .withColumn(
        "market_share_pct",
        round((col("size_revenue") / lit(total_revenue)) * 100, 2)
    )
)



#7. write outputs to parquet
output_path = "output"
monthly_revenue.write.mode("overwrite").parquet(f"{output_path}/monthly_revenue")
region_sales.write.mode("overwrite").parquet(f"{output_path}/region_sales")
promotion_impact.write.mode("overwrite").parquet(f"{output_path}/promotion_impact")
category_contribution.write.mode("overwrite").parquet(f"{output_path}/category_contribution")
b2b_performance.write.mode("overwrite").parquet(f"{output_path}/b2b_performance")
size_revenue.write.mode("overwrite").parquet(f"{output_path}/size_revenue")
size_market_share.write.mode("overwrite").parquet(f"{output_path}/size_market_share")


#8. verification

spark.read.parquet(f"{output_path}/monthly_revenue").show()
spark.read.parquet(f"{output_path}/region_sales").show()
spark.read.parquet(f"{output_path}/promotion_impact").show()
spark.read.parquet(f"{output_path}/category_contribution").show()
spark.read.parquet(f"{output_path}/b2b_performance").show()
spark.read.parquet(f"{output_path}/size_market_share").show()


print(f"AOV: {aov:.2f}")
print(f"Cancellation Rate: {cancellation_rate:.2f}%")
print(f"Average Basket Size: {avg_basket_size:.2f}")

print("Spark UI available at http://localhost:4040")
time.sleep(1200)
spark.stop()
