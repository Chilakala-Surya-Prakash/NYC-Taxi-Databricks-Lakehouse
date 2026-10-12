# Databricks notebook source
bronze_df = spark.read.table("workspace.default.bronze_yellow_taxi")

print(f"Bronze table count: {bronze_df.count()}")
display(bronze_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col, round, timestamp_diff, lit

# Calculate trip duration directly in minutes
duration_in_minutes = timestamp_diff(lit("MINUTE"), col("tpep_pickup_datetime"), col("tpep_dropoff_datetime"))

silver_df = bronze_df.withColumn("trip_duration_minutes", round(duration_in_minutes, 2))

display(silver_df.select(
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime", 
    "trip_duration_minutes", 
    "trip_distance"
).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DATA QUALITY AND BOUNDARY FILTERS**

# COMMAND ----------

#FILTER OUT DIRTY & IMPOSSIBLE RECORDS
silver_filtered_df = silver_df.filter(
    (col("trip_distance")>0) & (col("trip_distance")<200) & (col("passenger_count")>0)
    & (col("fare_amount")>2.50) & (col("fare_amount")<1000) & (col("trip_duration_minutes")>= 1) & (col("trip_duration_minutes")<= 360)
)
print(f"Rows before filter:{silver_df.count()}")
print(f"Rows after filter:{silver_filtered_df.count()}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### **DEDUPLICATION**

# COMMAND ----------

# 1. Deduplicate records based on trip identity columns
silver_deduped_df = silver_filtered_df.dropDuplicates([
    "VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"
])

# 2. Read Bronze taxi zones reference table
zones_df = spark.read.table("workspace.default.bronze_taxi_zones")

# 3. Enrich pickup and dropoff zone details
silver_enriched_df = silver_deduped_df \
    .join(zones_df.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone")
    ), on="PULocationID", how="left") \
    .join(zones_df.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone")
    ), on="DOLocationID", how="left")

display(silver_enriched_df.select(
    "tpep_pickup_datetime", "pickup_borough", "pickup_zone",
    "dropoff_borough", "dropoff_zone", "trip_distance", "fare_amount"
).limit(20))

# COMMAND ----------

# -------------------------------------------------------------------------
# Feature Engineering & Business Normalization:
# 1. Temporal attributes: Extract date, hour, and day name for time-series analytics.
# 2. Payment categorization: Decode numeric payment IDs into business descriptions.
# 3. Tip percentage: Calculate tip rate relative to base fare for card transactions.
# -------------------------------------------------------------------------

from pyspark.sql.functions import col, to_date, hour, date_format, when, round

silver_transformed_df = silver_enriched_df \
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_day_name", date_format(col("tpep_pickup_datetime"), "EEEE")) \
    .withColumn("payment_type_desc", 
        when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .otherwise("Other")
    ) \
    .withColumn("tip_percentage", 
        when((col("payment_type") == 1) & (col("fare_amount") > 0), 
             round((col("tip_amount") / col("fare_amount")) * 100, 2))
        .otherwise(0.0)
    )

display(silver_transformed_df.select(
    "pickup_date", "pickup_hour", "pickup_day_name", 
    "payment_type_desc", "fare_amount", "tip_amount", "tip_percentage"
).limit(5))



# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 1. Add audit metadata timestamp
silver_final_df = silver_transformed_df.withColumn(
    "silver_processed_timestamp", 
    current_timestamp()
)

# 2. Write to Unity Catalog as managed Delta table
silver_table_name = "workspace.default.silver_yellow_taxi"

silver_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table_name)

print(f"Table {silver_table_name} saved successfully!")

# COMMAND ----------

# Verify the Silver Delta table from Unity Catalog
silver_table = spark.read.table("workspace.default.silver_yellow_taxi")

print(f"Final Silver Row Count: {silver_table.count()}")
display(silver_table.limit(5))

# COMMAND ----------

