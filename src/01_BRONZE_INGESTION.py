# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# 1. Define explicit schema matching the file column order
Zone_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

# 2. Path to raw file
file_path = "/Volumes/workspace/default/landing_zone/taxi_zone_lookup.csv"

# 3. Read the CSV file
raw_df = spark.read.csv(file_path, header=True, schema=Zone_schema)

# 4. Add Bronze metadata columns
bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("file_name", col("_metadata.file_path"))

display(bronze_df)

# COMMAND ----------

# save as a managed Delta table in the workspace catalog and default schema
bronze_zones_table = "workspace.default.bronze_taxi_zones"

bronze_df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable(bronze_zones_table)

print(f"Table {bronze_zones_table} created successfully")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

#path to raw yellow_trips file 
raw_yellow_file_path = "/Volumes/workspace/default/landing_zone/yellow_tripdata_2026-01.parquet"

#Read
raw_trips_df = spark.read.parquet(raw_yellow_file_path)

#Add Bronze audit metadata columns
bronze_trips_df = raw_trips_df.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("file_name", col("_metadata.file_path"))

#create bronze yellow delta table
bronze_trips_table = "workspace.default.bronze_yellow_taxi"

bronze_trips_df.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable(bronze_trips_table)

print(f"Table {bronze_trips_table} created successfully")


# COMMAND ----------

