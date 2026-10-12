# Databricks notebook source
#Load cleansed silver table from unity catalog
silver_df = spark.read.table("workspace.default.silver_yellow_taxi")

print(f"Silver Base Records Available: {silver_df.count():,}")
display(silver_df.select("pickup_date", "pickup_hour", "pickup_borough", "fare_amount", "trip_distance").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Business Questions Answered by Gold Tables**
# MAGIC
# MAGIC #### **Table 1: Hourly Demand & Revenue Patterns**
# MAGIC * **Q1:** Which hours of the day generate the highest trip demand and volume across New York City?
# MAGIC * **Q2:** How does total revenue correlate with hour of day, and when does fare yield peak?
# MAGIC * **Q3:** Do passenger trip lengths (distance in miles and duration in minutes) fluctuate between daytime rush hours and late-night hours?
# MAGIC
# MAGIC #### **Table 2: Borough-to-Borough Route Dynamics**
# MAGIC * **Q4:** Which pickup-to-dropoff borough routes handle the heaviest taxi traffic?
# MAGIC * **Q5:** Which specific corridors generate the highest average fares and longest trip distances?
# MAGIC * **Q6:** What proportion of taxi trips stay within the same borough versus crossing into other boroughs?
# MAGIC
# MAGIC #### **Table 3: Payment Economics & Tipping Behavior**
# MAGIC * **Q7:** What percentage of riders pay with credit cards versus cash?
# MAGIC * **Q8:** How does average tip percentage vary by pickup borough for card transactions?

# COMMAND ----------

# MAGIC %md
# MAGIC ## **GOLD TABLE **1****

# COMMAND ----------

from pyspark.sql.functions import count,sum,round,avg,col

# COMMAND ----------

Hourly_demand_df = silver_df.groupby("pickup_hour").agg(
    count("*").alias("total_trips"),
    round(sum("total_amount"),2).alias("total_revenue"),
    round(avg("fare_amount"),2).alias("avg_fare"),
    round(avg("trip_distance"),2).alias("avg_distance_miles"),
    round(avg("trip_duration_minutes"),2).alias("avg_duration_minutes")
).orderBy("pickup_hour")
display(Hourly_demand_df)

# COMMAND ----------

Hourly_demand_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_hourly_demand")

print("Saved: workspace.default.gold_hourly_demand")

# COMMAND ----------

# Q1: Peak hours by trip count
display(
    Hourly_demand_df.select("pickup_hour", "total_trips")
    .orderBy(col("total_trips").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1: Which hours generate the highest demand?**
# MAGIC   * **Peak Demand:** **5:00 PM – 6:59 PM (Hours 17 & 18)** with **180,457** and **181,267** trips respectively.
# MAGIC   * **Lowest Demand:** **4:00 AM (Hour 4)** with just **12,174 trips**.

# COMMAND ----------

# To answer Q2 (Top Revenue Hours):# Q2: Top revenue hours
display(
    Hourly_demand_df.select("pickup_hour", "total_revenue")
    .orderBy(col("total_revenue").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2: When does revenue peak and how does it correlate with volume?**
# MAGIC   * **Peak Revenue:** Peaks at **5:00 PM (Hour 17)** at **$5,353,419.54**, directly aligning with volume.
# MAGIC   * Total hourly revenue rises steadily from 6:00 AM onward until 6:00 PM.
# MAGIC

# COMMAND ----------

# Q3: Trip distance, duration, and fare across hours
display(
    Hourly_demand_df.select(
        "pickup_hour", 
        "avg_distance_miles", 
        "avg_duration_minutes", 
        "avg_fare"
    ).orderBy("pickup_hour")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q3: Do trip distance and duration fluctuate between daytime rush hours and late-night?**
# MAGIC   * **Yes, significantly:** 
# MAGIC     * Daytime rush hour (Hour 17–18) trips are shorter in distance (~**2.96 to 3.11 miles**) due to dense intra-borough traffic.
# MAGIC     * Early morning (Hour 5:00 AM) trips have the longest average distance (**6.84 miles**) and duration (**22.19 minutes**), driven by airport and inter-borough runs, yielding the highest average fare of **$28.89**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **GOLD TABLE 2**

# COMMAND ----------


# Business Rule: Physical Borough Corridor Filtering
# Raw zone references map unassigned/outside coordinates to 'Unknown' or 'N/A'.
# While retained in Silver for financial reconciliation, they are excluded from Gold
# corridor reporting to ensure traffic and distance metrics reflect authentic NYC boroughs.



invalid_boroughs = ["N/A", "Unknown"]

gold_borough_metrics_df = silver_df.filter(
    col("pickup_borough").isNotNull() & 
    col("dropoff_borough").isNotNull() &
    (~col("pickup_borough").isin(invalid_boroughs)) &
    (~col("dropoff_borough").isin(invalid_boroughs))
).groupBy("pickup_borough", "dropoff_borough").agg(
    count("*").alias("total_trips"),
    round(sum("total_amount"), 2).alias("total_revenue"),
    round(avg("fare_amount"), 2).alias("avg_fare"),
    round(avg("trip_distance"), 2).alias("avg_distance_miles")
).orderBy(col("total_trips").desc())

# Save to Unity Catalog
gold_borough_metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_borough_metrics")

print("Saved clean: workspace.default.gold_borough_metrics")
display(gold_borough_metrics_df)

# COMMAND ----------

# Q4: Top 5 corridors by trip volume
display(
    gold_borough_metrics_df.select(
        "pickup_borough", "dropoff_borough", "total_trips", "total_revenue"
    ).orderBy(col("total_trips").desc()).limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC * **Q4: Which routes handle the heaviest taxi traffic?**
# MAGIC   * **Top Route:** **Manhattan to Manhattan** dominates overall volume with **2,041,355 trips** generating over **$43.7M**.
# MAGIC   * **Second Top Route:** **Queens to Manhattan** (airport inbound traffic) with **137,676 trips** generating **$11.36M+**.

# COMMAND ----------

# Q5: Highest average fare corridors (minimum 100 trips for statistical relevance)
display(
    gold_borough_metrics_df.filter(col("total_trips") >= 100)
    .select("pickup_borough", "dropoff_borough", "avg_fare", "avg_distance_miles", "total_trips")
    .orderBy(col("avg_fare").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q5: Which corridors generate the highest average fares and longest distances?**
# MAGIC   * **Queens to EWR:** **$155.27 avg fare** (33.98 avg miles).
# MAGIC   * **Queens to Staten Island:** **$102.42 avg fare** (26.52 avg miles).
# MAGIC   * **Manhattan to EWR:** **$95.32 avg fare** (17.64 avg miles).
# MAGIC   * **Manhattan to Staten Island:** **$75.30 avg fare** (18.59 avg miles).
# MAGIC   * **Queens to Bronx:** **$59.56 avg fare** (15.74 avg miles).

# COMMAND ----------

from pyspark.sql.functions import when

# Q6: Intra-Borough vs. Inter-Borough Mobility Ratio
# Measures what percentage of taxi trips remain inside the pickup borough
# versus crossing boundaries into another borough.

mobility_df = silver_df.filter(
    col("pickup_borough").isNotNull() & 
    col("dropoff_borough").isNotNull() &
    (~col("pickup_borough").isin(["N/A", "Unknown"])) &
    (~col("dropoff_borough").isin(["N/A", "Unknown"]))
).withColumn(
    "trip_type",
    when(col("pickup_borough") == col("dropoff_borough"), "Intra-Borough")
    .otherwise("Inter-Borough")
)

total_valid_trips = mobility_df.count()

display(
    mobility_df.groupBy("trip_type").agg(
        count("*").alias("total_trips"),
        round((count("*") / total_valid_trips) * 100, 2).alias("percentage_share"),
        round(sum("total_amount"), 2).alias("total_revenue")
    ).orderBy(col("total_trips").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC * **Q6: What proportion of taxi trips stay within the same borough versus crossing into other boroughs?**
# MAGIC   * **Intra-Borough (Within same borough):** **85.58%** of all trips (**2,119,401 trips**), generating **$46.60M** in revenue.
# MAGIC   * **Inter-Borough (Crossing borough lines):** **14.42%** of trips (**357,250 trips**), generating **$23.89M** in revenue.
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 3

# COMMAND ----------


# Gold Table 3: Payment Economics & Tipping Behavior
# Maps payment IDs to human-readable names and computes transaction shares.

from pyspark.sql.functions import when

silver_payment_df = silver_df.withColumn(
    "payment_method",
    when(col("payment_type") == 1, "Credit Card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No Charge")
    .when(col("payment_type") == 4, "Dispute")
    .otherwise("Other")
)

gold_payment_tipping_df = silver_payment_df.groupBy("payment_method").agg(
    count("*").alias("total_transactions"),
    round(sum("total_amount"), 2).alias("total_revenue"),
    round(avg("fare_amount"), 2).alias("avg_fare"),
    round(sum("tip_amount"), 2).alias("total_tips_recorded"),
    round(avg("tip_amount"), 2).alias("avg_tip_recorded"),
    round(avg("tip_percentage"), 2).alias("avg_tip_pct")
).orderBy(col("total_transactions").desc())

# Save to Unity Catalog
gold_payment_tipping_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_payment_tipping")

print("Saved clean: workspace.default.gold_payment_tipping")
display(gold_payment_tipping_df)

# COMMAND ----------

 
# Q7: Payment Method Adoption & Cashless Mobility Share

total_txns = silver_payment_df.count()

display(
    silver_payment_df.groupBy("payment_method").agg(
        count("*").alias("total_transactions"),
        round((count("*") / total_txns) * 100, 2).alias("transaction_share_pct"),
        round(sum("total_amount"), 2).alias("total_revenue")
    ).orderBy(col("total_transactions").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q7: What percentage of riders pay with credit cards versus cash?**
# MAGIC * **Credit Card:** **87.08%** (2,173,349 transactions)
# MAGIC * **Cash:** **11.53%** (287,690 transactions)
# MAGIC * **Dispute:** **1.06%** (26,405 transactions)
# MAGIC * **No Charge:** **0.33%** (8,268 transactions)

# COMMAND ----------

# Q8: Average tip percentage by pickup borough for card transactions
display(
    silver_df.filter(
        (col("payment_type") == 1) & 
        col("pickup_borough").isNotNull() & 
        (~col("pickup_borough").isin(["N/A", "Unknown"]))
    ).groupBy("pickup_borough").agg(
        round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
        count("*").alias("total_card_trips")
    ).orderBy(col("avg_tip_pct").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * **Q8: How does average tip percentage vary by pickup borough for card transactions?**
# MAGIC   * **Manhattan:** **26.29%** (1,907,144 trips)
# MAGIC   * **Queens:** **19.76%** (200,385 trips)
# MAGIC   * **EWR:** **12.89%** (20 trips)
# MAGIC   * **Staten Island:** **4.24%** (47 trips)
# MAGIC   * **Brooklyn:** **3.43%** (44,517 trips)
# MAGIC   * **Bronx:** **0.63%** (17,600 trips)