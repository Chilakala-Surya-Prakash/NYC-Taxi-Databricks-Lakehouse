<div align="center">

# 🚕 NYC Taxi Lakehouse Platform
### End-to-End Medallion Lakehouse Architecture on Databricks & Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Unity_Catalog-00ADD8?style=for-the-badge)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](#license)

**A high-performance Lakehouse data engineering project built on the Databricks platform, transforming 2.49+ million NYC Yellow Taxi records into production Delta tables and executive AI/BI Lakeview dashboards.**

</div>

---

## 📌 Executive Summary

This project implements an enterprise **Medallion Lakehouse Architecture** (Bronze ➔ Silver ➔ Gold) on **Databricks** using **PySpark** and **Unity Catalog**. 

The pipeline ingests raw high-volume NYC Yellow Taxi trip records and geographic taxi zone metadata, enforces schema validation and audit lineage, executes strict data quality filters, enriches spatial dimensions, and computes analytics-ready Gold tables powering an interactive executive **Databricks AI/BI Lakeview Dashboard**.

```
                        MEDALLION ARCHITECTURE
                        
 ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
 │   BRONZE LAYER  │       │  SILVER LAYER   │       │   GOLD LAYER    │
 │ Raw Delta Ingest│  ──▶  │Cleansed/Enriched│  ──▶  │Business Aggs    │
 └─────────────────┘       └─────────────────┘       └─────────────────┘
         │                          │                         │
         ▼                          ▼                         ▼
 • landing_zone Volumes     • Deduplication           • gold_hourly_demand
 • Explicit schema          • Spatial Zone Enriched   • gold_borough_metrics
 • Ingestion timestamp      • Business Filters        • gold_payment_tipping
 • File source metadata     • Feature Engineering             │
                                                              ▼
                                                   ┌─────────────────────┐
                                                   │ DATABRICKS AI/BI    │
                                                   │ Executive Dashboard │
                                                   └─────────────────────┘
```

---

## 🏗️ Architecture & Pipeline Layers

### 🥉 Bronze Layer — Raw Ingestion & Auditing
- **Files Ingested:** `yellow_tripdata_2024-01.parquet` (2.9M+ raw rows) and `taxi_zone_lookup.csv` (265 zones).
- **Storage:** Managed Delta tables stored in Unity Catalog (`workspace.default`).
- **Audit Columns:** Every record is stamped with:
  - `ingestion_timestamp`: UTC timestamp when data was ingested into the Lakehouse.
  - `file_name`: Original file lineage via `_metadata.file_path`.
- **Target Tables:**
  - `workspace.default.bronze_yellow_taxi`
  - `workspace.default.bronze_taxi_zones`

### 🥈 Silver Layer — Cleaning, Validation & Enrichment
- **Data Quality & Boundary Rules:**
  - `trip_distance`: $0 < \text{distance} < 200\text{ miles}$
  - `passenger_count`: $> 0$
  - `fare_amount`: $\$2.50 \le \text{fare} \le \$1,000$
  - `trip_duration_minutes`: $1 \le \text{duration} \le 360\text{ minutes}$ (computed via `timestamp_diff`)
- **Deduplication:** Dropped duplicates based on `[VendorID, tpep_pickup_datetime, PULocationID, DOLocationID]`.
- **Spatial Enrichment:** Left-joined with `bronze_taxi_zones` on both pickup (`PULocationID`) and dropoff (`DOLocationID`) to enrich with `pickup_borough`, `pickup_zone`, `dropoff_borough`, and `dropoff_zone`.
- **Feature Engineering:**
  - `pickup_date`, `pickup_hour`, and `pickup_day_name` extracted from timestamp.
  - `payment_type_desc` (Credit Card, Cash, No Charge, Dispute, Other).
  - `tip_percentage` calculated relative to base fare for card transactions.
- **Target Table:** `workspace.default.silver_yellow_taxi` (2,495,651 cleansed records).

### 🥇 Gold Layer — Business Aggregations & Analytics
The Gold layer materializes purpose-built Delta tables answering critical operational and revenue questions:

#### 1. `workspace.default.gold_hourly_demand`
Aggregates trip volume, total revenue, average fare, average distance, and average duration by hour of day (0–23).
- **Peak Demand:** **5:00 PM – 6:59 PM (Hours 17 & 18)** with **180,457** and **181,267** trips.
- **Peak Revenue:** 5:00 PM (Hour 17) generated **$5,353,419.54**.
- **Distance Fluctuation:** Early morning trips (5:00 AM) average **6.84 miles** / **$28.89 avg fare** (airport runs), whereas evening rush-hour trips average **2.96–3.11 miles**.

#### 2. `workspace.default.gold_borough_metrics`
Computes trip volume, total revenue, average fare, and distance across pickup-dropoff borough pairs.
- **Top Corridor by Volume:** Manhattan ➔ Manhattan handles **2,041,355 trips** (**$43.7M+** revenue).
- **Inbound Airport Gateway:** Queens ➔ Manhattan handles **137,676 trips** (**$11.36M+** revenue).
- **Highest Yield Long-Haul Corridors:**
  - Queens ➔ EWR: **$155.27** avg fare (33.98 miles)
  - Queens ➔ Staten Island: **$102.42** avg fare (26.52 miles)
  - Manhattan ➔ EWR: **$95.32** avg fare (17.64 miles)
- **Mobility Split:**
  - **Intra-Borough (within same borough):** **85.58%** (2,119,401 trips, $46.60M revenue)
  - **Inter-Borough (crossing boroughs):** **14.42%** (357,250 trips, $23.89M revenue)

#### 3. `workspace.default.gold_payment_tipping`
Tracks payment method adoption and tipping patterns:
- **Cashless Adoption:** **87.08%** Credit Card vs. **11.53%** Cash.
- **Borough Tipping Behavior (Card Transactions):**
  - Manhattan: **26.29%** average tip (1,907,144 trips)
  - Queens: **19.76%** average tip (200,385 trips)
  - EWR: **12.89%** average tip
  - Staten Island: **4.24%** average tip
  - Brooklyn: **3.43%** average tip
  - Bronx: **0.63%** average tip

---

## 📊 Databricks AI/BI Executive Dashboard

The repository includes the complete Databricks Lakeview Dashboard export file:
[`dashboards/NYC Taxi Executive Mobility & Revenue Dashboard.lvdash.json`](dashboards/NYC%20Taxi%20Executive%20Mobility%20&%20Revenue%20Dashboard.lvdash.json).

### Dashboard Layout & Canvas Pages:

#### 1. Demand & Economics Page
- **Total Trips by Pickup Hour** *(Line chart)*: Time-series curve of 24-hour demand.
- **Total Revenue by Pickup Borough** *(Bar chart)*: Revenue generation broken down by borough.
- **Payment Method Distribution (Card vs. Cash)** *(Pie chart)*: Payment share breakdown.
- **Total Revenue by Hour** *(Bar chart)*: Financial performance across hourly buckets.

#### 2. Corridors & Tipping Page
- **Trip Distance & Duration Fluctuations by Hour** *(Multi-line dual measure)*: Traffic congestion vs. distance.
- **Heaviest Route Corridors by Volume** *(Heatmap)*: Origin-destination density matrix.
- **Highest Average Fare by Borough Corridor** *(Bar chart)*: Yield and pricing per corridor.
- **Borough-to-Borough Trip Flow (Intra vs. Inter)** *(Heatmap)*: Cross-borough transit patterns.
- **Average Card Tip Percentage by Pickup Borough** *(Bar chart)*: Spatial tipping generosity.

---

## 📁 Repository Structure

```
NYC-Taxi-Databricks-Lakehouse/
│
├── notebooks/                                      # Databricks Notebook exports (.ipynb)
│   ├── 01_BRONZE_INGESTION.ipynb                   # Bronze layer ingestion with execution outputs
│   ├── 02_SILVER_TRANSFORMATION.ipynb              # Silver layer cleaning & feature engineering
│   └── 03_GOLD_AGGREGATIONS.ipynb                  # Gold layer business aggregations & questions
│
├── src/                                            # Pure Python pipeline scripts (.py)
│   ├── 01_BRONZE_INGESTION.py
│   ├── 02_SILVER_TRANSFORMATION.py
│   └── 03_GOLD_AGGREGATIONS.py
│
├── dashboards/                                     # Databricks AI/BI Lakeview Dashboard
│   └── NYC Taxi Executive Mobility & Revenue Dashboard.lvdash.json
│
├── data/                                           # Dimension and reference data
│   └── taxi_zone_lookup.csv                        # NYC TLC Official Taxi Zone lookup (265 zones)
│
├── .gitignore
└── README.md
```

---

## 🚀 How to Run on Databricks

### 1. Data Preparation
1. Download the NYC TLC Trip Record dataset (e.g., `yellow_tripdata_2024-01.parquet`) from the [NYC TLC Official Website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
2. Upload `yellow_tripdata_2024-01.parquet` and `data/taxi_zone_lookup.csv` into your Databricks Unity Catalog Volume:
   ```
   /Volumes/workspace/default/landing_zone/
   ```

### 2. Import Notebooks
1. In your Databricks workspace, go to **Workspace** ➔ **Import**.
2. Import the three notebooks from the `notebooks/` directory in order:
   - `01_BRONZE_INGESTION.ipynb`
   - `02_SILVER_TRANSFORMATION.ipynb`
   - `03_GOLD_AGGREGATIONS.ipynb`

### 3. Import the AI/BI Dashboard
1. In Databricks, navigate to **Dashboards** (or Lakeview).
2. Click **Create Dashboard** ➔ click the `...` menu ➔ **Import from JSON**.
3. Select `dashboards/NYC Taxi Executive Mobility & Revenue Dashboard.lvdash.json`.
4. Connect the dataset sources to `workspace.default.gold_*` tables and publish!

---

## 🛠️ Technology Stack

- **Cloud Platform:** Databricks
- **Processing Engine:** Apache Spark / PySpark
- **Table Format:** Delta Lake
- **Governance & Catalog:** Databricks Unity Catalog
- **BI & Visualizations:** Databricks AI/BI Lakeview Dashboards
- **Language:** Python & SQL

---

## 👤 Author

**Chilakala Surya Prakash**  
- GitHub: [@Chilakala-Surya-Prakash](https://github.com/Chilakala-Surya-Prakash)  
- LinkedIn: [Chilakala Surya Prakash](https://www.linkedin.com/in/chilakalasuryaprakash/)

---

## 📄 License

This project is licensed under the MIT License.
