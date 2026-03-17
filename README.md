## 📊 Vendor Performance Data Analytics – End-to-End Project

### 📌 Project Overview

This project analyzes vendor performance to help a retail/wholesale company optimize profitability. It simulates a real-world analytics workflow by building an **ETL pipeline**, performing **statistical analysis in Python**, and developing an **interactive Power BI dashboard** for business insights.

---

### 🎯 Business Problem

The company aims to improve profitability by addressing the following challenges:

* Inefficient product pricing
* Poor inventory turnover
* High dependency on specific vendors

The objective is to identify **underperforming brands**, evaluate **top vendor contributions**, and analyze how **bulk purchasing impacts unit costs**.

---

### 🛠 Tech Stack

| Category          | Tools                                    |
| ----------------- | ---------------------------------------- |
| **Database**      | SQLite, SQL                              |
| **Programming**   | Python (Pandas, SQLAlchemy)              |
| **Visualization** | Matplotlib, Seaborn, Power BI            |
| **Statistics**    | SciPy (T-tests, Confidence Intervals)    |
| **Workflow**      | ETL pipeline with logging and automation |

---

### ⚙️ Project Workflow

#### 1️⃣ Data Ingestion & ETL Pipeline

* Extracted raw data from multiple CSV files (Purchases, Sales, Inventory, etc.).
* Built a **Python ingestion script** to load data into an SQLite database.
* Implemented **logging** to monitor execution and detect errors during the **2GB data ingestion process**.

#### 2️⃣ SQL Analysis & Query Optimization

* Explored database tables to identify key variables for vendor performance.
* Created an **aggregated vendor summary table** by joining multiple large tables (one containing **10M+ records**).
* Optimized SQL queries to reduce memory usage and ensure the pipeline runs **in under one minute**.
