# walmart Retail Sales Analytics Platform (Databricks End-to-End Data Engineering Project)

##  Overview

This project demonstrates an **end-to-end Retail Sales Analytics Platform** built using **Databricks, Delta Lake and Unity Catalog**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

The solution focuses on transforming **raw, real-world retail data** containing malformed dates and inconsistent numeric values into **clean, governed and analytics-ready datasets**.  
The final output supports fast analytical queries and business intelligence use cases using a **star schema design**.

---

## Architecture

**Medallion Architecture ensures**:
- Traceability of raw data
- Incremental data quality improvements
- Clear separation between ingestion, processing and analytics

---

## Highlights

- Implemented **Medallion Architecture** using Delta Lake (Bronze, Silver, Gold)
- Handled **real-world dirty data** using tolerant casting (`try_cast`, `try_to_date`)
- Designed a **star schema** with fact and dimension tables for analytics
- Applied **Unity Catalog governance** for schema control and access management
- Built **analytics-ready Gold tables** for BI consumption
- Applied **performance optimizations**:
  - Z-Ordering
  - Table statistics
  - Data quality constraints
- Followed **enterprise-grade Databricks best practices**

---

## Tech Stack

- **Databricks** – Distributed data processing & analytics
- **Delta Lake** – Reliable, ACID-compliant data storage
- **Unity Catalog** – Centralized governance and schema management
- **PySpark** – Data transformations and cleaning
- **SQL** – Analytics, modeling and optimizations
- **Medallion Architecture** – Scalable data design pattern

---


