# Walmart Retail Sales Analytics Platform (Databricks End-to-End Data Engineering Project)

##  Overview

This project demonstrates an **end-to-end Retail Sales Analytics Platform** built using **Databricks, Delta Lake and Unity Catalog**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

The solution focuses on transforming **raw, real-world retail data** containing malformed dates and inconsistent numeric values into **clean, governed and analytics-ready datasets**.  
The final output supports fast analytical queries and business intelligence use cases using a **star schema design**.

**Note:**  
- This project conceptually follows an AWS-based architecture using Amazon S3 as the raw data source. However, due to limitations of the Databricks Community Edition (which does not support direct cloud storage  integrations), the actual implementation uses Databricks-managed storage to simulate the S3 ingestion layer.    
- The overall architecture, data modeling, transformations, and optimization techniques remain aligned with real-world, production-grade data engineering practices.

---

## Architecture
<img width="1024" height="559" alt="Walmart project architecture" src="https://github.com/user-attachments/assets/3eddf26c-aaa8-4717-9f5f-67e0452edc75" />

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


