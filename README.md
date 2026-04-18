# OpenLiberary_Data_Analysis-Data_Engineering-etl-pipeline
Built an end-to-end ETL pipeline using OpenLibrary API. Extracted raw JSON (Bronze), transformed into clean relational tables (Silver) with keys, deduplication, and validation. Loaded into MySQL for analysis, enabling queries on subjects, authors, and trends in a scalable setup.
# Open Library Book Discovery & Subject Analytics Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![MySQL](https://img.shields.io/badge/mysql-8.0-orange.svg)](https://www.mysql.com/)

An end-to-end Data Engineering pipeline following the **Medallion Architecture**. This project ingests book and author data from the Open Library API, processes it through a local data lake (Bronze), transforms it into a relational MySQL schema (Silver), and generates analytical insights (Gold).

## 🏗️ Architecture Diagram
<img width="1084" height="755" alt="image" src="https://github.com/user-attachments/assets/8acf142c-f587-441f-95a8-79aed9f6a16e" />



---

## 🚀 Project Workflow

### 1. Bronze Layer (Extraction)
The extraction process uses **Python Multi-threading** to pull data concurrently from four different API endpoints.
* **Subjects:** Discovery of works based on config subjects.
* **Author Details:** Biographic info for every discovered author.
* **Author Works:** Complete bibliography for authors.
* **Work Details:** Metadata for specific book keys.

### 2. Silver Layer (Transformation & Loading)
Using **Pandas**, raw JSON files are flattened into a structured relational format.
* **Data Cleaning:** Handling nulls, deduplicating keys, and formatting timestamps.
* **Persistence:** Data is loaded into MySQL using `executemany` for high-performance batch insertion.

### 3. Quality Assurance
The pipeline includes a dedicated `quality.py` module that runs 6 critical checks before finalizing the batch:
* Primary Key integrity (Null checks for `work_key`, `author_key`).
* Foreign Key validation between subjects and works.
* Range validation for `first_publish_year`.

---

## 🛠️ Tech Stack & Skills Demonstrated
* **Languages:** Python (Multi-threading, Requests, Pandas)
* **Database:** MySQL (Relational Modeling, Constraints, Joins)
* **DevOps:** Logging, Batch ID tracking, YAML Configuration
* **Architecture:** Medallion (Bronze/Silver/Gold)

---

## 📂 Database Schema (Silver Layer)
The project creates a robust relational schema in MySQL:
- `silver_ingestion_batch`: Audit trail for every pipeline run.
- `silver_work`: Core metadata for books.
- `silver_author`: Author biographies and stats.
- `silver_work_author` & `silver_work_subject`: Bridge tables for Many-to-Many relationships.
- `silver_work_search_snapshot`: Captures API ranking at the time of ingestion.

---

## 📈 Analytics (Gold Insights)
The following SQL queries are used to extract value from the processed data:
```sql
-- Example: Top author diversity by subject
SELECT 
    ws.subject_name, 
    COUNT(DISTINCT wa.author_key) AS unique_authors
FROM silver_work_subject ws
JOIN silver_work_author wa ON ws.work_key = wa.work_key
GROUP BY ws.subject_name
ORDER BY unique_authors DESC;
