# 🩺 Public Health Data Pipeline

This project demonstrates the design and development of a **scalable, end-to-end ETL (Extract–Transform–Load) pipeline** for ingesting and processing public health data from the **CDC Socrata API**.  

The pipeline ingests raw data, stages it in **PostgreSQL**, performs **data cleaning and validation**, and optionally loads the processed data into **Google BigQuery** for downstream analytics and visualization.  
It also exposes a **FastAPI-based validation microservice** for automated data-quality checks and supports alerting through email or logging integrations.

---

## 🎯 Objectives

- Automate the ingestion of large-scale CDC public datasets (100k+ records).  
- Implement modular, testable data workflows using Python.  
- Apply validation checks through a RESTful API.  
- Leverage containerized infrastructure for reproducible local development.  
- Prepare data for analytical use cases in BigQuery.

---

## 🏗️ Architecture Overview

```text
           +----------------------+
           |   CDC Socrata API    |
           +----------+-----------+
                      |
                      v
           +----------------------+
           |   Ingestion Script   |   ← pipeline.py
           +----------+-----------+
                      |
                      v
           +----------------------+
           |  PostgreSQL Staging  |   ← docker-compose Postgres
           +----------+-----------+
                      |
                      v
           +----------------------+
           | Data Cleaning & DQ   |   ← validation_api.py
           +----------+-----------+
                      |
                      v
           +----------------------+
           |  BigQuery (Optional) |
           +----------------------+
                      |
                      v
           +----------------------+
           |  Alerts / Logging    |
           +----------------------+
