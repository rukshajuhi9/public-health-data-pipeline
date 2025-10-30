# 🩺 Public Health Data Pipeline

An end-to-end **ETL pipeline** that automates the ingestion, validation, and transformation of **CDC public health data** using Python, PostgreSQL, Docker, and Google BigQuery.  

This project demonstrates a **real-world data engineering workflow** — from API ingestion to cloud-ready analytics — and includes a **FastAPI-based validation microservice** for automated data-quality checks and logging.

---

## 🎯 Project Objectives

- Build a scalable and reproducible ETL pipeline for CDC public health datasets (100k+ records).  
- Implement automated **data cleaning and validation** prior to analysis.  
- Use **Docker** for consistent local development environments.  
- Integrate **PostgreSQL → BigQuery** for analytics-ready storage.  
- Develop a **FastAPI microservice** to automate validation and reporting.  
- Include **testing, logging, and alerting** for quality assurance.

---

## 🧩 Tech Stack

| Category | Tools / Technologies |
|-----------|----------------------|
| Language | Python 3.10+ |
| Frameworks | FastAPI, Requests, Pandas |
| Database | PostgreSQL (Dockerized) |
| Cloud | Google BigQuery |
| Orchestration | Bash Script + Python Orchestrator |
| Containerization | Docker, Docker Compose |
| Testing | Pytest, Unittest |
| Logging & Alerts | Python Logging, SendGrid (optional) |

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
           |      BigQuery
           +----------+-----------+
                      |
                      v
           +----------------------+
           |  Alerts / Logging    |
           +----------------------+
