# Company-Operations-Data-Analytics-Pipeline
## Overview:
End-to-End Data Pipeline built with Airflow, dbt, and PostgreSQL. Extracts Notion data (People Directory) for transformation into analytics-ready dimensions (dim_employees). 
Demonstrates robust ETL/ELT, scheduling, and data modeling skills.

This repository contains the source code and infrastructure configuration for an End-to-End ELT (Extract, Load, Transform) data pipeline. The pipeline's purpose is to sync operational data from **Notion** into a **PostgreSQL** data warehouse, transforming the raw data into clean analytical models using **dbt**, and visualizing the final output in **Metabase**.

## 1. Project Architecture (ELT Stack)

The project is built on an isolated, containerized environment that can be launched with a single command.

| **Component** | **Role** | **Technology Used** | **Location** |
| --- | --- | --- | --- |
| **Orchestration** | Scheduler, Workflow Manager | **Apache Airflow 3+** | Docker Container |
| **Extraction (E)** | Source Data Input | **Notion API** | External Service |
| **Loading (L)** | Raw Data Storage (Data Warehouse) | **PostgreSQL 16** | Docker Container |
| **Transformation (T)** | Data Modeling & Cleaning | **dbt (Data Build Tool)** | Docker Container |
| **Visualization** | Business Intelligence Dashboard | **Metabase** | Docker Container |

---

## 2. Environment Setup & Launch Guide (For Maintenance)

### A. Prerequisites

The following software must be installed on the host machine (server) to run this project:

1. **Docker Desktop:** (Requires continuous uptime for the pipeline to run 24/7).
2. **Git:** For version control.

### B. Setup Steps

1. **Clone the Repository:**
    
    `git clone https://github.com/StellaPng/Company-Operations-Data-Analytics-Pipeline.git
    cd airflow-data-pipeline`
    
2. **Launch All Services:** Run the main Docker Compose command. This starts Airflow, PostgreSQL, dbt, and Metabase simultaneously.
    
    `docker compose up`
    

### C. First-Time Initialization (Done Once)

When running for the first time, Airflow's database needs to be initialized (already performed, but required if volumes are rebuilt).



`docker compose up airflow-init # Runs necessary database migrations`

---

## 3. Integration & Credentials Setup

The pipeline uses Airflow's secure **Connections** feature to manage external passwords.

### A. Notion API Secret (`notion_conn`)

This connection is used for **Extraction**.

| **Airflow Field** | **Value** | **Notes** |
| --- | --- | --- |
| **Connection ID** | `notion_conn` | Used by DAG code. |
| **Connection Type** | `Generic` |  |
| **Extra (JSON)** | `{"host": "https://api.notion.com", "password": "YOUR_NOTION_TOKEN", "database_id": "YOUR_DATABASE_ID"}` | **MANDATORY:** Replace the placeholders with the current Notion Token and the People Directory ID. |

### B. Database Credentials (`postgres_default`)

This connection is used by the **dbt Transformation** layer.

| **Airflow Field** | **Value** | **Notes** |
| --- | --- | --- |
| **Connection ID** | `postgres_default` | Used by dbt and Airflow tasks. |
| **Host** | `postgres` | **Crucial:** This is the internal Docker service name. |
| **Schema/DB Name** | `airflow` |  |
| **Login/Password** | `airflow` / `airflow` | Default credentials for the Docker container. |

---

## 4. Pipeline Execution and Maintenance

### A. Pipeline Name and Tasks

| **DAG ID (Airflow)** | **Purpose** |
| --- | --- |
| **`notion_to_postgres_loaddata_dag`** | Executes the full ELT process daily. |

| **Task Name** | **Function** |
| --- | --- |
| `get_notion_credentials` | Securely retrieves API secrets from the Airflow UI. |
| `extract_notion_data` | Uses the Notion API to pull raw data. |
| `load_data_to_postgres` | Writes raw data to the `notion_people` table in PostgreSQL. |

### B. Data Transformation (dbt Commands)

To run the dbt models inside the container and update the clean tables:



`# Executing the full transformation model run
docker compose run dbt-cli bash -c "cd dbt_project && dbt run"`

---

## 5. Accessing the Final Dashboard

The Metabase server provides the visualization layer, connecting directly to the PostgreSQL database inside the Docker network.

### A. Metabase Access

1. Ensure services are running (`docker compose up`).
2. Access Metabase in your browser: **`http://localhost:3000`**
3. The final data model is available by querying the clean table: **`"dim_employees"`**.

### B. Key Analytical Models
