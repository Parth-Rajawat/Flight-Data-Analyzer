# Flight-Data-Analyzer

## Project Overview:
This project demonstrates an end-to-end data engineering pipeline for flight analytics and delay prediction. Synthetic flight data is generated, wrangled with PySpark, transformed into multiple analytical views, and loaded into a MySQL database. A Streamlit dashboard visualizes key insights such as flight status trends and busiest airports, while an integrated Random Forest model (~70% accuracy) predicts flight delays. The entire workflow is orchestrated and scheduled daily using Apache Airflow.

## Architecture:
<img width="976" height="439" alt="Architecture" src="https://github.com/user-attachments/assets/2be0ac56-e920-48ee-837b-033c6eefd083" />

This architecture represents a complete end-to-end data engineering pipeline. The process begins at the Source with a Synthetic Flight Generator that produces daily flight records. Data then flows into the Transformation Layer, where PySpark is used for wrangling, cleaning, and analytical transformations such as flight delay prediction, airport traffic analysis, and flight status trends. The outputs are stored in the Destination Layer within a MySQL database. Finally, the Front End is powered by a Streamlit application, which provides an interactive dashboard and integrates a flight delay prediction model. The entire workflow is orchestrated and scheduled by Apache Airflow, which ensures the pipeline runs automatically every day at midnight, handling task sequencing and parallel execution.

## 🛠 Tech Stack  

| Tool                    | Purpose                                        |
|-------------------------|------------------------------------------------|
| **Python 3.12**         | Core programming language                      |
| **PySpark 3.5.1**       | Distributed ETL & machine learning (MLlib)     |
| **MySQL 8.0**           | Relational database for structured storage     |
| **Streamlit 1.38**      | Interactive dashboard & ML integration         |
| **Plotly 5.24**         | Interactive visualizations for dashboard       |
| **Matplotlib**          | Static charts in transformation notebooks      |
| **Apache Airflow 2.10** | Orchestration & scheduling (daily DAG runs)    |
| **Papermill**           | Executes transformation notebooks              |

## ✨ Features  

| Feature                     | Description                                   |
|-----------------------------|-----------------------------------------------|
| **Synthetic Data Generation** | Creates **100k** flight records for **last** 10 days if the file is empty, else adds the day before the current day and deletes the data older than 10 days for rolling 10 days window (airlines, airports, delays, etc.) |
| **Automated ETL**           | Cleansing, enrichment, and feature engineering using **PySpark** |
| **Analytical Transformations** | Status trends, busiest airports, delay statistics (visualized with Matplotlib/Plotly) |
| **Streamlit Dashboard**     | 5+ interactive visual reports powered by MySQL |
| **ML Delay Prediction**     | Random Forest Classifier via **PySpark MLlib (~70% accuracy)** |
| **Airflow DAG Orchestration** | Daily scheduling, sequential + parallel task execution |

## 🚀 How to Run the Pipeline  

### 1. Clone the Repository  
```bash
git clone https://github.com/Parth-Rajawat/flight-data-engineering.git
cd flight-data-engineering
```

### 2. Create and Activate Virtual Environment
```bash
python3 -m venv flight-env
source flight-env/bin/activate   # Mac/Linux
# flight-env\Scripts\activate    # Windows
```

### 3. Install Requirements
```bash
pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"
```

### 4. Set up MySQL Database
**->** After running the MYSQL server, run the below command to create database
```SQL
CREATE DATABASE flight_db;
```
**->** make sure to update these database credentials in `app.py` as well as in `loader.py` as the streamlit page fetches data from MySQL and loader loads the transformed dataframes into MYSQL.

### 5. Initialize Airflow
**->** run the below command
```bash
export AIRFLOW_HOME=$(pwd)/airflow   # keeps airflow inside project so it does not touch the system
airflow db init
airflow users create \
    --username admin \
    --firstname name \
    --lastname name \
    --role Admin \
    --email admin@example.com
```

### 6. Start Airflow Services
Open two terminals and run:
6.1. For `Terminal 1`:
```bash
airflow scheduler
```
6.2. For `Terminal 2`:
```bash
airflow webserver --port 8080
```
