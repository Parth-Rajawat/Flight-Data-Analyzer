# Flight-Data-Analyzer

## Project Overview:
This project demonstrates an end-to-end data engineering pipeline for flight analytics and delay prediction. Synthetic flight data is generated, wrangled with PySpark, transformed into multiple analytical views, and loaded into a MySQL database. A Streamlit dashboard visualizes key insights such as flight status trends and busiest airports, while an integrated Random Forest model (~70% accuracy) predicts flight delays. The entire workflow is orchestrated and scheduled daily using Apache Airflow.

## Architecture:
<img width="1018" height="507" alt="Screenshot 2025-09-02 at 10 06 45 PM" src="https://github.com/user-attachments/assets/95535b58-bef3-4324-bf9f-062b41c9949c" />
The pipeline is scheduled and orchestrated by Apache Airflow (dashed arrows). Data flows (solid arrows) from Synthetic Data Generator → PySpark Wrangling → Transformations (status, traffic, delay) → MySQL Database, which powers a Streamlit Dashboard + ML Predictor. The workflow runs automatically every day at midnight.

## 🛠 Tech Stack

Python 3.12 – Core programming language
PySpark 3.5.1 – Distributed data wrangling & transformations
MySQL 8.0 – Relational database for structured storage
Streamlit 1.38 – Interactive dashboard & ML integration
Plotly 5.24 – Rich visualizations for analytics
scikit-learn 1.5 – Machine learning (Random Forest Classifier)
Apache Airflow 2.10 – Orchestration & scheduling (daily DAG runs)
Papermill – Executes transformation notebooks programmatically
