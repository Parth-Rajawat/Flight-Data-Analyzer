# Flight-Data-Engineering-Pipeline

## Project Overview:
This project demonstrates an end-to-end data engineering pipeline for flight analytics and delay prediction. Synthetic flight data is generated, wrangled with PySpark, transformed into multiple analytical views, and loaded into a MySQL database. A Streamlit dashboard visualizes key insights such as flight status trends and busiest airports, while an integrated Random Forest model (~80% accuracy) predicts flight delays. The entire workflow is orchestrated and scheduled daily using Apache Airflow.
