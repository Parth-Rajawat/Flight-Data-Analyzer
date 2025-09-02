from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
import os

# Base paths
BASE_DIR = "/Users/parthmac/Desktop/Projects/Flight Data Engineering"
EXTRACTION_DIR = os.path.join(BASE_DIR, "extraction and wrangling")
TRANSFORMATION_DIR = os.path.join(BASE_DIR, "transformation for use cases")

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["noemail@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    "flight_pipeline_dag",
    default_args=default_args,
    description="Flight ETL pipeline with transformations",
    schedule_interval="0 0 * * *",   # daily at midnight
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["flight", "etl"],
) as dag:

    # Step 1: Synthetic flight generator
    generate_data = BashOperator(
        task_id="generate_synthetic_flights",
        bash_command=f"python '{os.path.join(EXTRACTION_DIR, 'synthetic_flight_generator_script.py')}'"
    )

    # Step 2: Data wrangling
    wrangle_data = BashOperator(
        task_id="wrangle_flight_data",
        bash_command=f"python '{os.path.join(EXTRACTION_DIR, 'data_wrangling.py')}'"
    )

    # Step 3: Run transformations (in parallel)
    transform_airport = PapermillOperator(
        task_id="transform_airport_traffic",
        input_nb=os.path.join(TRANSFORMATION_DIR, "airport_traffic_analysis.ipynb"),
        output_nb=os.path.join(TRANSFORMATION_DIR, "out_airport_traffic_analysis.ipynb"),
    )

    transform_delay = PapermillOperator(
        task_id="transform_flight_delay",
        input_nb=os.path.join(TRANSFORMATION_DIR, "Flight_Delay_Predictor.ipynb"),
        output_nb=os.path.join(TRANSFORMATION_DIR, "out_Flight_Delay_Predictor.ipynb"),
    )

    transform_status = PapermillOperator(
        task_id="transform_flight_status",
        input_nb=os.path.join(TRANSFORMATION_DIR, "Flight_status_analysis.ipynb"),
        output_nb=os.path.join(TRANSFORMATION_DIR, "out_Flight_status_analysis.ipynb"),
    )

    # DAG dependencies
    generate_data >> wrangle_data >> [transform_airport, transform_delay, transform_status]
