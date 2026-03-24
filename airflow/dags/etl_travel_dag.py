
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="travel_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id="run_etl_script",
        bash_command="python /opt/airflow/scripts/etl_travel.py"
    )