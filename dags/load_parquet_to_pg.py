from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

BUCKET  = "scholarship-data-trusted"
FOLDERS = ["erasmus_scholarships_standardized",
           "german_scholarships_llm"]

default_args = {"owner": "airflow", "depends_on_past": False}

with DAG(
    dag_id="load_scholarships_to_pg",
    start_date=datetime(2024, 1, 1),
    schedule=None,               # trigger manually or from another DAG
    catchup=False,
    default_args=default_args,
) as dag:

    for folder in FOLDERS:
        task = BashOperator(
            task_id=f"load_latest_{folder}",
            bash_command=(
                "python /opt/airflow/scripts/load_parquet_to_pg.py "
                f"--bucket {BUCKET} --prefix {folder}/"
            ),
        )
