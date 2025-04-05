from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="export_clickstream_hourly",
    default_args=DEFAULT_ARGS,
    description="Export clickstream data to S3 and clean DB hourly",
    schedule="@hourly",
    start_date=datetime(2025, 4, 5),
    catchup=False,
    tags=["clickstream", "postgres", "s3"],
)
def export_clickstream_dag():
    @task
    def run_export_script():
        script_path = "/opt/airflow/dags/send_clickstream_to_aws.py"
        subprocess.check_call(["python", script_path])

    @task
    def run_cleanup_script():
        cleanup_path = "/opt/airflow/dags/cleanup_postgres_clickstream.py"
        subprocess.check_call(["python", cleanup_path])

    @task
    def run_s3_folder_cleanup():
        cleanup_s3_path = "/opt/airflow/dags/cleanup_s3_clickstream.py"
        subprocess.check_call(["python", cleanup_s3_path])

    run_export_script() >> run_cleanup_script() >> run_s3_folder_cleanup()

dag = export_clickstream_dag()
