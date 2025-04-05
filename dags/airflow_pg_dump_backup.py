from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess, logging, os

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pg_dump_to_s3_daily",
    default_args=DEFAULT_ARGS,
    description="Daily PostgreSQL database backup to S3",
    schedule="0 2 * * *",
    start_date=datetime(2025, 4, 5),
    catchup=False,
    tags=["postgres", "backup", "s3"],
)
def pg_dump_backup_dag():
    
    @task
    def run_pg_dump_task():
        """Calls the python script that uses local pg_dump over the network."""
        script_path = "/opt/airflow/dags/pg_dump_to_s3.py"
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Backup script missing: {script_path}")
        
        logging.info(f"Executing backup script: {script_path}")
        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logging.error(f"Backup failed with code {result.returncode}")
            logging.error(f"Stderr: {result.stderr}")
            raise RuntimeError("pg_dump_to_s3 script failed.")
        
        logging.info(f"Backup script output:\n{result.stdout}")

    run_pg_dump_task()

dag = pg_dump_backup_dag()
