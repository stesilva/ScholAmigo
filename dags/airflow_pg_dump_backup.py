from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess
import logging
import os

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
    
    @task(task_id="run_pg_dump")
    def run_pg_dump():
        script_path = "/opt/airflow/dags/pg_dump_to_s3.py"
        
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
            
        logging.info(f"Running pg_dump script at {script_path}")
        
        try:
            result = subprocess.run(
                ["python", script_path],
                capture_output=True,
                text=True,
                check=True
            )
            logging.info(f"Script output: {result.stdout}")
            return "Backup completed successfully"
        except subprocess.CalledProcessError as e:
            logging.error(f"Script failed with exit code {e.returncode}")
            logging.error(f"Error output: {e.stderr}")
            raise
    
    run_pg_dump()

dag = pg_dump_backup_dag()
