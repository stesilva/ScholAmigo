from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys
import os
import logging
from pathlib import Path

# Add the current directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from recommendation_system import create_local_embeddings
from discord_notifications import notify_task_failure, notify_dag_success

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_task_failure
}

def update_recommendation_system():
    """
    Task to update the recommendation system embeddings
    """
    try:
        # Verify Pinecone variables are set
        pinecone_api_key = Variable.get("PINECONE_API_KEY", default_var=None)
        pinecone_host = Variable.get("PINECONE_HOST", default_var=None)
        
        if not pinecone_api_key or not pinecone_host:
            raise ValueError(
                "Missing required Airflow variables. Please set: "
                + "PINECONE_API_KEY, PINECONE_HOST"
            )
        
        logging.info("Starting recommendation system update...")
        create_local_embeddings()
        logging.info("Recommendation system updated successfully!")
        
    except Exception as e:
        logging.error(f"Error updating recommendation system: {str(e)}")
        raise

with DAG(
    'scholarship_recommendation_system',
    default_args=default_args,
    description='DAG to update scholarship recommendation system embeddings',
    schedule_interval='0 2 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scholarship', 'recommendation']
) as dag:

    update_embeddings = PythonOperator(
        task_id='update_recommendation_embeddings',
        python_callable=update_recommendation_system,
        dag=dag,
        on_success_callback=lambda context: notify_dag_success(
            context,
            message="Recommendation system embeddings updated successfully!"
        )
    )

    # Set task dependencies
    update_embeddings 