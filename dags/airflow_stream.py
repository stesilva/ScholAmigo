# from datetime import datetime
# from airflow.operators.python import PythonOperator
# from airflow import DAG
# from airflow.utils.helpers import chain
# from airflow.exceptions import AirflowException
# import os, re, tarfile
# from scraping_daad import scrape_callable, send_data_to_aws

# log_file='/Users/elnararb/airflow/the_logs/log.txt'
# output_path='/Users/elnararb/airflow/the_logs'

# with DAG('scrape_daad', start_date=datetime(2024, 11, 8),
#          description='DAG to extract DAAD data', tags=['batch_scholarships'],
#          schedule='@monthly', catchup=False):
#     task_1 = PythonOperator(task_id='scrape', python_callable=scrape_callable)
#     task_2 = PythonOperator(task_id='send_to_aws', python_callable=send_data_to_aws)
#     task_1 >> task_2

from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import os, re, tarfile
from kafka_producer import produce

log_file='/Users/elnararb/airflow/the_logs/log.txt'
output_path='/Users/elnararb/airflow/the_logs'

@dag('stream', start_date=datetime(2025, 3 , 29),
        description='DAG for streaming data', tags=['stream_'],
        schedule=None, catchup=False)
def my_dag():
    
    @task
    def produce_kafka():
        produce()

    produce_kafka()

my_dag()
    