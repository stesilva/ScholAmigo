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
from scraping_daad import scrape_callable
from datetime import timedelta


@dag('batch_scholarships_daad', start_date=datetime(2025, 3 , 29),
        description='DAG to scrape german scholarship data', tags=['batch_processing'],
        schedule='0 1 1 * *', catchup=False)
def my_dag():

    @task
    def scrape_daad_task(execution_timeout=timedelta(minutes=10)):
        scrape_callable()

    scrape_daad_task()

my_dag()
    

