from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import os, re, tarfile
from scraping_erasmus import run_pipeline
from datetime import timedelta


@dag('batch_scholarships_erasmus', start_date=datetime(2025, 3 , 29),
        description='DAG to scrape Erasmus scholarship data', tags=['batch_processing'],
        schedule='0 1 1 * *', catchup=False)
def my_dag():

    @task
    def scrape_erasmus_task(execution_timeout=timedelta(minutes=10)):
        run_pipeline()

    scrape_erasmus_task()

my_dag()
    
