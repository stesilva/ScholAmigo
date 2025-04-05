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
    

